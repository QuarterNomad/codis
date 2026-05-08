# Codis Proxy 数据面主链路源码阅读

本文只阅读普通请求链路：

```text
客户端 -> proxy -> Session -> Request -> Router -> Slot -> BackendConn -> codis-server
```

示例命令固定为：

```redis
SET user:1 abc
```

第一轮先假设：无认证、使用 DB 0、无 pipeline、无迁移、无读写分离。PE、background、dashboard 如何生成并下发 slot 路由表也先不展开，只把 `Router.slots` 当成已经被控制面填好的运行时路由表。

## 1. 客户端连接进入 proxy

入口在 [`pkg/proxy/proxy.go`](pkg/proxy/proxy.go) 的 `serveProxy()`，大约从 L396 开始：

```go
c, err := s.acceptConn(l)
if err != nil {
	return err
}
NewSession(c, s.config).Start(s.router)
```

这里完成两件事：

- `acceptConn()` 从 proxy 的 TCP listener 上接收客户端连接。
- `NewSession(c, s.config).Start(s.router)` 为这个客户端连接创建一个 `Session`，并把当前 `Router` 传进去。

所以第一层对象关系是：

```text
一个客户端 TCP 连接 = 一个 Session
```

## 2. Session 启动读写协程

`Session` 定义在 [`pkg/proxy/session.go`](pkg/proxy/session.go)，大约从 L21 开始。`Start()` 在同文件大约 L114：

```go
tasks := NewRequestChanBuffer(1024)

go func() {
	s.loopWriter(tasks)
	decrSessions()
}()

go func() {
	s.loopReader(tasks, d)
	tasks.Close()
}()
```

`Session` 内部有两个主协程：

- `loopReader()`：从客户端连接读 Redis 协议，创建并分发 `Request`。
- `loopWriter()`：按请求顺序等待响应，然后把响应写回客户端。

中间的 `tasks` 是 `RequestChan`，负责保持同一个客户端连接内的请求响应顺序。

即使第一轮不看 pipeline，也要注意这个结构：单条 `SET` 请求同样会先进入 `loopReader()`，再进入 `loopWriter()` 返回结果。

## 3. Redis 命令变成 Request

`loopReader()` 在 [`pkg/proxy/session.go`](pkg/proxy/session.go)，大约从 L152 开始：

```go
multi, err := s.Conn.DecodeMultiBulk()
...
r := &Request{}
r.Multi = multi
r.Batch = &sync.WaitGroup{}
r.Database = s.database
r.UnixNano = start.UnixNano()
```

客户端发来的：

```redis
SET user:1 abc
```

会被 Redis RESP 解码成 `multi`，再包装成一个 `Request`。

`Request` 定义在 [`pkg/proxy/request.go`](pkg/proxy/request.go)，大约从 L14 开始，第一轮重点看这些字段：

- `Multi`：原始 Redis 命令参数数组，对 `SET user:1 abc` 来说就是 `SET`、`user:1`、`abc`。
- `Batch`：用于让 `Session.loopWriter()` 等待后端响应完成。
- `Database`：当前 DB，普通场景下是 0。
- `OpStr`、`OpFlag`：命令识别后填充。
- `Resp`、`Err`：后端返回结果或错误。
- `Group`：指向 `Slot.refs`，用于保护 slot 更新期间的在途请求。

`loopReader()` 随后调用：

```go
if err := s.handleRequest(r, d); err != nil {
	...
} else {
	tasks.PushBack(r)
}
```

这里有一个关键点：`handleRequest()` 负责把请求送往后端；`tasks.PushBack(r)` 负责把同一个请求交给 `Session.loopWriter()`，让它稍后等待并回写响应。

## 4. 识别 SET 并提取 hash key

`handleRequest()` 在 [`pkg/proxy/session.go`](pkg/proxy/session.go)，大约从 L257 开始：

```go
opstr, flag, err := getOpInfo(r.Multi)
...
r.OpStr = opstr
r.OpFlag = flag
r.Broken = &s.broken
...
default:
	return d.dispatch(r)
```

`getOpInfo()` 位于 [`pkg/proxy/mapper.go`](pkg/proxy/mapper.go)，大约从 L272 开始，会把命令名转成大写并查询 `opTable`。

`SET` 在 [`pkg/proxy/mapper.go`](pkg/proxy/mapper.go) 大约 L185 被注册为：

```go
{"SET", FlagWrite},
```

所以 `SET` 是写命令。`FlagWrite` 会影响后面的后端选择：写命令必须走 master。

hash key 的提取发生在 [`pkg/proxy/mapper.go`](pkg/proxy/mapper.go)，大约从 L310 开始：

```go
func getHashKey(multi []*redis.Resp, opstr string) []byte {
	var index = 1
	switch opstr {
	case "ZINTERSTORE", "ZUNIONSTORE", "EVAL", "EVALSHA":
		index = 3
	}
	if index < len(multi) {
		return multi[index].Value
	}
	return nil
}
```

普通命令默认取第 2 个参数作为 key，因此：

```text
SET user:1 abc
    ^^^^^^
    hkey = user:1
```

## 5. Router 根据 key 找 Slot

`Router.dispatch()` 在 [`pkg/proxy/router.go`](pkg/proxy/router.go)，大约从 L139 开始：

```go
hkey := getHashKey(r.Multi, r.OpStr)
var id = Hash(hkey) % MaxSlotNum
slot := &s.slots[id]
return slot.forward(r, hkey)
```

`MaxSlotNum` 来自 [`pkg/models/slots.go`](pkg/models/slots.go)，大约 L13，固定为 1024。

`Hash()` 在 [`pkg/proxy/mapper.go`](pkg/proxy/mapper.go)，大约从 L297 开始：

```go
func Hash(key []byte) uint32 {
	const (
		TagBeg = '{'
		TagEnd = '}'
	)
	if beg := bytes.IndexByte(key, TagBeg); beg >= 0 {
		if end := bytes.IndexByte(key[beg+1:], TagEnd); end >= 0 {
			key = key[beg+1 : beg+1+end]
		}
	}
	return crc32.ChecksumIEEE(key)
}
```

因此 slot 计算不是简单地对完整 key 做 CRC32。若 key 含有 hash tag，例如 `user:{1}:name`，只会 hash `{}` 内部的 `1`。本例 `user:1` 没有 hash tag，所以使用完整 key：

```text
slot id = crc32("user:1") % 1024
```

## 6. Slot 是运行时分片对象

`Slot` 定义在 [`pkg/proxy/slots.go`](pkg/proxy/slots.go)，大约从 L12 开始：

```go
type Slot struct {
	id int
	...
	backend, migrate struct {
		id int
		bc *sharedBackendConn
	}
	replicaGroups [][]*sharedBackendConn

	method forwardMethod
}
```

第一轮只需要理解这些字段：

- `id`：slot 编号，范围是 `[0, 1024)`。
- `backend.bc`：当前 slot 的目标 master 后端连接池。
- `migrate.bc`：迁移来源，普通场景为空。
- `replicaGroups`：副本连接池，第一轮读写分离先不看。
- `method`：转发策略，普通场景看 `forwardSync`。

`Slot.forward()` 很薄：

```go
func (s *Slot) forward(r *Request, hkey []byte) error {
	return s.method.Forward(s, r, hkey)
}
```

真正的选择逻辑在 `forward.go`。

## 7. SET 为什么走 master

普通同步转发在 [`pkg/proxy/forward.go`](pkg/proxy/forward.go)，大约从 L35 开始：

```go
func (d *forwardSync) Forward(s *Slot, r *Request, hkey []byte) error {
	s.lock.RLock()
	bc, err := d.process(s, r, hkey)
	s.lock.RUnlock()
	if err != nil {
		return err
	}
	bc.PushBack(r)
	return nil
}
```

`process()` 在无迁移场景下会设置 slot 引用计数，并进入 `forward2()`：

```go
r.Group = &s.refs
r.Group.Add(1)
return d.forward2(s, r), nil
```

`forward2()` 在 [`pkg/proxy/forward.go`](pkg/proxy/forward.go)，大约从 L216 开始：

```go
if s.migrate.bc == nil && !r.IsMasterOnly() && len(s.replicaGroups) != 0 {
	...
}
return s.backend.bc.BackendConn(database, seed, true)
```

`SET` 的 `OpFlag` 包含 `FlagWrite`，而 [`pkg/proxy/mapper.go`](pkg/proxy/mapper.go) 大约 L42 的 `IsMasterOnly()` 判断是：

```go
const mask = FlagWrite | FlagMayWrite | FlagMasterOnly
return (f & mask) != 0
```

所以 `SET` 满足 `r.IsMasterOnly() == true`，不会进入 replica 分支，最终选择：

```text
s.backend.bc.BackendConn(..., must=true)
```

也就是该 slot 对应 master 的 `BackendConn`。

## 8. BackendConn 写到 codis-server

`BackendConn.PushBack()` 在 [`pkg/proxy/backend.go`](pkg/proxy/backend.go)，大约从 L74 开始：

```go
func (bc *BackendConn) PushBack(r *Request) {
	if r.Batch != nil {
		r.Batch.Add(1)
	}
	bc.input <- r
}
```

这里做了两件事：

- `r.Batch.Add(1)`：告诉 `Session.loopWriter()` 这个请求还要等一个后端响应。
- `bc.input <- r`：把请求放到 proxy 到 codis-server 的后端连接队列。

`BackendConn.run()` 会不断运行 `loopWriter()`。`loopWriter()` 在 [`pkg/proxy/backend.go`](pkg/proxy/backend.go)，大约从 L328 开始：

```go
for r := range bc.input {
	if err := p.EncodeMultiBulk(r.Multi); err != nil {
		return bc.setResponse(r, nil, fmt.Errorf("backend conn failure, %s", err))
	}
	if err := p.Flush(len(bc.input) == 0); err != nil {
		return bc.setResponse(r, nil, fmt.Errorf("backend conn failure, %s", err))
	} else {
		tasks <- r
	}
}
```

对本例来说，`EncodeMultiBulk(r.Multi)` 会把 `SET user:1 abc` 按 RESP 协议写给后端 codis-server。写成功后，`r` 会进入后端 reader 的 `tasks` 队列，等待按同样顺序读取响应。

后端 reader 在 [`pkg/proxy/backend.go`](pkg/proxy/backend.go)，大约从 L276 开始：

```go
for r := range tasks {
	resp, err := c.Decode()
	if err != nil {
		return bc.setResponse(r, nil, fmt.Errorf("backend conn failure, %s", err))
	}
	...
	bc.setResponse(r, resp, nil)
}
```

如果 codis-server 返回 `OK`，`resp` 会被写回 `Request.Resp`。

`setResponse()` 在 [`pkg/proxy/backend.go`](pkg/proxy/backend.go)，大约从 L241 开始：

```go
func (bc *BackendConn) setResponse(r *Request, resp *redis.Resp, err error) error {
	r.Resp, r.Err = resp, err
	if r.Group != nil {
		r.Group.Done()
	}
	if r.Batch != nil {
		r.Batch.Done()
	}
	return err
}
```

这一步同时释放两个等待：

- `r.Group.Done()`：释放 `Slot.refs`，表示这个 slot 上的一个在途请求结束。
- `r.Batch.Done()`：释放 `Session.loopWriter()`，表示这个请求可以回给客户端。

## 9. 响应回到客户端

回到 `Session.loopWriter()`，它在 [`pkg/proxy/session.go`](pkg/proxy/session.go) 大约 L199 按 `tasks` 顺序处理请求：

```go
resp, err := s.handleResponse(r)
...
if err := p.Encode(resp); err != nil {
	return s.incrOpFails(r, err)
}
```

`handleResponse()` 会先等待后端：

```go
r.Batch.Wait()
...
return r.Resp, nil
```

当 `BackendConn.loopReader()` 已经把 `OK` 放进 `r.Resp` 并执行 `r.Batch.Done()` 后，`Session.loopWriter()` 就会把这个 `OK` 编码写回客户端连接。

最终客户端看到：

```redis
OK
```

## 一眼版调用链

```text
Proxy.serveProxy
  -> acceptConn
  -> NewSession(...).Start(router)
    -> Session.loopReader
      -> redis.Conn.DecodeMultiBulk
      -> Request{Multi, Batch, Database, UnixNano}
      -> Session.handleRequest
        -> getOpInfo: SET -> FlagWrite
        -> Router.dispatch
          -> getHashKey: user:1
          -> Hash(user:1) % 1024
          -> Slot.forward
            -> forwardSync.Forward
              -> forwardSync.process
                -> Request.Group = &Slot.refs
              -> forwardHelper.forward2
                -> choose slot backend master
              -> BackendConn.PushBack
                -> Request.Batch.Add(1)
    -> BackendConn.loopWriter
      -> EncodeMultiBulk(SET user:1 abc)
      -> flush to codis-server
    -> BackendConn.loopReader
      -> Decode OK
      -> setResponse(Request, OK, nil)
        -> Slot.refs.Done()
        -> Request.Batch.Done()
    -> Session.loopWriter
      -> handleResponse
        -> Request.Batch.Wait()
      -> Encode OK to client
```

## 第一轮自测问题

读完后应该能回答：

- 客户端连接 proxy 后，`Session` 在 `serveProxy()` 的 `NewSession(c, s.config).Start(s.router)` 创建。
- `SET user:1 abc` 在 `Session.loopReader()` 中被 `DecodeMultiBulk()` 读出，并包装为 `Request`。
- `user:1` 在 `Router.dispatch()` 调用的 `getHashKey()` 中取出。
- slot id 通过 `Hash(hkey) % MaxSlotNum` 计算，`MaxSlotNum` 是 1024。
- `SET` 在 `opTable` 中是 `FlagWrite`，因此 `IsMasterOnly()` 为 true，最终走 master。
- 后端返回 `OK` 后，`BackendConn.setResponse()` 设置 `Request.Resp` 并 `Batch.Done()`，`Session.loopWriter()` 再把响应写回客户端。

## 第一轮暂时跳过

- `AUTH` 和 `SELECT`：它们在 `Session.handleRequest()` 中有单独分支。
- pipeline：当前文档只看一条命令，但同一个 `RequestChan` 和 backend pipeline 结构也服务于 pipeline。
- `MGET`、`MSET`、`DEL`、`EXISTS`：这些多 key 命令有单独处理逻辑。
- 迁移：`Slot.migrate.bc` 不为空时会进入 `slotsmgrt` 或 `slotsmgrt-exec-wrapper`。
- 读写分离：只有非 master-only 请求才可能在 `forward2()` 中选择 replica。
- 控制面：dashboard、topom、PE、background 如何生成和刷新 `Router.slots` 放到第二阶段阅读。
