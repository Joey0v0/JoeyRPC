# JoeyRPC 扩展功能

四项功能仅使用标准库，现有 `NewServer`、`Dial`、`NewXClient` 和两参数业务方法继续可用。默认没有拦截器，重试默认关闭。

可运行示例：

```sh
go run ./examples/features
```

## Metadata

调用方通过 Context 提供每次请求的附加信息：

```go
ctx := joeyrpc.WithMetadata(ctx, joeyrpc.Metadata{
    "token": "demo-token",
    "traceID": "trace-001",
})
err := client.Call(ctx, "Greeter.Hello", "Joey", &reply)
```

客户端把 Metadata 放入 Gob 请求头，服务端解码后创建请求 Context。Context 本身不会被序列化，客户端取消也不会通过这个字段传到服务端。请求 Metadata 不会回显到响应。

业务方法可以增加 Context 参数：

```go
func (Greeter) Hello(ctx context.Context, name string, reply *string) error {
    md := joeyrpc.MetadataFromContext(ctx)
    *reply = "hello " + name + ", trace=" + md["traceID"]
    return nil
}
```

原有 `Method(args, reply) error` 仍可注册。`WithMetadata` 和 `MetadataFromContext` 均复制 map，避免后续修改影响其他请求；调用方不能在复制期间并发修改原 map。

Gob 支持按字段名匹配结构体，旧请求头和新请求头可以互相解码，未知 Metadata 字段会被旧对端忽略。新服务依赖 token 鉴权时，旧客户端需要升级才能传 token。直接构造公开的 `codec.Header` 应使用带字段名的字面量。

## Interceptor

客户端拦截器使用以下函数类型：

```go
func(ctx context.Context, method string, args, reply interface{}, next joeyrpc.ClientInvoker) error
```

服务端拦截器的最后一个参数是 `joeyrpc.ServerHandler`，通过 `next(ctx, args, reply)` 继续执行。直接返回错误即可中止调用。

配置入口：

```go
server := joeyrpc.NewServerWithOptions(joeyrpc.ServerOptions{
    Interceptors: []joeyrpc.ServerInterceptor{auth, serverLogging},
})
client, err := joeyrpc.DialWithOptions("tcp", address, joeyrpc.ClientOptions{
    Interceptors: []joeyrpc.ClientInterceptor{clientLogging},
})
```

这些选项只保存在本地，连接初始化的 `Option` 仍只传网络配置。配置切片在构造时复制；拦截器自身的共享数据由应用负责并发保护。

链 `[A, B]` 的执行顺序是 `A 前 → B 前 → 实际调用 → B 后 → A 后`。客户端链作用于同步 `Call`，包括 XClient 和 Broadcast 内部的调用；异步 `Go` 保持原有行为，不执行客户端拦截器且不传 Metadata。服务端链处理两种客户端发来的请求。

## Retry 与退避

使用新构造器显式启用：

```go
xc := xclient.NewXClientWithOptions(d, discovery.RoundRobinSelect, nil, xclient.Options{
    Client: joeyrpc.ClientOptions{Interceptors: []joeyrpc.ClientInterceptor{clientLogging}},
    Retry: xclient.RetryOptions{
        MaxAttempts: 3,
        InitialBackoff: 100 * time.Millisecond,
        MaxBackoff: time.Second,
        Multiplier: 2,
        Idempotent: func(method string) bool { return method == "Greeter.Hello" },
    },
})
```

- `MaxAttempts` 包含第一次调用；小于等于 1 表示关闭重试。未指定等待参数时，初始等待 100ms，上限 1s，倍数 2。
- 每次等待为当前退避值的 50%～100%，加入随机抖动；退避值逐次乘以倍数，且不超过上限。
- 建连阶段的网络错误可以重试，因为业务请求尚未发出。发送或接收时的 EOF、网络错误，仅在 `Idempotent(method)` 返回 true 时重试。这个标记是应用的承诺，框架不会自动实现业务去重。
- `RemoteError`（包括业务错误、服务端处理超时、鉴权错误）不重试。协议/编码错误、Context 取消或超时也不重试。
- 重试重新查询发现接口，存在其他节点时避开刚失败的地址；不会强制绕过发现缓存。只有一个节点时可以再次尝试该节点。
- 每次尝试分配独立的响应对象，只将成功结果复制到调用方的 reply，失败尝试不覆盖它。
- 所有尝试共享原来的 Context。退避、建连、初始化，以及内置注册中心查询和等待刷新均支持取消。自定义 Discovery 的旧接口可能阻塞；发送中的阻塞和已经开始的响应解码仍可能让 Call 晚于截止时间返回。

连接失效后的重新创建并不等于重试业务；重试默认关闭。Broadcast 不自动重试，避免把一次广播扩展成额外的重复执行。

## 优雅关闭

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
err := server.Shutdown(ctx)
```

流程：标记关闭 → 关闭由 `Accept` 管理的监听器 → 拒绝已有连接上的新请求 → 等待已接收业务完成及响应写入完成 → 关闭连接。

关闭状态与请求登记共用互斥锁，避免等待结束后又加入新任务。实际业务执行和响应发送分别计数；即使 HandleTimeout 已向客户端返回错误，仍等待尚未结束的业务方法。

超过 Shutdown 的 Context 限制时，取消连接上的服务端 Context，关闭连接并返回 Context 错误。不能强行杀掉业务 goroutine；业务方法需要监听 Context，或将它交给支持 Context 的下游操作。服务端处理超时也会取消当前 handler 的 Context，原有不接收 Context 的方法可能继续运行。

Shutdown 后 Server 不再接受请求。HTTP CONNECT 已劫持的 RPC 连接也纳入管理；应用自己创建的外层 `http.Server` 需要由应用单独关闭。
