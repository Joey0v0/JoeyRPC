# JoeyRPC

JoeyRPC 是一个使用 Go 标准库实现的轻量级 RPC 框架。在 TCP 持久连接上使用 Gob 编码请求与响应，通过反射注册服务方法，支持并发调用、超时控制和基础的多节点调用。

项目还提供请求 Metadata、同步调用拦截器、可选重试及服务端优雅关闭，适合学习 RPC 协议、请求生命周期和 Go 并发实现。

Go 模块路径为 `github.com/Joey0v0/JoeyRPC`，根包名为 `joeyrpc`，无第三方运行依赖。

## 功能

| 能力 | 当前实现 |
| --- | --- |
| 通信 | TCP、HTTP CONNECT、Unix Socket；Gob 编解码 |
| 服务方法 | 反射注册，支持普通方法和带 `context.Context` 的方法 |
| 并发调用 | Seq 与 pending 表匹配响应，单连接支持多个请求 |
| 超时和错误 | 建连、调用、服务处理三类超时；区分业务错误与传输错误 |
| 多节点 | 内存注册中心、心跳、地址发现、加权轮询选择、连接复用和广播 |
| 扩展能力 | 请求 Metadata、拦截器链、可选重试、优雅关闭 |

## 安装

在使用方的 Go Modules 项目中执行：

```sh
go get github.com/Joey0v0/JoeyRPC@latest
```

根包和子包使用完整模块路径导入，根包在代码中仍以 `joeyrpc` 引用：

```go
import (
    "github.com/Joey0v0/JoeyRPC"
    "github.com/Joey0v0/JoeyRPC/discovery"
    "github.com/Joey0v0/JoeyRPC/xclient"
)
```

## 运行示例

### 两个进程之间的 RPC 调用

在仓库根目录打开两个终端。先启动服务端：

```sh
go run ./examples/basic/server
```

再启动客户端：

```sh
go run ./examples/basic/client -a 10 -b 20
```

客户端输出 `10 + 20 = 30`。两端默认使用 `127.0.0.1:8000`，也可通过 `-addr` 指定地址；服务端按 Ctrl+C 后优雅关闭。

### 注册发现与多节点调用

在项目根目录执行：

```sh
go run ./examples/registry
```

示例在同一进程启动 HTTP 注册中心和两个 RPC 服务实例，演示随机节点选择、连接复用、广播和超时。部分休眠调用预期返回超时错误。

### Metadata、拦截器、重试与关闭

```sh
go run ./examples/features
```

示例使用临时端口，演示 traceID 传递、token 鉴权、调用日志、连接失败后切换节点和 `Shutdown(ctx)`。缺少 token 的调用预期返回鉴权错误。

## 项目结构

```text
.
├── *.go                   # 客户端、服务端、注册和扩展功能
├── *_test.go              # 核心与集成测试
├── codec/                 # Gob 编解码与消息头
├── discovery/             # 地址发现、缓存和节点选择
├── registry/              # HTTP 注册中心与心跳
├── xclient/               # 多节点调用、连接复用、重试与广播
├── examples/
│   ├── basic/             # 独立服务端和客户端示例
│   ├── registry/          # 注册发现与广播示例
│   └── features/          # Metadata、拦截器、重试与关闭示例
└── docs/                  # 架构、扩展功能和测试说明
```

测试与对应代码放在一起。Go 在普通构建和导入库时不编译 `*_test.go`，保留测试不会增加使用方的运行代码。

## 核心用法

服务端注册方法：

```go
type Calculator struct{}
type Args struct{ A, B int }

func (Calculator) Sum(args Args, reply *int) error {
    *reply = args.A + args.B
    return nil
}

server := joeyrpc.NewServer()
if err := server.Register(&Calculator{}); err != nil {
    log.Fatal(err)
}
listener, err := net.Listen("tcp", "127.0.0.1:8000")
if err != nil {
    log.Fatal(err)
}
defer listener.Close()
server.Accept(listener)
```

客户端在建立连接后调用方法：

```go
client, err := joeyrpc.Dial("tcp", "127.0.0.1:8000")
if err != nil {
    log.Fatal(err)
}
defer client.Close()

ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()
var reply int
if err := client.Call(ctx, "Calculator.Sum", Args{A: 1, B: 2}, &reply); err != nil {
    log.Fatal(err)
}
```

以上是函数内的使用片段，完整程序见[服务端](examples/basic/server/main.go)与[客户端](examples/basic/client/main.go)。普通方法的签名为 `Method(args, replyPointer) error`；需要读取 Metadata 的方法可增加首个 `context.Context` 参数。

多实例调用使用 `discovery` 和 `xclient`：

```go
import (
    "github.com/Joey0v0/JoeyRPC/discovery"
    "github.com/Joey0v0/JoeyRPC/xclient"
)

// 以下语句放在函数内，ctx、Args 等沿用上方示例。
d := discovery.NewMultiServerDiscovery([]string{
    "tcp@127.0.0.1:8000",
    "tcp@127.0.0.1:8001",
})
xc := xclient.NewXClient(d, discovery.RoundRobinSelect, nil)
defer xc.Close()
err = xc.Call(ctx, "Calculator.Sum", Args{A: 10, B: 20}, &reply)
```

`XClient` 选择节点并管理底层连接；随机策略使用 `discovery.RandomSelect`。使用注册中心时，将静态发现替换为 `discovery.NewRegistryDiscovery(registryURL, 0)`。

需要按实例能力分配请求时，服务端调用 `registry.HeartbeatWithWeight(registryURL, endpoint, 5, 0)` 上报权重，客户端将选择模式改为 `discovery.WeightedRoundRobinSelect`。例如两个实例权重为 5 和 1，长期调用次数会接近 5:1，并通过平滑加权轮询交错分配；未配置权重的实例默认为 1。静态地址也可用 `d.UpdateWeighted([]discovery.Server{{Addr: "tcp@127.0.0.1:8000", Weight: 5}, {Addr: "tcp@127.0.0.1:8001", Weight: 1}})` 配置。权重只影响请求选址，`Broadcast` 仍对每个实例调用一次。

默认没有拦截器，重试默认关闭。完整配置见[扩展功能说明](docs/features.md)。

## 验证

```sh
go test -count=1 ./...
go vet ./...
```

测试覆盖并发调用、错误处理、Metadata、拦截器、重试和关闭。测试范围及平台说明见[测试说明](docs/testing.md)。

本机 TCP 回环条件下的吞吐、延迟、连接复用和故障验证见[基准结果与复现方法](docs/benchmark.md)。

## 实现边界

- 当前使用 Gob 编码，通信双方都需要理解 JoeyRPC 协议。仅支持 TCP 的既有系统仍需接入或适配该协议。
- 注册中心使用内存地址表，没有按服务名分组、持久化或高可用机制。
- 客户端取消不会传到服务端；业务 goroutine 需要配合 Context 才能在取消后停止。
- 客户端拦截器与 Metadata 用于同步 `Call`；异步 `Go` 保留基础调用行为。Broadcast 不自动重试。
- 请求发出后是否可重试，由应用保证方法幂等性。项目尚未完成生产环境下的长期压力与故障验证。

详细的协议和默认参数见[架构说明](docs/architecture.md)。

## 许可

项目包含第三方代码及其修改。版权与许可说明见 [NOTICE.md](NOTICE.md)，完整许可文本见 [LICENSE](LICENSE)。
