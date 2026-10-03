# JoeyRPC

JoeyRPC 是一个使用 Go 标准库实现的轻量级 RPC 学习项目，包含反射服务注册、并发调用、Gob 编解码、超时控制，以及基础的注册中心、服务发现和客户端负载均衡。

项目展示名称为 **JoeyRPC**，Go 模块路径为 `github.com/Joey0v0/JoeyRPC`，根包名为 `joeyrpc`。目前没有第三方依赖。

## 在其他项目中引入

代码推送到 GitHub 仓库后，其他项目可执行：

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

在发布版本标签后，也可以指定版本，例如 `go get github.com/Joey0v0/JoeyRPC@v0.1.0`。此命令要求仓库中已存在对应标签。

## 快速运行

在项目根目录执行：

```sh
go test ./...
go vet ./...
go run ./examples/registry
```

示例在一个进程中启动 HTTP 注册中心（端口 9999）和两个 RPC 服务实例，演示随机选择实例、并发求和、广播调用和超时返回。`Foo.Sleep` 的部分调用预期会输出超时错误。

HTTP 协议名称现已统一为 JoeyRPC：RPC 路径为 `/_joeyrpc_`，调试页为 `/debug/joeyrpc`，默认注册中心路径为 `/_joeyrpc_/registry`。旧版 HTTP 客户端和注册发现组件需同步升级，手动配置的 URL 也需更新；直接 TCP 调用的 JSON/Gob 消息格式保持不变。

## 目录结构

```text
.
├── client.go              # 单连接客户端、请求跟踪、同步与异步调用
├── server.go              # 接收连接、请求分发、并发处理
├── service.go             # 反射注册、参数创建、方法执行
├── option.go              # 连接配置、默认值和配置解析
├── dial.go                # 建连超时和 protocol@address 路由
├── http.go                # HTTP CONNECT 传输适配
├── debug.go               # 服务与方法调用次数调试页
├── doc.go                 # 根包文档
├── codec/                 # 消息头、编解码接口与 Gob 实现
├── discovery/             # 静态/注册中心发现，随机与轮询选择
├── registry/              # HTTP 注册中心与心跳发送
├── xclient/               # 多实例客户端、连接复用和广播
├── examples/registry/     # 可直接运行的完整示例
└── docs/architecture.md   # 架构、协议、调用流程与实现边界
```

核心库留在根包，避免调用方导入多层路径；按功能划分独立子包。测试与对应代码放在一起，根目录的 `integration_test.go` 验证包之间的协作。

模块内部设计、兼容范围及逐目录改动见[架构文档](docs/architecture.md)和[实现重写记录](docs/rewrite.md)。

## 使用方式

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

以上片段需放入应用程序并导入 `github.com/Joey0v0/JoeyRPC` 及所用标准库。多实例调用使用 `discovery` 和 `xclient`：

```go
import (
    "github.com/Joey0v0/JoeyRPC/discovery"
    "github.com/Joey0v0/JoeyRPC/xclient"
)

// 以下语句放在函数内。
d := discovery.NewMultiServerDiscovery([]string{
    "tcp@127.0.0.1:8000",
    "tcp@127.0.0.1:8001",
})
xc := xclient.NewXClient(d, discovery.RoundRobinSelect, nil)
defer xc.Close()
// 使用 xc.Call(ctx, "Calculator.Sum", args, &reply)。
```

## 许可

第三方代码的版权说明见 [NOTICE.md](NOTICE.md)，完整许可文本见 [LICENSE](LICENSE)。
