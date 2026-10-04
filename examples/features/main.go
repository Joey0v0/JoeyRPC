package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/Joey0v0/JoeyRPC"
	"github.com/Joey0v0/JoeyRPC/discovery"
	"github.com/Joey0v0/JoeyRPC/xclient"
)

type Greeter struct{}

func (Greeter) Hello(ctx context.Context, name string, reply *string) error {
	*reply = fmt.Sprintf("hello %s, trace=%s", name, joeyrpc.MetadataFromContext(ctx)["traceID"])
	return nil
}

func auth(ctx context.Context, method string, args, reply interface{}, next joeyrpc.ServerHandler) error {
	if joeyrpc.MetadataFromContext(ctx)["token"] != "demo-token" {
		return errors.New("unauthorized")
	}
	return next(ctx, args, reply)
}

func logging(ctx context.Context, method string, args, reply interface{}, next joeyrpc.ClientInvoker) error {
	start := time.Now()
	err := next(ctx, method, args, reply)
	log.Printf("%s took %s, error=%v", method, time.Since(start), err)
	return err
}

func main() {
	log.SetFlags(0)
	server := joeyrpc.NewServerWithOptions(joeyrpc.ServerOptions{
		Interceptors: []joeyrpc.ServerInterceptor{auth},
	})
	if err := server.Register(Greeter{}); err != nil {
		log.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	go server.Accept(listener)
	// Reserve then release an address to demonstrate connection-refused failover.
	unavailable, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	bad := "tcp@" + unavailable.Addr().String()
	_ = unavailable.Close()
	d := discovery.NewMultiServerDiscovery([]string{bad, "tcp@" + listener.Addr().String()})
	client := xclient.NewXClientWithOptions(d, discovery.RoundRobinSelect, nil, xclient.Options{
		Client: joeyrpc.ClientOptions{Interceptors: []joeyrpc.ClientInterceptor{logging}},
		Retry: xclient.RetryOptions{
			MaxAttempts: 3, InitialBackoff: 20 * time.Millisecond, MaxBackoff: 100 * time.Millisecond,
			Idempotent: func(method string) bool { return method == "Greeter.Hello" },
		},
	})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		request := joeyrpc.WithMetadata(ctx, joeyrpc.Metadata{"token": "demo-token", "traceID": fmt.Sprint(i)})
		var reply string
		if err := client.Call(request, "Greeter.Hello", "Joey", &reply); err != nil {
			log.Fatal(err)
		}
		log.Println(reply)
	}
	var reply string
	log.Printf("missing-token call (expected error): %v", client.Call(ctx, "Greeter.Hello", "Joey", &reply))
	shutdown, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := server.Shutdown(shutdown); err != nil {
		log.Fatal(err)
	}
	log.Println("server shut down gracefully")
}
