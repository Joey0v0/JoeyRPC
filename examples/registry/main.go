package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/Joey0v0/JoeyRPC"
	"github.com/Joey0v0/JoeyRPC/discovery"
	"github.com/Joey0v0/JoeyRPC/registry"
	"github.com/Joey0v0/JoeyRPC/xclient"
)

type Foo int
type Args struct{ Num1, Num2 int }

func (Foo) Sum(args Args, reply *int) error { *reply = args.Num1 + args.Num2; return nil }
func (f Foo) Sleep(args Args, reply *int) error {
	time.Sleep(time.Duration(args.Num1) * time.Second)
	return f.Sum(args, reply)
}

func runRegistry() (string, func()) {
	listener, err := net.Listen("tcp", ":9999")
	if err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/_joeyrpc_/registry", registry.New(5*time.Minute))
	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()
	return "http://localhost:9999/_joeyrpc_/registry", func() { _ = server.Close() }
}

func runInstance(registryAddress string) func() {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	server := joeyrpc.NewServer()
	if err := server.Register(new(Foo)); err != nil {
		log.Fatal(err)
	}
	go server.Accept(listener)
	registry.Heartbeat(registryAddress, "tcp@"+listener.Addr().String(), 0)
	return func() { _ = listener.Close() }
}

func report(kind, method string, args Args, reply int, err error) {
	if err != nil {
		log.Printf("%s %s error: %v", kind, method, err)
		return
	}
	log.Printf("%s %s success: %d + %d = %d", kind, method, args.Num1, args.Num2, reply)
}

func demonstrate(address string, broadcast bool) {
	endpoints := discovery.NewRegistryDiscovery(address, 0)
	client := xclient.NewXClient(endpoints, discovery.RandomSelect, nil)
	defer client.Close()
	var tasks sync.WaitGroup
	for number := 0; number < 5; number++ {
		tasks.Add(1)
		go func(n int) {
			defer tasks.Done()
			args := Args{Num1: n, Num2: n * n}
			var reply int
			if !broadcast {
				err := client.Call(context.Background(), "Foo.Sum", &args, &reply)
				report("call", "Foo.Sum", args, reply, err)
				return
			}
			err := client.Broadcast(context.Background(), "Foo.Sum", &args, &reply)
			report("broadcast", "Foo.Sum", args, reply, err)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err = client.Broadcast(ctx, "Foo.Sleep", &args, &reply)
			report("broadcast", "Foo.Sleep", args, reply, err)
		}(number)
	}
	tasks.Wait()
}

func main() {
	log.SetFlags(0)
	address, stopRegistry := runRegistry()
	defer stopRegistry()
	stopFirst := runInstance(address)
	defer stopFirst()
	stopSecond := runInstance(address)
	defer stopSecond()
	demonstrate(address, false)
	demonstrate(address, true)
}
