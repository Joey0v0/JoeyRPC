package joeyrpc

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type ContextService struct{ calls int32 }

func (s *ContextService) Echo(ctx context.Context, arg string, reply *string) error {
	atomic.AddInt32(&s.calls, 1)
	*reply = arg + ":" + MetadataFromContext(ctx)["traceID"]
	return nil
}

func featureClient(t *testing.T, server *Server, local ClientOptions, option *Option) (*Client, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go server.Accept(listener)
	client, err := DialWithOptions("tcp", listener.Addr().String(), local, option)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	return client, func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		_ = client.Close()
	}
}

func TestMetadataSnapshotsAndConcurrentRequests(t *testing.T) {
	md := Metadata{"traceID": "original"}
	ctx := WithMetadata(context.Background(), md)
	md["traceID"] = "changed"
	copy := MetadataFromContext(ctx)
	copy["traceID"] = "also changed"
	if MetadataFromContext(ctx)["traceID"] != "original" {
		t.Fatal("metadata snapshot was changed")
	}
	server := NewServer()
	service := &ContextService{}
	if err := server.Register(service); err != nil {
		t.Fatal(err)
	}
	if err := server.Register(Boundary{}); err != nil {
		t.Fatal(err)
	}
	client, stop := featureClient(t, server, ClientOptions{}, nil)
	defer stop()
	deadline, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var jobs sync.WaitGroup
	for i := 0; i < 24; i++ {
		jobs.Add(1)
		go func(n int) {
			defer jobs.Done()
			trace := fmt.Sprint(n)
			ctx := WithMetadata(deadline, Metadata{"traceID": trace})
			var reply string
			if err := client.Call(ctx, "ContextService.Echo", "hello", &reply); err != nil || reply != "hello:"+trace {
				t.Errorf("trace=%s reply=%s err=%v", trace, reply, err)
			}
		}(i)
	}
	jobs.Wait()
	var reply int
	if err := client.Call(deadline, "Boundary.Add", 41, &reply); err != nil || reply != 42 {
		t.Fatalf("legacy method: %d %v", reply, err)
	}
}

func TestInterceptorOrderAndMetadataAuthentication(t *testing.T) {
	var mu sync.Mutex
	var events []string
	log := func(event string) { mu.Lock(); events = append(events, event); mu.Unlock() }
	serverHook := func(name string) ServerInterceptor {
		return func(ctx context.Context, method string, args, reply interface{}, next ServerHandler) error {
			log(name + " before")
			err := next(ctx, args, reply)
			log(name + " after")
			return err
		}
	}
	auth := func(ctx context.Context, method string, args, reply interface{}, next ServerHandler) error {
		if MetadataFromContext(ctx)["token"] != "secret" {
			return errors.New("unauthorized")
		}
		return next(ctx, args, reply)
	}
	server := NewServerWithOptions(ServerOptions{Interceptors: []ServerInterceptor{serverHook("s1"), serverHook("s2"), auth}})
	service := &ContextService{}
	if err := server.Register(service); err != nil {
		t.Fatal(err)
	}
	clientHook := func(name string) ClientInterceptor {
		return func(ctx context.Context, method string, args, reply interface{}, next ClientInvoker) error {
			log(name + " before")
			err := next(ctx, method, args, reply)
			log(name + " after")
			return err
		}
	}
	client, stop := featureClient(t, server, ClientOptions{Interceptors: []ClientInterceptor{clientHook("c1"), clientHook("c2")}}, nil)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx = WithMetadata(ctx, Metadata{"token": "secret", "traceID": "trace"})
	var reply string
	if err := client.Call(ctx, "ContextService.Echo", "hello", &reply); err != nil || reply != "hello:trace" {
		t.Fatalf("call: %s %v", reply, err)
	}
	want := []string{"c1 before", "c2 before", "s1 before", "s2 before", "s2 after", "s1 after", "c2 after", "c1 after"}
	mu.Lock()
	got := append([]string(nil), events...)
	mu.Unlock()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("interceptor order: %v", got)
	}
	err := client.Call(WithMetadata(ctx, nil), "ContextService.Echo", "denied", &reply)
	var remote *RemoteError
	if !errors.As(err, &remote) || err.Error() != "unauthorized" || atomic.LoadInt32(&service.calls) != 1 {
		t.Fatalf("authentication did not stop handler: %v", err)
	}
}

func TestClientInterceptorCanStopBeforeSending(t *testing.T) {
	service := &ContextService{}
	server := NewServer()
	_ = server.Register(service)
	hook := func(ctx context.Context, method string, args, reply interface{}, next ClientInvoker) error {
		return errors.New("locally rejected")
	}
	client, stop := featureClient(t, server, ClientOptions{Interceptors: []ClientInterceptor{hook}}, nil)
	defer stop()
	var reply string
	if err := client.Call(context.Background(), "ContextService.Echo", "hello", &reply); err == nil || err.Error() != "locally rejected" {
		t.Fatalf("local error: %v", err)
	}
	if atomic.LoadInt32(&service.calls) != 0 {
		t.Fatal("client interceptor sent rejected request")
	}
}

func TestServerInterceptorCanReplaceArguments(t *testing.T) {
	hook := func(ctx context.Context, method string, args, reply interface{}, next ServerHandler) error {
		return next(ctx, args.(int)+1, reply)
	}
	server := NewServerWithOptions(ServerOptions{Interceptors: []ServerInterceptor{hook}})
	_ = server.Register(Boundary{})
	client, stop := featureClient(t, server, ClientOptions{}, nil)
	defer stop()
	var reply int
	if err := client.Call(context.Background(), "Boundary.Add", 41, &reply); err != nil || reply != 43 {
		t.Fatalf("replaced arguments: %d %v", reply, err)
	}
}

func TestXDialContextCancelsInitializationAndClosesConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	closed := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// Accept CONNECT but never respond. Cancellation must close the client.
		_, _ = ioutil.ReadAll(conn)
		close(closed)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err = XDialContext(ctx, "http@"+listener.Addr().String(), ClientOptions{}, &Option{ConnectTimeout: time.Second})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("initialization cancellation: %v", err)
	}
	awaitSignal(t, closed)
}
