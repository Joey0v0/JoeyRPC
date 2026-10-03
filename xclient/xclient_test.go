package xclient

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Joey0v0/JoeyRPC"
	"github.com/Joey0v0/JoeyRPC/discovery"
)

type CacheService struct{}

func (CacheService) Sum(arg int, reply *int) error { *reply = arg + 1; return nil }

type countedListener struct {
	net.Listener
	connections int32
}

func (l *countedListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		atomic.AddInt32(&l.connections, 1)
	}
	return conn, err
}

func liveEndpoint(t *testing.T) (string, *countedListener) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	counted := &countedListener{Listener: listener}
	server := joeyrpc.NewServer()
	if err := server.Register(CacheService{}); err != nil {
		t.Fatal(err)
	}
	go server.Accept(counted)
	return "tcp@" + listener.Addr().String(), counted
}

func TestConcurrentCallsReuseConnectionAndCloseAllowsReuse(t *testing.T) {
	address, listener := liveEndpoint(t)
	defer listener.Close()
	client := NewXClient(discovery.NewMultiServerDiscovery([]string{address}), discovery.RandomSelect, nil)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var jobs sync.WaitGroup
	for i := 0; i < 16; i++ {
		jobs.Add(1)
		go func() {
			defer jobs.Done()
			var reply int
			if err := client.Call(ctx, "CacheService.Sum", 41, &reply); err != nil || reply != 42 {
				t.Errorf("reply=%d error=%v", reply, err)
			}
		}()
	}
	jobs.Wait()
	if got := atomic.LoadInt32(&listener.connections); got != 1 {
		t.Fatalf("expected one shared connection, got %d", got)
	}
	client.Close()
	var reply int
	if err := client.Call(ctx, "CacheService.Sum", 41, &reply); err != nil || reply != 42 {
		t.Fatalf("reuse after Close: %d %v", reply, err)
	}
	if got := atomic.LoadInt32(&listener.connections); got != 2 {
		t.Fatalf("expected a fresh connection after Close, got %d", got)
	}
	if err := client.Broadcast(ctx, "CacheService.Sum", 41, nil); err != nil {
		t.Fatalf("broadcast with discarded reply: %v", err)
	}
}

func TestSlowEndpointDoesNotBlockAnotherEndpoint(t *testing.T) {
	fast, listener := liveEndpoint(t)
	defer listener.Close()
	slow, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	accepted := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	go func() {
		conn, err := slow.Accept()
		if err != nil {
			return
		}
		close(accepted)
		<-release
		conn.Close()
	}()
	client := NewXClient(discovery.NewMultiServerDiscovery(nil), discovery.RandomSelect, &joeyrpc.Option{ConnectTimeout: 5 * time.Second})
	defer client.Close()
	slowResult := make(chan error, 1)
	go func() { _, err := client.dial("http@" + slow.Addr().String()); slowResult <- err }()
	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("slow endpoint did not accept")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	completed := make(chan error, 1)
	go func() {
		var reply int
		err := client.call(fast, ctx, "CacheService.Sum", 41, &reply)
		if err == nil && reply != 42 {
			err = errors.New("wrong reply")
		}
		completed <- err
	}()
	select {
	case err := <-completed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("slow endpoint blocked the other connection")
	}
	unblock()
	select {
	case <-slowResult:
	case <-time.After(5 * time.Second):
		t.Fatal("slow dial did not end")
	}
}

func TestCloseDuringDialDoesNotCacheLateConnection(t *testing.T) {
	slow, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	accepted := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	go func() {
		conn, err := slow.Accept()
		if err != nil {
			return
		}
		close(accepted)
		<-release
		conn.Close()
	}()
	client := NewXClient(discovery.NewMultiServerDiscovery(nil), discovery.RandomSelect, &joeyrpc.Option{ConnectTimeout: 5 * time.Second})
	result := make(chan error, 1)
	go func() { _, err := client.dial("http@" + slow.Addr().String()); result <- err }()
	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("dial did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- client.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close waited for the network handshake")
	}
	unblock()
	select {
	case err := <-result:
		if !errors.Is(err, joeyrpc.ErrShutdown) {
			t.Fatalf("late constructor error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("late constructor did not return")
	}
	client.mu.Lock()
	count := len(client.connections)
	client.mu.Unlock()
	if count != 0 {
		t.Fatal("closed cache was repopulated")
	}
}
