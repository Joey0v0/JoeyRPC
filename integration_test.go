package joeyrpc_test

import (
	"context"
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Joey0v0/JoeyRPC"
	"github.com/Joey0v0/JoeyRPC/discovery"
	"github.com/Joey0v0/JoeyRPC/registry"
	"github.com/Joey0v0/JoeyRPC/xclient"
)

type Arithmetic struct{ calls int32 }
type SumArgs struct{ A, B int }

func (a *Arithmetic) Sum(args SumArgs, reply *int) error {
	atomic.AddInt32(&a.calls, 1)
	*reply = args.A + args.B
	return nil
}

func (a *Arithmetic) Fail(args SumArgs, reply *int) error {
	return errors.New("intentional service error")
}

func startRPC(t *testing.T, service *Arithmetic) (string, func()) {
	t.Helper()
	server := joeyrpc.NewServer()
	if err := server.Register(service); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go server.Accept(l)
	return "tcp@" + l.Addr().String(), func() { _ = l.Close() }
}

func TestTCPConcurrentAndAsyncCalls(t *testing.T) {
	address, stop := startRPC(t, &Arithmetic{})
	defer stop()
	client, err := joeyrpc.XDial(address)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			var reply int
			if err := client.Call(ctx, "Arithmetic.Sum", SumArgs{n, 2}, &reply); err != nil {
				t.Errorf("call %d: %v", n, err)
			} else if reply != n+2 {
				t.Errorf("call %d: got %d", n, reply)
			}
		}(i)
	}
	wg.Wait()
	var reply int
	call := client.Go("Arithmetic.Sum", SumArgs{20, 22}, &reply, nil)
	select {
	case completed := <-call.Done:
		if completed.Error != nil || reply != 42 {
			t.Fatalf("async call: reply=%d error=%v", reply, completed.Error)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := client.Call(ctx, "Arithmetic.Fail", SumArgs{}, &reply); err == nil || err.Error() != "intentional service error" {
		t.Fatalf("service error was not propagated: %v", err)
	}
	if err := client.Call(ctx, "Arithmetic.Sum", SumArgs{3, 4}, &reply); err != nil || reply != 7 {
		t.Fatalf("connection unusable after service error: reply=%d error=%v", reply, err)
	}
}

func TestHTTPTransportAndDebug(t *testing.T) {
	server := joeyrpc.NewServer()
	if err := server.Register(&Arithmetic{}); err != nil {
		t.Fatal(err)
	}
	// HandleHTTP uses the default mux; restore it before serving requests.
	originalMux := http.DefaultServeMux
	mux := http.NewServeMux()
	http.DefaultServeMux = mux
	server.HandleHTTP()
	http.DefaultServeMux = originalMux
	ts := httptest.NewServer(mux)
	defer ts.Close()
	client, err := joeyrpc.XDial("http@" + strings.TrimPrefix(ts.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var reply int
	if err := client.Call(ctx, "Arithmetic.Sum", SumArgs{5, 6}, &reply); err != nil || reply != 11 {
		t.Fatalf("HTTP call: reply=%d error=%v", reply, err)
	}
	resp, err := http.Get(ts.URL + "/debug/joeyrpc")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "Arithmetic") || !strings.Contains(string(body), "JoeyRPC Services") {
		t.Fatalf("debug page: status=%d body=%s error=%v", resp.StatusCode, body, err)
	}
}

func TestRegistryDiscoveryRoundRobinAndBroadcast(t *testing.T) {
	services := []*Arithmetic{{}, {}}
	ts := httptest.NewServer(registry.New(time.Minute))
	defer ts.Close()
	for _, service := range services {
		address, stop := startRPC(t, service)
		defer stop()
		req, err := http.NewRequest(http.MethodPost, ts.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Joeyrpc-Server", address)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("registration failed: %s", resp.Status)
		}
	}
	d := discovery.NewRegistryDiscovery(ts.URL, 0)
	xc := xclient.NewXClient(d, discovery.RoundRobinSelect, nil)
	defer xc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 4; i++ {
		var reply int
		if err := xc.Call(ctx, "Arithmetic.Sum", SumArgs{10, i}, &reply); err != nil || reply != 10+i {
			t.Fatalf("balanced call: reply=%d error=%v", reply, err)
		}
	}
	for _, service := range services {
		if got := atomic.LoadInt32(&service.calls); got != 2 {
			t.Fatalf("round robin: expected 2 calls per instance, got %d", got)
		}
	}
	var reply int
	if err := xc.Broadcast(ctx, "Arithmetic.Sum", SumArgs{20, 22}, &reply); err != nil || reply != 42 {
		t.Fatalf("broadcast: reply=%d error=%v", reply, err)
	}
	for _, service := range services {
		if got := atomic.LoadInt32(&service.calls); got != 3 {
			t.Fatalf("broadcast did not reach every instance: calls=%d", got)
		}
	}
	if err := xc.Broadcast(ctx, "Arithmetic.Fail", SumArgs{}, &reply); err == nil || err.Error() != "intentional service error" {
		t.Fatalf("broadcast error was not propagated: %v", err)
	}
}
