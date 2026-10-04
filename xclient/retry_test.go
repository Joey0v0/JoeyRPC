package xclient

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Joey0v0/JoeyRPC"
	"github.com/Joey0v0/JoeyRPC/discovery"
)

type orderedDiscovery struct {
	mu        sync.Mutex
	addresses []string
	gets      int
}

func (d *orderedDiscovery) Refresh() error { return nil }
func (d *orderedDiscovery) Update(a []string) error {
	d.mu.Lock()
	d.addresses = append([]string(nil), a...)
	d.mu.Unlock()
	return nil
}
func (d *orderedDiscovery) Get(discovery.SelectMode) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	address := d.addresses[d.gets%len(d.addresses)]
	d.gets++
	return address, nil
}
func (d *orderedDiscovery) GetAll() ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.addresses...), nil
}
func (d *orderedDiscovery) count() int { d.mu.Lock(); defer d.mu.Unlock(); return d.gets }

type AttemptService struct{ calls int32 }

func (s *AttemptService) Sum(arg int, reply *int) error {
	atomic.AddInt32(&s.calls, 1)
	*reply = arg + 1
	return nil
}
func (s *AttemptService) Fail(arg int, reply *int) error {
	atomic.AddInt32(&s.calls, 1)
	*reply = 999
	return errors.New("business failure")
}

type droppedResponseConn struct{ net.Conn }

func (c *droppedResponseConn) Write([]byte) (int, error) {
	c.Conn.Close()
	return 0, io.ErrClosedPipe
}

type droppedResponseListener struct{ net.Listener }

func (l *droppedResponseListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &droppedResponseConn{conn}, nil
}

func retryEndpoint(t *testing.T, drop bool) (string, *AttemptService, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := joeyrpc.NewServer()
	service := &AttemptService{}
	if err := server.Register(service); err != nil {
		t.Fatal(err)
	}
	var accepting net.Listener = listener
	if drop {
		accepting = &droppedResponseListener{listener}
	}
	go server.Accept(accepting)
	return "tcp@" + listener.Addr().String(), service, func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		server.Shutdown(ctx)
		listener.Close()
	}
}

func unavailableEndpoint(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := "tcp@" + listener.Addr().String()
	listener.Close()
	return address
}

func TestRetryConnectionFailureSelectsAnotherNode(t *testing.T) {
	good, service, stop := retryEndpoint(t, false)
	defer stop()
	d := &orderedDiscovery{addresses: []string{unavailableEndpoint(t), good}}
	x := NewXClientWithOptions(d, discovery.RoundRobinSelect, nil, Options{Retry: RetryOptions{MaxAttempts: 3, InitialBackoff: time.Millisecond}})
	defer x.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var reply int
	if err := x.Call(ctx, "AttemptService.Sum", 41, &reply); err != nil || reply != 42 {
		t.Fatalf("failover: %d %v", reply, err)
	}
	if d.count() != 2 || atomic.LoadInt32(&service.calls) != 1 {
		t.Fatal("unexpected retry count")
	}
}

func TestRetryNeverRepeatsBusinessError(t *testing.T) {
	address, service, stop := retryEndpoint(t, false)
	defer stop()
	d := &orderedDiscovery{addresses: []string{address}}
	x := NewXClientWithOptions(d, discovery.RandomSelect, nil, Options{Retry: RetryOptions{
		MaxAttempts: 3, Idempotent: func(string) bool { return true },
	}})
	defer x.Close()
	reply := 17
	err := x.Call(context.Background(), "AttemptService.Fail", 0, &reply)
	var remote *joeyrpc.RemoteError
	if !errors.As(err, &remote) || d.count() != 1 || atomic.LoadInt32(&service.calls) != 1 || reply != 17 {
		t.Fatalf("business error retried or reply overwritten: %d %v", reply, err)
	}
}

func TestRetryLostResponseRequiresExplicitIdempotence(t *testing.T) {
	for _, idempotent := range []bool{false, true} {
		t.Run(map[bool]string{false: "not-idempotent", true: "idempotent"}[idempotent], func(t *testing.T) {
			bad, executed, stopBad := retryEndpoint(t, true)
			defer stopBad()
			good, succeeded, stopGood := retryEndpoint(t, false)
			defer stopGood()
			d := &orderedDiscovery{addresses: []string{bad, good}}
			policy := RetryOptions{MaxAttempts: 2, InitialBackoff: time.Millisecond}
			if idempotent {
				policy.Idempotent = func(method string) bool { return method == "AttemptService.Sum" }
			}
			x := NewXClientWithOptions(d, discovery.RoundRobinSelect, nil, Options{Retry: policy})
			defer x.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			reply := 17
			err := x.Call(ctx, "AttemptService.Sum", 41, &reply)
			if atomic.LoadInt32(&executed.calls) != 1 {
				t.Fatal("first server did not execute before losing response")
			}
			if idempotent {
				if err != nil || reply != 42 || atomic.LoadInt32(&succeeded.calls) != 1 {
					t.Fatalf("idempotent failover: %d %v", reply, err)
				}
			} else if err == nil || reply != 17 || d.count() != 1 || atomic.LoadInt32(&succeeded.calls) != 0 {
				t.Fatalf("ambiguous result was retried: %d %v", reply, err)
			}
		})
	}
}

func TestRetryBoundAndCancellationDuringBackoff(t *testing.T) {
	for _, cancelDuringWait := range []bool{false, true} {
		d := &orderedDiscovery{addresses: []string{unavailableEndpoint(t)}}
		policy := RetryOptions{MaxAttempts: 3, InitialBackoff: time.Millisecond}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if cancelDuringWait {
			cancel()
			ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
			policy.InitialBackoff, policy.MaxBackoff = time.Second, time.Second
		}
		x := NewXClientWithOptions(d, discovery.RandomSelect, nil, Options{Retry: policy})
		var reply int
		err := x.Call(ctx, "AttemptService.Sum", 41, &reply)
		x.Close()
		cancel()
		if cancelDuringWait {
			if !errors.Is(err, context.DeadlineExceeded) || d.count() != 1 {
				t.Fatalf("backoff ignored deadline: attempts=%d err=%v", d.count(), err)
			}
		} else if err == nil || d.count() != 3 {
			t.Fatalf("attempt limit: attempts=%d err=%v", d.count(), err)
		}
	}
}
