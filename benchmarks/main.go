// Command benchmarks measures JoeyRPC on a local TCP loopback connection.
// It is a reproducible development benchmark, not a production capacity test.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Joey0v0/JoeyRPC"
	"github.com/Joey0v0/JoeyRPC/discovery"
	"github.com/Joey0v0/JoeyRPC/xclient"
)

type EchoArgs struct{ Data []byte }
type EchoService struct{}

func (EchoService) Echo(args EchoArgs, reply *[]byte) error {
	*reply = append(*reply, args.Data...)
	return nil
}

type countingListener struct {
	net.Listener
	accepted int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		atomic.AddInt64(&l.accepted, 1)
	}
	return conn, err
}

func startServer(receiver interface{}) (*joeyrpc.Server, *countingListener, error) {
	server := joeyrpc.NewServer()
	if err := server.Register(receiver); err != nil {
		return nil, nil, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	counted := &countingListener{Listener: listener}
	go server.Accept(counted)
	return server, counted, nil
}

func stopServer(server *joeyrpc.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}

type caller interface {
	Call(context.Context, string, interface{}, interface{}) error
}

func echo(client caller, args EchoArgs) error {
	var reply []byte
	if err := client.Call(context.Background(), "EchoService.Echo", args, &reply); err != nil {
		return err
	}
	if !bytes.Equal(reply, args.Data) {
		return errors.New("echo response differs from request")
	}
	return nil
}

type timing struct {
	Samples   int     `json:"samples"`
	ElapsedMS float64 `json:"elapsed_ms"`
	QPS       float64 `json:"qps"`
	P50MS     float64 `json:"p50_ms"`
	P95MS     float64 `json:"p95_ms"`
}

func rounded(value float64) float64 { return math.Round(value*1000) / 1000 }

func summarize(samples []time.Duration, elapsed time.Duration) timing {
	ordered := append([]time.Duration(nil), samples...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	percentile := func(p float64) float64 {
		index := int(math.Ceil(p*float64(len(ordered)))) - 1
		return rounded(float64(ordered[index]) / float64(time.Millisecond))
	}
	return timing{
		Samples: len(samples), ElapsedMS: rounded(float64(elapsed) / float64(time.Millisecond)),
		QPS:   rounded(float64(len(samples)) / elapsed.Seconds()),
		P50MS: percentile(0.50), P95MS: percentile(0.95),
	}
}

func sequential(count int, operation func() error) (timing, error) {
	samples := make([]time.Duration, count)
	start := startStamp()
	for i := range samples {
		begin := startStamp()
		if err := operation(); err != nil {
			return timing{}, err
		}
		samples[i] = elapsedStamp(begin)
	}
	return summarize(samples, elapsedStamp(start)), nil
}

func concurrent(count, workers int, operation func() error) (timing, error) {
	samples := make([]time.Duration, count)
	var next int64
	var group sync.WaitGroup
	var firstErr error
	var once sync.Once
	ready := make(chan struct{})
	for i := 0; i < workers; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-ready
			for {
				index := int(atomic.AddInt64(&next, 1)) - 1
				if index >= count {
					return
				}
				begin := startStamp()
				if err := operation(); err != nil {
					once.Do(func() { firstErr = err })
				}
				samples[index] = elapsedStamp(begin)
			}
		}()
	}
	start := startStamp()
	close(ready)
	group.Wait()
	if firstErr != nil {
		return timing{}, firstErr
	}
	return summarize(samples, elapsedStamp(start)), nil
}

type reuseResult struct {
	Fresh        timing `json:"fresh_dial_call_close"`
	FreshAccepts int64  `json:"fresh_tcp_connections"`
	Reused       timing `json:"reused_call_only"`
	ReuseAccepts int64  `json:"reused_tcp_connections"`
}

func measureReuse(address string, listener *countingListener, args EchoArgs, count int) (reuseResult, error) {
	before := atomic.LoadInt64(&listener.accepted)
	fresh, err := sequential(count, func() error {
		client, err := joeyrpc.Dial("tcp", address)
		if err != nil {
			return err
		}
		defer client.Close()
		return echo(client, args)
	})
	if err != nil {
		return reuseResult{}, err
	}
	freshAccepts := atomic.LoadInt64(&listener.accepted) - before
	before = atomic.LoadInt64(&listener.accepted)
	client, err := joeyrpc.Dial("tcp", address)
	if err != nil {
		return reuseResult{}, err
	}
	defer client.Close()
	reused, err := sequential(count, func() error { return echo(client, args) })
	if err != nil {
		return reuseResult{}, err
	}
	return reuseResult{Fresh: fresh, FreshAccepts: freshAccepts,
		Reused: reused, ReuseAccepts: atomic.LoadInt64(&listener.accepted) - before}, nil
}

// offlineFirst always selects an offline endpoint first; retry can then choose
// the live endpoint from GetAll. This controls the fault instead of relying on
// random/round-robin selection.
type offlineFirst struct{ offline, live string }

func (d offlineFirst) Refresh() error                           { return nil }
func (d offlineFirst) Update([]string) error                    { return nil }
func (d offlineFirst) Get(discovery.SelectMode) (string, error) { return d.offline, nil }
func (d offlineFirst) GetAll() ([]string, error)                { return []string{d.offline, d.live}, nil }

type failoverResult struct {
	WithoutRetryFailed int    `json:"without_retry_failed"`
	RetriedSuccess     int    `json:"retried_success"`
	RetriedFailed      int    `json:"retried_failed"`
	AttemptsLimit      int    `json:"attempts_limit"`
	Backoff            string `json:"initial_backoff"`
	Timing             timing `json:"retry_call_timing"`
}

func measureFailover(args EchoArgs, count int) (failoverResult, error) {
	server, listener, err := startServer(EchoService{})
	if err != nil {
		return failoverResult{}, err
	}
	defer stopServer(server)
	offlineServer, offlineListener, err := startServer(EchoService{})
	if err != nil {
		return failoverResult{}, err
	}
	offline := "tcp@" + offlineListener.Addr().String()
	probe, err := joeyrpc.Dial("tcp", offlineListener.Addr().String())
	if err != nil {
		stopServer(offlineServer)
		return failoverResult{}, err
	}
	_ = probe.Close()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = offlineServer.Shutdown(shutdownCtx)
	cancel()
	if err != nil {
		return failoverResult{}, err
	}
	d := offlineFirst{offline: offline, live: "tcp@" + listener.Addr().String()}
	withoutRetry := xclient.NewXClient(d, discovery.RoundRobinSelect, nil)
	defer withoutRetry.Close()
	failed := 0
	for i := 0; i < count; i++ {
		if err := echo(withoutRetry, args); err == nil {
			return failoverResult{}, errors.New("offline endpoint unexpectedly succeeded without retry")
		}
		failed++
	}
	policy := xclient.RetryOptions{
		MaxAttempts: 2, InitialBackoff: 10 * time.Millisecond,
		MaxBackoff: 10 * time.Millisecond, Multiplier: 2,
		Idempotent: func(string) bool { return true },
	}
	withRetry := xclient.NewXClientWithOptions(d, discovery.RoundRobinSelect, nil, xclient.Options{Retry: policy})
	defer withRetry.Close()
	result := failoverResult{WithoutRetryFailed: failed, AttemptsLimit: 2, Backoff: "10ms with equal jitter"}
	measure, err := sequential(count, func() error {
		callCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var reply []byte
		callErr := withRetry.Call(callCtx, "EchoService.Echo", args, &reply)
		if callErr != nil || !bytes.Equal(reply, args.Data) {
			result.RetriedFailed++
			return fmt.Errorf("retry call failed: %v", callErr)
		}
		result.RetriedSuccess++
		return nil
	})
	if err != nil {
		return result, err
	}
	result.Timing = measure
	return result, nil
}

type WaitService struct {
	started   chan struct{}
	release   chan struct{}
	completed int64
}

func (s *WaitService) Wait(value int, reply *int) error {
	s.started <- struct{}{}
	<-s.release
	*reply = value + 1
	atomic.AddInt64(&s.completed, 1)
	return nil
}

type shutdownResult struct {
	InFlight            int     `json:"in_flight_at_shutdown"`
	Completed           int     `json:"completed"`
	ClientSuccess       int     `json:"client_success"`
	HoldBeforeReleaseMS int     `json:"hold_before_release_ms"`
	ShutdownMS          float64 `json:"shutdown_ms"`
}

func measureShutdown(count int) (shutdownResult, error) {
	service := &WaitService{started: make(chan struct{}, count), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(service.release) }) }
	defer release()
	server, listener, err := startServer(service)
	if err != nil {
		return shutdownResult{}, err
	}
	defer stopServer(server)
	client, err := joeyrpc.Dial("tcp", listener.Addr().String())
	if err != nil {
		return shutdownResult{}, err
	}
	defer client.Close()
	results := make(chan error, count)
	for i := 0; i < count; i++ {
		go func(value int) {
			var reply int
			callErr := client.Call(context.Background(), "WaitService.Wait", value, &reply)
			if callErr == nil && reply != value+1 {
				callErr = errors.New("incorrect shutdown response")
			}
			results <- callErr
		}(i)
	}
	readyTimeout := time.NewTimer(5 * time.Second)
	defer readyTimeout.Stop()
	for i := 0; i < count; i++ {
		select {
		case <-service.started:
		case <-readyTimeout.C:
			return shutdownResult{}, errors.New("requests did not enter the handler in time")
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdownDone := make(chan error, 1)
	start := startStamp()
	go func() { shutdownDone <- server.Shutdown(shutdownCtx) }()
	const hold = 100 * time.Millisecond
	select {
	case err := <-shutdownDone:
		return shutdownResult{}, fmt.Errorf("shutdown returned before active handlers finished: %v", err)
	case <-time.After(hold):
	}
	release()
	result := shutdownResult{InFlight: count, HoldBeforeReleaseMS: int(hold / time.Millisecond)}
	for i := 0; i < count; i++ {
		select {
		case callErr := <-results:
			if callErr != nil {
				return result, callErr
			}
			result.ClientSuccess++
		case <-shutdownCtx.Done():
			return result, shutdownCtx.Err()
		}
	}
	select {
	case err := <-shutdownDone:
		if err != nil {
			return result, err
		}
	case <-shutdownCtx.Done():
		return result, shutdownCtx.Err()
	}
	result.Completed = int(atomic.LoadInt64(&service.completed))
	result.ShutdownMS = rounded(float64(elapsedStamp(start)) / float64(time.Millisecond))
	return result, nil
}

type report struct {
	GoVersion       string         `json:"go_version"`
	OS              string         `json:"os"`
	Architecture    string         `json:"architecture"`
	LogicalCPUs     int            `json:"logical_cpus"`
	Transport       string         `json:"transport"`
	PayloadBytes    int            `json:"payload_bytes"`
	Concurrency     int            `json:"concurrency"`
	WarmupCalls     int            `json:"warmup_calls"`
	Throughput      timing         `json:"shared_connection_throughput"`
	ConnectionReuse reuseResult    `json:"connection_reuse"`
	Failover        failoverResult `json:"offline_node_retry"`
	Shutdown        shutdownResult `json:"graceful_shutdown"`
}

func run(requests, workers, payloadBytes, warmup, reuseCount, failoverCount, inflight int) (report, error) {
	args := EchoArgs{Data: bytes.Repeat([]byte("J"), payloadBytes)}
	server, listener, err := startServer(EchoService{})
	if err != nil {
		return report{}, err
	}
	defer stopServer(server)
	address := listener.Addr().String()
	client, err := joeyrpc.Dial("tcp", address)
	if err != nil {
		return report{}, err
	}
	for i := 0; i < warmup; i++ {
		if err := echo(client, args); err != nil {
			_ = client.Close()
			return report{}, err
		}
	}
	throughput, err := concurrent(requests, workers, func() error { return echo(client, args) })
	_ = client.Close()
	if err != nil {
		return report{}, err
	}
	reuse, err := measureReuse(address, listener, args, reuseCount)
	if err != nil {
		return report{}, err
	}
	failover, err := measureFailover(args, failoverCount)
	if err != nil {
		return report{}, err
	}
	shutdown, err := measureShutdown(inflight)
	if err != nil {
		return report{}, err
	}
	return report{
		GoVersion: runtime.Version(), OS: runtime.GOOS, Architecture: runtime.GOARCH,
		LogicalCPUs: runtime.NumCPU(), Transport: "single process, TCP loopback, Gob",
		PayloadBytes: payloadBytes, Concurrency: workers, WarmupCalls: warmup,
		Throughput: throughput, ConnectionReuse: reuse, Failover: failover, Shutdown: shutdown,
	}, nil
}

func main() {
	requests := flag.Int("requests", 3000, "measured throughput calls")
	workers := flag.Int("concurrency", 16, "concurrent callers sharing one connection")
	payloadBytes := flag.Int("payload", 1024, "request and response payload bytes")
	warmup := flag.Int("warmup", 100, "unmeasured calls before throughput measurement")
	reuseCount := flag.Int("reuse-samples", 200, "calls for each connection mode")
	failoverCount := flag.Int("failover-samples", 50, "calls against an offline-first endpoint list")
	inflight := flag.Int("inflight", 20, "in-flight calls held during shutdown")
	flag.Parse()
	if *requests < 1 || *workers < 1 || *payloadBytes < 1 || *warmup < 0 || *reuseCount < 1 || *failoverCount < 1 || *inflight < 1 {
		fmt.Fprintln(os.Stderr, "all counts and payload must be positive; warmup may be zero")
		os.Exit(2)
	}
	result, err := run(*requests, *workers, *payloadBytes, *warmup, *reuseCount, *failoverCount, *inflight)
	if err != nil {
		fmt.Fprintln(os.Stderr, "benchmark failed:", err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
