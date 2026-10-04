package joeyrpc

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Joey0v0/JoeyRPC/codec"
)

type ShutdownService struct {
	started  chan struct{}
	release  chan struct{}
	finished chan struct{}
}

func (s *ShutdownService) Block(arg int, reply *int) error {
	s.started <- struct{}{}
	<-s.release
	*reply = arg
	return nil
}

func (s *ShutdownService) Cooperative(ctx context.Context, arg int, reply *int) error {
	s.started <- struct{}{}
	<-ctx.Done()
	s.finished <- struct{}{}
	return ctx.Err()
}

func awaitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("signal was not delivered")
	}
}

func awaitError(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("operation did not finish")
		return nil
	}
}

func waitForShutdown(t *testing.T, server *Server) {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		server.lifecycle.mu.Lock()
		closing := server.lifecycle.closing
		server.lifecycle.mu.Unlock()
		if closing {
			return
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatal("shutdown did not start")
		}
	}
}

func TestShutdownDrainsResponseAndRejectsNewRPCOnExistingConnection(t *testing.T) {
	service := &ShutdownService{started: make(chan struct{}, 1), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(service.release) }) }
	defer release()
	server := NewServer()
	_ = server.Register(service)
	_ = server.Register(Boundary{})
	client, stop := featureClient(t, server, ClientOptions{}, nil)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var reply int
	result := make(chan error, 1)
	go func() { result <- client.Call(ctx, "ShutdownService.Block", 42, &reply) }()
	awaitSignal(t, service.started)
	shutdown := make(chan error, 1)
	go func() { shutdown <- server.Shutdown(ctx) }()
	waitForShutdown(t, server)
	var rejected int
	if err := client.Call(ctx, "Boundary.Add", 1, &rejected); err == nil {
		t.Fatal("accepted a new RPC during shutdown")
	}
	select {
	case err := <-shutdown:
		t.Fatalf("shutdown returned before handler finished: %v", err)
	default:
	}
	release()
	if err := awaitError(t, result); err != nil || reply != 42 {
		t.Fatalf("existing response was lost: %d %v", reply, err)
	}
	if err := awaitError(t, shutdown); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownWaitsForHandlerThatOutlivesHandleTimeout(t *testing.T) {
	service := &ShutdownService{started: make(chan struct{}, 1), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(service.release) }) }
	defer release()
	server := NewServer()
	_ = server.Register(service)
	client, stop := featureClient(t, server, ClientOptions{}, &Option{HandleTimeout: 15 * time.Millisecond})
	defer stop()
	var reply int
	result := make(chan error, 1)
	go func() { result <- client.Call(context.Background(), "ShutdownService.Block", 42, &reply) }()
	awaitSignal(t, service.started)
	if err := awaitError(t, result); err == nil {
		t.Fatal("expected handle timeout")
	}
	shutdown := make(chan error, 1)
	go func() { shutdown <- server.Shutdown(context.Background()) }()
	waitForShutdown(t, server)
	select {
	case err := <-shutdown:
		t.Fatalf("forgot timed-out handler: %v", err)
	default:
	}
	release()
	if err := awaitError(t, shutdown); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownDeadlineCancelsHandlerAndClosesConnection(t *testing.T) {
	service := &ShutdownService{started: make(chan struct{}, 1), finished: make(chan struct{}, 1)}
	server := NewServer()
	_ = server.Register(service)
	client, stop := featureClient(t, server, ClientOptions{}, nil)
	defer stop()
	var reply int
	result := make(chan error, 1)
	go func() { result <- client.Call(context.Background(), "ShutdownService.Cooperative", 42, &reply) }()
	awaitSignal(t, service.started)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := server.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown deadline: %v", err)
	}
	awaitSignal(t, service.finished)
	if err := awaitError(t, result); err == nil {
		t.Fatal("pending call should fail after forced close")
	}
}

func TestShutdownWaitsForResponseWrite(t *testing.T) {
	service := &ShutdownService{started: make(chan struct{}, 1), release: make(chan struct{})}
	close(service.release)
	server := NewServer()
	_ = server.Register(service)
	client, peer := net.Pipe()
	defer client.Close()
	go server.ServeConn(peer)
	var request bytes.Buffer
	if err := json.NewEncoder(&request).Encode(DefaultOption); err != nil {
		t.Fatal(err)
	}
	encoder := gob.NewEncoder(&request)
	if err := encoder.Encode(&codec.Header{ServiceMethod: "ShutdownService.Block", Seq: 1}); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Encode(42); err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(client, &request); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, service.started)
	// net.Pipe cannot finish a response write until the peer reads it. Keep the
	// peer unread to verify that shutdown counts the response, not just the handler.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := server.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown did not wait for blocked response: %v", err)
	}
}
