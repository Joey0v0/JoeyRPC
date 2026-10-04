package joeyrpc

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
)

var ErrServerShutdown = errors.New("rpc server: shutting down")

type serverConnection struct {
	transport io.ReadWriteCloser
	ctx       context.Context
	cancel    context.CancelFunc
}

type serverLifecycle struct {
	mu          sync.Mutex
	closing     bool
	listeners   map[net.Listener]struct{}
	connections map[*serverConnection]struct{}
	tasks       sync.WaitGroup
	done        chan struct{}
}

func (s *Server) trackConnection(transport io.ReadWriteCloser) *serverConnection {
	s.lifecycle.mu.Lock()
	defer s.lifecycle.mu.Unlock()
	if s.lifecycle.closing {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	conn := &serverConnection{transport: transport, ctx: ctx, cancel: cancel}
	if s.lifecycle.connections == nil {
		s.lifecycle.connections = make(map[*serverConnection]struct{})
	}
	s.lifecycle.connections[conn] = struct{}{}
	return conn
}

func (s *Server) untrackConnection(conn *serverConnection) {
	conn.cancel()
	s.lifecycle.mu.Lock()
	delete(s.lifecycle.connections, conn)
	s.lifecycle.mu.Unlock()
}

// Count the actual handler and the final response separately. A handler that
// outlives HandleTimeout is still included in graceful shutdown.
func (s *Server) beginRequest() bool {
	s.lifecycle.mu.Lock()
	defer s.lifecycle.mu.Unlock()
	if s.lifecycle.closing {
		return false
	}
	s.lifecycle.tasks.Add(2)
	return true
}

func (s *Server) closeConnections() {
	s.lifecycle.mu.Lock()
	connections := make([]*serverConnection, 0, len(s.lifecycle.connections))
	for conn := range s.lifecycle.connections {
		connections = append(connections, conn)
	}
	s.lifecycle.mu.Unlock()
	for _, conn := range connections {
		conn.cancel()
		_ = conn.transport.Close()
	}
}

// Shutdown stops admission and drains handlers and responses. Context expiry
// closes connections and cancels handler contexts; it cannot kill goroutines.
func (s *Server) Shutdown(ctx context.Context) error {
	s.lifecycle.mu.Lock()
	first := !s.lifecycle.closing
	var listeners []net.Listener
	if first {
		s.lifecycle.closing = true
		s.lifecycle.done = make(chan struct{})
		for listener := range s.lifecycle.listeners {
			listeners = append(listeners, listener)
		}
	}
	done := s.lifecycle.done
	s.lifecycle.mu.Unlock()
	if first {
		for _, listener := range listeners {
			_ = listener.Close()
		}
		go func() {
			s.lifecycle.tasks.Wait()
			s.closeConnections()
			close(done)
		}()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.closeConnections()
		return ctx.Err()
	}
}
