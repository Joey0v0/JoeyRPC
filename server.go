// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package joeyrpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/Joey0v0/JoeyRPC/codec"
)

// Server keeps an immutable method catalog for each registered receiver.
// Its zero value is ready to accept registrations.
type Server struct {
	mu           sync.RWMutex
	services     map[string]*service
	interceptors []ServerInterceptor
	lifecycle    serverLifecycle
}

func NewServer() *Server { return &Server{services: make(map[string]*service)} }

func NewServerWithOptions(options ServerOptions) *Server {
	s := NewServer()
	s.interceptors = append([]ServerInterceptor(nil), options.Interceptors...)
	return s
}

var DefaultServer = NewServer()
var invalidRequest = struct{}{}

func (s *Server) Register(receiver interface{}) error {
	catalog := newService(receiver)
	if catalog.err != nil {
		return catalog.err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.services[catalog.name]; exists {
		return errors.New("rpc: service already defined: " + catalog.name)
	}
	if s.services == nil {
		s.services = make(map[string]*service)
	}
	s.services[catalog.name] = catalog
	return nil
}

func Register(receiver interface{}) error { return DefaultServer.Register(receiver) }

func (s *Server) lookup(name string) (*service, *methodType, error) {
	separator := strings.LastIndexByte(name, '.')
	if separator < 0 {
		return nil, nil, errors.New("rpc server: service/method request ill-formed: " + name)
	}
	serviceName, methodName := name[:separator], name[separator+1:]
	s.mu.RLock()
	catalog := s.services[serviceName]
	s.mu.RUnlock()
	if catalog == nil {
		return nil, nil, errors.New("rpc server: can't find service " + serviceName)
	}
	method := catalog.method[methodName]
	if method == nil {
		return nil, nil, errors.New("rpc server: can't find method " + methodName)
	}
	return catalog, method, nil
}

// A streamReader retains bytes read ahead by a protocol adapter.
type streamReader struct {
	io.ReadWriteCloser
	input io.Reader
}

func (s *streamReader) Read(buffer []byte) (int, error) { return s.input.Read(buffer) }

func (s *Server) ServeConn(conn io.ReadWriteCloser) {
	defer conn.Close()
	tracked := s.trackConnection(conn)
	if tracked == nil {
		return
	}
	defer s.untrackConnection(tracked)
	decoder := json.NewDecoder(conn)
	var settings Option
	if err := decoder.Decode(&settings); err != nil {
		log.Printf("rpc server: options error: %v", err)
		return
	}
	if settings.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", settings.MagicNumber)
		return
	}
	factory := codec.NewCodecFuncMap[settings.CodecType]
	if factory == nil {
		log.Printf("rpc server: invalid codec type %s", settings.CodecType)
		return
	}
	input := bufio.NewReader(io.MultiReader(decoder.Buffered(), conn))
	// json.Encoder terminates Option with one newline. All remaining bytes
	// belong to the codec, including any request received in the same read.
	if next, err := input.Peek(1); err == nil && next[0] == '\n' {
		_, _ = input.ReadByte()
	}
	stream := &streamReader{ReadWriteCloser: conn, input: input}
	session := &serverSession{
		owner: s, codec: factory(stream), timeout: settings.HandleTimeout,
		responses: make(chan response), writerDone: make(chan struct{}),
		ctx: tracked.ctx, cancel: tracked.cancel,
	}
	session.run()
}

type response struct {
	header codec.Header
	body   interface{}
	finish func()
}
type invocation struct {
	ctx      context.Context
	header   codec.Header
	catalog  *service
	method   *methodType
	argument reflect.Value
	reply    reflect.Value
}

// One writer owns each response stream; request workers only enqueue results.
type serverSession struct {
	owner      *Server
	codec      codec.Codec
	timeout    time.Duration
	responses  chan response
	writerDone chan struct{}
	jobs       sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

func (session *serverSession) publish(header codec.Header, body interface{}, err error) {
	session.publishWithDone(header, body, err, nil)
}

func (session *serverSession) publishWithDone(header codec.Header, body interface{}, err error, finish func()) {
	header.Metadata = nil // Request metadata (including tokens) is not echoed.
	if err != nil {
		header.Error = err.Error()
		body = invalidRequest
	}
	select {
	case session.responses <- response{header: header, body: body, finish: finish}:
	case <-session.writerDone:
		if finish != nil {
			finish()
		}
	}
}

func (session *serverSession) writeResponses() {
	defer close(session.writerDone)
	for reply := range session.responses {
		err := session.codec.Write(&reply.header, reply.body)
		if reply.finish != nil {
			reply.finish()
		}
		if err != nil {
			log.Printf("rpc server: write response error: %v", err)
			_ = session.codec.Close()
			return
		}
	}
}

func (session *serverSession) run() {
	defer session.codec.Close()
	go session.writeResponses()
	draining := false
	for {
		var header codec.Header
		if err := session.codec.ReadHeader(&header); err != nil {
			break
		}
		catalog, method, err := session.owner.lookup(header.ServiceMethod)
		if err != nil {
			// Discard the matching body before reading the next request header.
			readErr := session.codec.ReadBody(nil)
			session.publish(header, nil, err)
			if readErr != nil {
				break
			}
			continue
		}
		request := invocation{header: header, catalog: catalog, method: method,
			argument: method.newArgv(), reply: method.newReplyv()}
		destination := request.argument
		if destination.Kind() != reflect.Ptr {
			destination = destination.Addr()
		}
		if err := session.codec.ReadBody(destination.Interface()); err != nil {
			session.publish(header, nil, err)
			continue
		}
		if !session.owner.beginRequest() {
			draining = true
			session.publish(header, nil, ErrServerShutdown)
			break
		}
		request.ctx = WithMetadata(session.ctx, Metadata(header.Metadata))
		session.jobs.Add(1)
		go session.execute(request)
	}
	if !draining {
		session.cancel()
	}
	session.jobs.Wait()
	close(session.responses)
	<-session.writerDone
}

func (session *serverSession) execute(request invocation) {
	defer session.jobs.Done()
	ctx, cancel := context.WithCancel(request.ctx)
	defer cancel()
	handler := chainServer(session.owner.interceptors, request.header.ServiceMethod,
		func(ctx context.Context, args, reply interface{}) error {
			return request.catalog.callContext(ctx, request.method, reflect.ValueOf(args), reflect.ValueOf(reply))
		})
	run := func() (err error) {
		defer session.owner.lifecycle.tasks.Done()
		defer func() {
			if failure := recover(); failure != nil {
				err = fmt.Errorf("rpc server: interceptor panic: %v", failure)
			}
		}()
		return handler(ctx, request.argument.Interface(), request.reply.Interface())
	}
	respond := func(body interface{}, err error) {
		session.publishWithDone(request.header, body, err, session.owner.lifecycle.tasks.Done)
	}
	if session.timeout == 0 {
		err := run()
		respond(request.reply.Interface(), err)
		return
	}
	result := make(chan error, 1)
	go func() { result <- run() }()
	timer := time.NewTimer(session.timeout)
	defer timer.Stop()
	select {
	case err := <-result:
		respond(request.reply.Interface(), err)
	case <-timer.C:
		cancel()
		respond(nil, fmt.Errorf("rpc server: request handle timeout: expect within %s", session.timeout))
	case <-ctx.Done():
		respond(nil, ctx.Err())
	}
}

func (s *Server) Accept(listener net.Listener) {
	s.lifecycle.mu.Lock()
	if s.lifecycle.closing {
		s.lifecycle.mu.Unlock()
		_ = listener.Close()
		return
	}
	if s.lifecycle.listeners == nil {
		s.lifecycle.listeners = make(map[net.Listener]struct{})
	}
	s.lifecycle.listeners[listener] = struct{}{}
	s.lifecycle.mu.Unlock()
	defer func() {
		s.lifecycle.mu.Lock()
		delete(s.lifecycle.listeners, listener)
		s.lifecycle.mu.Unlock()
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go s.ServeConn(conn)
	}
}
func Accept(listener net.Listener) { DefaultServer.Accept(listener) }
