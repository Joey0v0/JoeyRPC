// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package joeyrpc

import (
	"bufio"
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
	mu       sync.RWMutex
	services map[string]*service
}

func NewServer() *Server { return &Server{services: make(map[string]*service)} }

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
	}
	session.run()
}

type response struct {
	header codec.Header
	body   interface{}
}
type invocation struct {
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
}

func (session *serverSession) publish(header codec.Header, body interface{}, err error) {
	if err != nil {
		header.Error = err.Error()
		body = invalidRequest
	}
	select {
	case session.responses <- response{header, body}:
	case <-session.writerDone:
	}
}

func (session *serverSession) writeResponses() {
	defer close(session.writerDone)
	for reply := range session.responses {
		if err := session.codec.Write(&reply.header, reply.body); err != nil {
			log.Printf("rpc server: write response error: %v", err)
			_ = session.codec.Close()
			return
		}
	}
}

func (session *serverSession) run() {
	defer session.codec.Close()
	go session.writeResponses()
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
		session.jobs.Add(1)
		go session.execute(request)
	}
	session.jobs.Wait()
	close(session.responses)
	<-session.writerDone
}

func (session *serverSession) execute(request invocation) {
	defer session.jobs.Done()
	if session.timeout == 0 {
		err := request.catalog.call(request.method, request.argument, request.reply)
		session.publish(request.header, request.reply.Interface(), err)
		return
	}
	result := make(chan error, 1)
	go func() { result <- request.catalog.call(request.method, request.argument, request.reply) }()
	timer := time.NewTimer(session.timeout)
	defer timer.Stop()
	select {
	case err := <-result:
		session.publish(request.header, request.reply.Interface(), err)
	case <-timer.C:
		session.publish(request.header, nil,
			fmt.Errorf("rpc server: request handle timeout: expect within %s", session.timeout))
	}
}

func (s *Server) Accept(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go s.ServeConn(conn)
	}
}
func Accept(listener net.Listener) { DefaultServer.Accept(listener) }
