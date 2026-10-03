// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package joeyrpc

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
)

// The client and server share the JoeyRPC HTTP protocol names.
const (
	connected        = "200 Connected to JoeyRPC"
	defaultRPCPath   = "/_joeyrpc_"
	defaultDebugPath = "/debug/joeyrpc"
)

func (s *Server) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodConnect {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}
	hijacker, supported := w.(http.Hijacker)
	if !supported {
		http.Error(w, "rpc: HTTP connection cannot be hijacked", http.StatusInternalServerError)
		return
	}
	conn, buffers, err := hijacker.Hijack()
	if err != nil {
		log.Printf("rpc hijacking %s: %v", request.RemoteAddr, err)
		return
	}
	if _, err = io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n"); err != nil {
		_ = conn.Close()
		return
	}
	s.ServeConn(&streamReader{ReadWriteCloser: conn, input: buffers.Reader})
}

func (s *Server) HandleHTTP() {
	http.Handle(defaultRPCPath, s)
	http.Handle(defaultDebugPath, debugHTTP{s})
}
func HandleHTTP() { DefaultServer.HandleHTTP() }

type httpTunnel struct {
	net.Conn
	input *bufio.Reader
}

func (t *httpTunnel) Read(buffer []byte) (int, error) { return t.input.Read(buffer) }

func NewHTTPClient(conn net.Conn, opt *Option) (*Client, error) {
	if _, err := fmt.Fprintf(conn, "CONNECT %s HTTP/1.0\n\n", defaultRPCPath); err != nil {
		return nil, err
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		return nil, err
	}
	if response.Status != connected {
		return nil, errors.New("unexpected HTTP response: " + response.Status)
	}
	return NewClient(&httpTunnel{Conn: conn, input: reader}, opt)
}

func DialHTTP(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewHTTPClient, network, address, opts...)
}
