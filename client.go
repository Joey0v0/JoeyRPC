// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package joeyrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/Joey0v0/JoeyRPC/codec"
)

// Call holds the arguments, destination, and completion notification of one request.
type Call struct {
	Seq           uint64
	ServiceMethod string
	Args          interface{}
	Reply         interface{}
	Error         error
	Done          chan *Call
}

func (call *Call) done() { call.Done <- call }

// A pendingCall owns access to the reply until it completes or is abandoned.
// Cancellation takes the same lock as decoding so a reply cannot be changed
// after a synchronous Call returns.
type pendingCall struct {
	mu      sync.Mutex
	call    *Call
	settled bool
}

func (p *pendingCall) fail(err error) {
	p.mu.Lock()
	if p.settled {
		p.mu.Unlock()
		return
	}
	p.settled = true
	p.call.Error = err
	p.mu.Unlock()
	p.call.done()
}

func (p *pendingCall) abandon() {
	p.mu.Lock()
	p.settled = true
	p.mu.Unlock()
}

func (p *pendingCall) decode(cc codec.Codec, header codec.Header) error {
	p.mu.Lock()
	if p.settled {
		p.mu.Unlock()
		return cc.ReadBody(nil)
	}
	var readErr error
	if header.Error != "" {
		p.call.Error = errors.New(header.Error)
		readErr = cc.ReadBody(nil)
	} else {
		readErr = cc.ReadBody(p.call.Reply)
		if readErr != nil {
			p.call.Error = fmt.Errorf("reading body %w", readErr)
		}
	}
	p.settled = true
	p.mu.Unlock()
	p.call.done()
	return readErr
}

type clientState uint8

const (
	clientReady clientState = iota
	clientClosed
	clientDisconnected
)

// Client multiplexes requests on one connection. Writes are serialized;
// outstanding calls and connection lifecycle are protected separately.
type Client struct {
	cc      codec.Codec
	writeMu sync.Mutex
	stateMu sync.Mutex
	state   clientState
	nextSeq uint64
	pending map[uint64]*pendingCall
}

var _ io.Closer = (*Client)(nil)
var ErrShutdown = errors.New("connection is shut down")

func (c *Client) IsAvailable() bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.state == clientReady
}

func (c *Client) Close() error {
	c.stateMu.Lock()
	if c.state == clientClosed {
		c.stateMu.Unlock()
		return ErrShutdown
	}
	c.state = clientClosed
	c.stateMu.Unlock()
	err := c.cc.Close()
	c.disconnect(ErrShutdown)
	return err
}

func (c *Client) detach(seq uint64) *pendingCall {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	p := c.pending[seq]
	delete(c.pending, seq)
	return p
}

func (c *Client) disconnect(err error) {
	c.stateMu.Lock()
	if c.state == clientReady {
		c.state = clientDisconnected
	}
	abandoned := c.pending
	c.pending = make(map[uint64]*pendingCall)
	c.stateMu.Unlock()
	for _, p := range abandoned {
		p.fail(err)
	}
}

func (c *Client) submit(method string, args, reply interface{}, done chan *Call) *pendingCall {
	if done == nil {
		done = make(chan *Call, 10)
	}
	if cap(done) == 0 {
		panic("rpc client: done channel is unbuffered")
	}
	p := &pendingCall{call: &Call{ServiceMethod: method, Args: args, Reply: reply, Done: done}}
	c.writeMu.Lock()
	c.stateMu.Lock()
	if c.state != clientReady {
		c.stateMu.Unlock()
		c.writeMu.Unlock()
		p.fail(ErrShutdown)
		return p
	}
	p.call.Seq = c.nextSeq
	c.nextSeq++
	c.pending[p.call.Seq] = p
	c.stateMu.Unlock()
	header := codec.Header{ServiceMethod: method, Seq: p.call.Seq}
	err := c.cc.Write(&header, args)
	c.writeMu.Unlock()
	if err != nil {
		if failed := c.detach(p.call.Seq); failed != nil {
			failed.fail(err)
		}
	}
	return p
}

func (c *Client) readResponses() {
	for {
		var header codec.Header
		err := c.cc.ReadHeader(&header)
		if err == nil {
			if p := c.detach(header.Seq); p != nil {
				err = p.decode(c.cc, header)
			} else {
				err = c.cc.ReadBody(nil)
			}
		}
		if err != nil {
			_ = c.cc.Close()
			c.disconnect(err)
			return
		}
	}
}

// Go submits a request and returns without waiting for its response.
func (c *Client) Go(method string, args, reply interface{}, done chan *Call) *Call {
	return c.submit(method, args, reply, done).call
}

// Call waits for the response or cancellation. Cancellation is local to this client.
func (c *Client) Call(ctx context.Context, method string, args, reply interface{}) error {
	p := c.submit(method, args, reply, make(chan *Call, 1))
	select {
	case completed := <-p.call.Done:
		return completed.Error
	case <-ctx.Done():
		c.detach(p.call.Seq)
		p.abandon()
		return fmt.Errorf("rpc client: call failed: %w", ctx.Err())
	}
}

// NewClient sends the JSON connection options before starting the response reader.
func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	if opt == nil {
		opt = DefaultOption
	}
	settings := *opt
	factory := codec.NewCodecFuncMap[settings.CodecType]
	if factory == nil {
		return nil, fmt.Errorf("invalid codec type %s", settings.CodecType)
	}
	if err := json.NewEncoder(conn).Encode(&settings); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return newClientCodec(factory(conn), &settings), nil
}

func newClientCodec(cc codec.Codec, opt *Option) *Client {
	c := &Client{cc: cc, nextSeq: 1, pending: make(map[uint64]*pendingCall)}
	go c.readResponses()
	return c
}
