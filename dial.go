// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package joeyrpc

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

type newClientFunc func(net.Conn, *Option) (*Client, error)
type clientResult struct {
	client *Client
	err    error
}

type connectTimeoutError struct{ duration time.Duration }

func (e *connectTimeoutError) Error() string {
	return fmt.Sprintf("rpc client: connect timeout: expect within %s", e.duration)
}
func (e *connectTimeoutError) Timeout() bool   { return true }
func (e *connectTimeoutError) Temporary() bool { return true }

func dialTimeout(build newClientFunc, network, address string, opts ...*Option) (*Client, error) {
	return dialTimeoutContext(context.Background(), build, network, address, opts...)
}

func dialTimeoutContext(ctx context.Context, build newClientFunc, network, address string, opts ...*Option) (*Client, error) {
	settings, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	dialer := net.Dialer{Timeout: settings.ConnectTimeout}
	conn, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	if settings.ConnectTimeout == 0 && ctx.Done() == nil {
		client, err := build(conn, settings)
		if err != nil {
			_ = conn.Close()
		}
		return client, err
	}
	// The ownership lock decides whether the caller or a late constructor
	// owns the connection. A timed-out constructor never blocks on a send.
	result := make(chan clientResult, 1)
	var ownership sync.Mutex
	expired := false
	go func() {
		client, err := build(conn, settings)
		ownership.Lock()
		if expired {
			ownership.Unlock()
			if client != nil {
				_ = client.Close()
			}
		} else {
			result <- clientResult{client, err}
			ownership.Unlock()
		}
	}()
	var timeout <-chan time.Time
	if settings.ConnectTimeout != 0 {
		timer := time.NewTimer(settings.ConnectTimeout)
		defer timer.Stop()
		timeout = timer.C
	}
	abandon := func() {
		ownership.Lock()
		expired = true
		_ = conn.Close()
		select {
		case value := <-result:
			if value.client != nil {
				_ = value.client.Close()
			}
		default:
		}
		ownership.Unlock()
	}
	select {
	case value := <-result:
		if value.err != nil {
			_ = conn.Close()
		}
		return value.client, value.err
	case <-timeout:
		abandon()
		return nil, &connectTimeoutError{duration: settings.ConnectTimeout}
	case <-ctx.Done():
		abandon()
		return nil, ctx.Err()
	}
}

func Dial(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewClient, network, address, opts...)
}

// DialWithOptions configures client-side interceptors without changing wire options.
func DialWithOptions(network, address string, local ClientOptions, opts ...*Option) (*Client, error) {
	interceptors := append([]ClientInterceptor(nil), local.Interceptors...)
	build := func(conn net.Conn, opt *Option) (*Client, error) {
		client, err := NewClient(conn, opt)
		if client != nil {
			client.interceptors = interceptors
		}
		return client, err
	}
	return dialTimeout(build, network, address, opts...)
}

// XDial accepts the established protocol@address notation.
func XDial(address string, opts ...*Option) (*Client, error) {
	return XDialContext(context.Background(), address, ClientOptions{}, opts...)
}

// XDialContext also cancels network dialing and RPC initialization.
func XDialContext(ctx context.Context, address string, local ClientOptions, opts ...*Option) (*Client, error) {
	separator := strings.IndexByte(address, '@')
	if separator < 0 || strings.Contains(address[separator+1:], "@") {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", address)
	}
	network, endpoint := address[:separator], address[separator+1:]
	constructor := newClientFunc(NewClient)
	if network == "http" {
		network = "tcp"
		constructor = NewHTTPClient
	}
	interceptors := append([]ClientInterceptor(nil), local.Interceptors...)
	build := func(conn net.Conn, opt *Option) (*Client, error) {
		client, err := constructor(conn, opt)
		if client != nil {
			client.interceptors = interceptors
		}
		return client, err
	}
	return dialTimeoutContext(ctx, build, network, endpoint, opts...)
}
