// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package joeyrpc

import (
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

func dialTimeout(build newClientFunc, network, address string, opts ...*Option) (*Client, error) {
	settings, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(network, address, settings.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	if settings.ConnectTimeout == 0 {
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
	timer := time.NewTimer(settings.ConnectTimeout)
	defer timer.Stop()
	select {
	case value := <-result:
		if value.err != nil {
			_ = conn.Close()
		}
		return value.client, value.err
	case <-timer.C:
		ownership.Lock()
		expired = true
		_ = conn.Close()
		// If construction won just before timeout, dispose of that client too.
		select {
		case value := <-result:
			if value.client != nil {
				_ = value.client.Close()
			}
		default:
		}
		ownership.Unlock()
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", settings.ConnectTimeout)
	}
}

func Dial(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewClient, network, address, opts...)
}

// XDial accepts the established protocol@address notation.
func XDial(address string, opts ...*Option) (*Client, error) {
	separator := strings.IndexByte(address, '@')
	if separator < 0 || strings.Contains(address[separator+1:], "@") {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", address)
	}
	network, endpoint := address[:separator], address[separator+1:]
	if network == "http" {
		return DialHTTP("tcp", endpoint, opts...)
	}
	return Dial(network, endpoint, opts...)
}
