// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package joeyrpc

import (
	"errors"
	"time"

	"github.com/Joey0v0/JoeyRPC/codec"
)

// MagicNumber and Option fields are part of the existing wire protocol.
const MagicNumber = 0x3bef5c

type Option struct {
	MagicNumber    int
	CodecType      codec.Type
	ConnectTimeout time.Duration
	HandleTimeout  time.Duration
}

var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.GobType,
	ConnectTimeout: 10 * time.Second,
}

// Each connection gets a value snapshot; normalizing one client's options must
// not mutate a shared Option while another goroutine is dialing.
func parseOptions(opts ...*Option) (*Option, error) {
	settings := *DefaultOption
	if len(opts) == 0 || opts[0] == nil {
		return &settings, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	settings = *opts[0]
	settings.MagicNumber = DefaultOption.MagicNumber
	if settings.CodecType == "" {
		settings.CodecType = DefaultOption.CodecType
	}
	return &settings, nil
}
