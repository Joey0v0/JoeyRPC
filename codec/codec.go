// Package codec defines the request/response stream used by JoeyRPC.
package codec

import "io"

// Header precedes every body. Field names and types are wire-compatible.
type Header struct {
	ServiceMethod string
	Seq           uint64
	Error         string
	Metadata      map[string]string
}

type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

type NewCodecFunc func(io.ReadWriteCloser) Codec
type Type string

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json" // Reserved; no JSON payload codec is registered.
)

// Configure factories before accepting or dialing connections.
var NewCodecFuncMap = map[Type]NewCodecFunc{GobType: NewGobCodec}
