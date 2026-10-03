package codec

import (
	"bufio"
	"encoding/gob"
	"io"
	"sync"
)

// GobCodec maintains one Gob stream per direction. A complete header/body pair
// is encoded under one lock and flushed before Write succeeds.
type GobCodec struct {
	transport io.ReadWriteCloser
	output    *bufio.Writer
	encoder   *gob.Encoder
	decoder   *gob.Decoder
	writeMu   sync.Mutex
	closeOnce sync.Once
	closeErr  error
}

var _ Codec = (*GobCodec)(nil)

func NewGobCodec(transport io.ReadWriteCloser) Codec {
	output := bufio.NewWriter(transport)
	return &GobCodec{transport: transport, output: output,
		encoder: gob.NewEncoder(output), decoder: gob.NewDecoder(transport)}
}

func (c *GobCodec) ReadHeader(header *Header) error        { return c.decoder.Decode(header) }
func (c *GobCodec) ReadBody(destination interface{}) error { return c.decoder.Decode(destination) }
func (c *GobCodec) Close() error {
	c.closeOnce.Do(func() { c.closeErr = c.transport.Close() })
	return c.closeErr
}

func (c *GobCodec) Write(header *Header, body interface{}) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	err := c.encoder.Encode(header)
	if err == nil {
		err = c.encoder.Encode(body)
	}
	if err == nil {
		err = c.output.Flush()
	}
	if err != nil {
		_ = c.Close()
	}
	return err
}
