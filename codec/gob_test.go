package codec

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

var writeFailure = errors.New("transport write failed")

type failedTransport struct{ closed bool }

func (t *failedTransport) Read([]byte) (int, error)  { return 0, io.EOF }
func (t *failedTransport) Write([]byte) (int, error) { return 0, writeFailure }
func (t *failedTransport) Close() error              { t.closed = true; return nil }

func TestFlushFailureIsReturnedAndClosesTransport(t *testing.T) {
	transport := &failedTransport{}
	stream := NewGobCodec(transport)
	err := stream.Write(&Header{ServiceMethod: "Echo.Value", Seq: 1}, 42)
	if !errors.Is(err, writeFailure) || !transport.closed {
		t.Fatalf("error=%v closed=%v", err, transport.closed)
	}
}

func TestConcurrentWritesKeepHeaderBodyPairs(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	left.SetDeadline(time.Now().Add(5 * time.Second))
	right.SetDeadline(time.Now().Add(5 * time.Second))
	writer, reader := NewGobCodec(left), NewGobCodec(right)
	var workers sync.WaitGroup
	errors := make(chan error, 16)
	for i := 1; i <= 16; i++ {
		workers.Add(1)
		go func(sequence int) {
			defer workers.Done()
			errors <- writer.Write(&Header{Seq: uint64(sequence)}, sequence)
		}(i)
	}
	for i := 0; i < 16; i++ {
		var header Header
		var body int
		if err := reader.ReadHeader(&header); err != nil {
			t.Fatal(err)
		}
		if err := reader.ReadBody(&body); err != nil {
			t.Fatal(err)
		}
		if header.Seq != uint64(body) {
			t.Fatalf("interleaved message: seq=%d body=%d", header.Seq, body)
		}
	}
	workers.Wait()
	for i := 0; i < 16; i++ {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
}
