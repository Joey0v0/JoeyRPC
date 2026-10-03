package joeyrpc

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Joey0v0/JoeyRPC/codec"
)

type Boundary struct{}

func (Boundary) Add(arg int, reply *int) error  { *reply = arg + 1; return nil }
func (Boundary) Fail(arg int, reply *int) error { return errors.New("100% service failure") }
func (Boundary) Slow(arg int, reply *int) error {
	time.Sleep(time.Duration(arg) * time.Millisecond)
	*reply = arg
	return nil
}
func (Boundary) Map(arg *int, reply *map[string]int) error { (*reply)["value"] = *arg; return nil }
func (Boundary) Slice(arg int, reply *[]int) error         { *reply = append(*reply, arg); return nil }

func boundaryClient(t *testing.T, option *Option) (*Client, func()) {
	t.Helper()
	server := NewServer()
	if err := server.Register(Boundary{}); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go server.Accept(l)
	client, err := Dial("tcp", l.Addr().String(), option)
	if err != nil {
		l.Close()
		t.Fatal(err)
	}
	return client, func() { client.Close(); l.Close() }
}

func TestOptionSnapshotPreservesWireValues(t *testing.T) {
	defaults, err := parseOptions()
	if err != nil || *defaults != *DefaultOption {
		t.Fatalf("default options: %v %v", defaults, err)
	}
	input := Option{HandleTimeout: time.Second}
	settings, err := parseOptions(&input)
	if err != nil || settings.MagicNumber != MagicNumber || settings.CodecType != codec.GobType || settings.ConnectTimeout != 0 || settings.HandleTimeout != time.Second {
		t.Fatalf("normalized options: %v %v", settings, err)
	}
	if input.MagicNumber != 0 || input.CodecType != "" {
		t.Fatal("normalization mutated caller-owned options")
	}
}

func TestOptionAndGobRequestInOneWrite(t *testing.T) {
	var packet bytes.Buffer
	if err := json.NewEncoder(&packet).Encode(DefaultOption); err != nil {
		t.Fatal(err)
	}
	encoder := gob.NewEncoder(&packet)
	if err := encoder.Encode(&codec.Header{ServiceMethod: "Boundary.Add", Seq: 7}); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Encode(41); err != nil {
		t.Fatal(err)
	}
	server := NewServer()
	if err := server.Register(Boundary{}); err != nil {
		t.Fatal(err)
	}
	client, peer := net.Pipe()
	defer client.Close()
	client.SetDeadline(time.Now().Add(5 * time.Second))
	go server.ServeConn(peer)
	written := make(chan error, 1)
	go func() { _, err := io.Copy(client, &packet); written <- err }()
	decoder := gob.NewDecoder(client)
	var header codec.Header
	var reply int
	if err := decoder.Decode(&header); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Decode(&reply); err != nil {
		t.Fatal(err)
	}
	if header.Seq != 7 || header.Error != "" || reply != 42 {
		t.Fatalf("header=%+v reply=%d", header, reply)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
}

func TestRequestErrorsKeepConnectionUsable(t *testing.T) {
	client, stop := boundaryClient(t, nil)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var reply int
	if err := client.Call(ctx, "Boundary.Missing", 1, &reply); err == nil || !strings.Contains(err.Error(), "can't find method") {
		t.Fatalf("missing method: %v", err)
	}
	if err := client.Call(ctx, "Boundary.Fail", 1, &reply); err == nil || err.Error() != "100% service failure" {
		t.Fatalf("service error: %v", err)
	}
	if err := client.Call(ctx, "Boundary.Add", 41, &reply); err != nil || reply != 42 {
		t.Fatalf("subsequent call: reply=%d error=%v", reply, err)
	}
}

func TestPointerArgumentsAndContainerReplies(t *testing.T) {
	client, stop := boundaryClient(t, nil)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	value := 42
	var mapping map[string]int
	if err := client.Call(ctx, "Boundary.Map", &value, &mapping); err != nil || mapping["value"] != 42 {
		t.Fatalf("map reply: %v %v", mapping, err)
	}
	var slice []int
	if err := client.Call(ctx, "Boundary.Slice", value, &slice); err != nil || len(slice) != 1 || slice[0] != 42 {
		t.Fatalf("slice reply: %v %v", slice, err)
	}
}

func TestServerTimeoutDoesNotSendLateResponse(t *testing.T) {
	client, stop := boundaryClient(t, &Option{HandleTimeout: 20 * time.Millisecond})
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reply := -1
	if err := client.Call(ctx, "Boundary.Slow", 80, &reply); err == nil || !strings.Contains(err.Error(), "handle timeout") {
		t.Fatalf("timeout: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if reply != -1 {
		t.Fatal("timed-out call changed its reply")
	}
	if err := client.Call(ctx, "Boundary.Add", 41, &reply); err != nil || reply != 42 {
		t.Fatalf("after timeout: %d %v", reply, err)
	}
}

type blockingReplyCodec struct {
	headers   chan codec.Header
	entered   chan struct{}
	release   chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
}

func newBlockingReplyCodec() *blockingReplyCodec {
	return &blockingReplyCodec{headers: make(chan codec.Header, 1), entered: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}
}
func (c *blockingReplyCodec) Close() error { c.closeOnce.Do(func() { close(c.closed) }); return nil }
func (c *blockingReplyCodec) Write(header *codec.Header, body interface{}) error {
	c.headers <- *header
	return nil
}
func (c *blockingReplyCodec) ReadHeader(header *codec.Header) error {
	select {
	case value := <-c.headers:
		*header = value
		return nil
	case <-c.closed:
		return io.EOF
	}
}
func (c *blockingReplyCodec) ReadBody(body interface{}) error {
	close(c.entered)
	select {
	case <-c.release:
	case <-c.closed:
		return io.EOF
	}
	if body != nil {
		*body.(*int) = 42
	}
	return nil
}

func TestCancellationWaitsForActiveReplyDecode(t *testing.T) {
	stream := newBlockingReplyCodec()
	client := newClientCodec(stream, DefaultOption)
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reply := 0
	returned := make(chan error, 1)
	go func() { returned <- client.Call(ctx, "Boundary.Add", 41, &reply) }()
	select {
	case <-stream.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("reply decoder did not start")
	}
	cancel()
	select {
	case <-returned:
		t.Fatal("Call returned while the codec still owned the reply")
	case <-time.After(30 * time.Millisecond):
	}
	close(stream.release)
	select {
	case err := <-returned:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled Call did not return")
	}
	reply = 99 // The receiver has released this memory before Call returns.
}

func TestTimedOutConstructorClosesLateClient(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	release := make(chan struct{})
	stream := newBlockingReplyCodec()
	build := func(conn net.Conn, option *Option) (*Client, error) {
		defer conn.Close()
		<-release
		return &Client{cc: stream}, nil
	}
	_, err = dialTimeout(build, "tcp", listener.Addr().String(), &Option{ConnectTimeout: 20 * time.Millisecond})
	close(release)
	if err == nil || !strings.Contains(err.Error(), "connect timeout") {
		t.Fatalf("dial timeout: %v", err)
	}
	select {
	case <-stream.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("late client was not closed")
	}
}

func TestInvalidRegistrationReturnsError(t *testing.T) {
	server := NewServer()
	if err := server.Register(nil); err == nil {
		t.Fatal("nil receiver was accepted")
	}
	var receiver *Boundary
	if err := server.Register(receiver); err == nil {
		t.Fatal("typed nil receiver was accepted")
	}
	if err := server.Register(Boundary{}); err != nil {
		t.Fatal(err)
	}
	if err := server.Register(Boundary{}); err == nil {
		t.Fatal("duplicate service was accepted")
	}
}
