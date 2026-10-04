// Package xclient combines endpoint discovery with reusable RPC connections.
package xclient

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/Joey0v0/JoeyRPC"
	"github.com/Joey0v0/JoeyRPC/discovery"
)

type connectionSlot struct {
	ready  chan struct{}
	client *joeyrpc.Client
	err    error
}

// XClient coordinates each endpoint separately so dialing a slow endpoint
// does not hold the cache lock or delay calls to other endpoints.
type XClient struct {
	d           discovery.Discovery
	mode        discovery.SelectMode
	opt         *joeyrpc.Option
	mu          sync.Mutex
	connections map[string]*connectionSlot
	generation  uint64
	options     Options
}

var _ io.Closer = (*XClient)(nil)

func NewXClient(d discovery.Discovery, mode discovery.SelectMode, opt *joeyrpc.Option) *XClient {
	return NewXClientWithOptions(d, mode, opt, Options{})
}

func NewXClientWithOptions(d discovery.Discovery, mode discovery.SelectMode, opt *joeyrpc.Option, options Options) *XClient {
	options.Client.Interceptors = append([]joeyrpc.ClientInterceptor(nil), options.Client.Interceptors...)
	return &XClient{d: d, mode: mode, opt: opt, connections: make(map[string]*connectionSlot), options: options}
}

func (x *XClient) Close() error {
	x.mu.Lock()
	previous := x.connections
	x.connections = make(map[string]*connectionSlot)
	x.generation++
	x.mu.Unlock()
	for _, slot := range previous {
		select {
		case <-slot.ready:
			if slot.client != nil {
				_ = slot.client.Close()
			}
		default:
			// The in-flight constructor checks the generation and closes its
			// late connection itself.
		}
	}
	return nil
}

func (x *XClient) dial(address string) (*joeyrpc.Client, error) {
	return x.dialContext(context.Background(), address)
}

func (x *XClient) dialContext(ctx context.Context, address string) (*joeyrpc.Client, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		x.mu.Lock()
		if slot := x.connections[address]; slot != nil {
			select {
			case <-slot.ready:
				if slot.client != nil && slot.client.IsAvailable() {
					client := slot.client
					x.mu.Unlock()
					return client, nil
				}
				delete(x.connections, address)
				x.mu.Unlock()
				if slot.client != nil {
					_ = slot.client.Close()
				}
				continue
			default:
				x.mu.Unlock()
				select {
				case <-slot.ready:
					return slot.client, slot.err
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
		}
		slot := &connectionSlot{ready: make(chan struct{})}
		generation := x.generation
		x.connections[address] = slot
		x.mu.Unlock()
		client, err := joeyrpc.XDialContext(ctx, address, x.options.Client, x.opt)
		x.mu.Lock()
		if generation != x.generation {
			err = joeyrpc.ErrShutdown
			if client != nil {
				_ = client.Close()
				client = nil
			}
		}
		slot.client, slot.err = client, err
		if err != nil && x.connections[address] == slot {
			delete(x.connections, address)
		}
		close(slot.ready)
		x.mu.Unlock()
		return client, err
	}
}

func (x *XClient) call(address string, ctx context.Context, method string, args, reply interface{}) error {
	client, err := x.dialContext(ctx, address)
	if err != nil {
		return &dialFailure{err: err}
	}
	return client.Call(ctx, method, args, reply)
}

func (x *XClient) Call(ctx context.Context, method string, args, reply interface{}) error {
	return x.callWithRetry(ctx, method, args, reply)
}

type broadcastResult struct {
	reply reflect.Value
	err   error
}

// Broadcast collects one result per endpoint. Only the collector writes to the
// caller's reply; it keeps the first success and returns the first failure.
func (x *XClient) Broadcast(ctx context.Context, method string, args, reply interface{}) error {
	addresses, err := x.d.GetAll()
	if err != nil {
		return err
	}
	var destination reflect.Value
	if reply != nil {
		pointer := reflect.ValueOf(reply)
		if pointer.Kind() != reflect.Ptr || pointer.IsNil() {
			return fmt.Errorf("rpc client: reply must be a non-nil pointer")
		}
		destination = pointer.Elem()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan broadcastResult, len(addresses))
	for _, address := range addresses {
		go func(endpoint string) {
			var clone reflect.Value
			var target interface{}
			if destination.IsValid() {
				clone = reflect.New(destination.Type())
				target = clone.Interface()
			}
			err := x.call(endpoint, ctx, method, args, target)
			results <- broadcastResult{reply: clone, err: err}
		}(address)
	}
	var firstError error
	delivered := false
	for range addresses {
		result := <-results
		if result.err != nil && firstError == nil {
			firstError = result.err
			cancel()
		}
		if result.err == nil && destination.IsValid() && !delivered {
			destination.Set(result.reply.Elem())
			delivered = true
		}
	}
	return firstError
}
