package xclient

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"reflect"
	"time"

	"github.com/Joey0v0/JoeyRPC"
	"github.com/Joey0v0/JoeyRPC/discovery"
)

// RetryOptions is disabled when MaxAttempts <= 1. MaxAttempts includes the first
// call. Idempotent explicitly permits retry after a possibly executed request.
type RetryOptions struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
	Idempotent     func(method string) bool
}

type Options struct {
	Client joeyrpc.ClientOptions
	Retry  RetryOptions
}

type dialFailure struct{ err error }

func (e *dialFailure) Error() string { return e.err.Error() }
func (e *dialFailure) Unwrap() error { return e.err }

func networkFailure(err error) bool {
	var network net.Error
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &network)
}

func (r RetryOptions) allows(method string, err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var remote *joeyrpc.RemoteError
	if errors.As(err, &remote) {
		return false
	}
	var dial *dialFailure
	if errors.As(err, &dial) {
		return networkFailure(dial.err) // The RPC was not sent.
	}
	var transport *joeyrpc.TransportError
	return errors.As(err, &transport) && networkFailure(transport.Err) && r.Idempotent != nil && r.Idempotent(method)
}

func (x *XClient) selectAddress(ctx context.Context, previous string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if previous != "" {
		if d, ok := x.d.(interface {
			GetExcludingContext(context.Context, discovery.SelectMode, string) (string, error)
		}); ok {
			return d.GetExcludingContext(ctx, x.mode, previous)
		}
		if d, ok := x.d.(interface {
			GetExcluding(discovery.SelectMode, string) (string, error)
		}); ok {
			return d.GetExcluding(x.mode, previous)
		}
	}
	var address string
	var err error
	if d, ok := x.d.(interface {
		GetContext(context.Context, discovery.SelectMode) (string, error)
	}); ok {
		address, err = d.GetContext(ctx, x.mode)
	} else {
		address, err = x.d.Get(x.mode)
	}
	if err != nil || previous == "" || address != previous {
		return address, err
	}
	var addresses []string
	if d, ok := x.d.(interface {
		GetAllContext(context.Context) ([]string, error)
	}); ok {
		addresses, err = d.GetAllContext(ctx)
	} else {
		addresses, err = x.d.GetAll()
	}
	if err != nil {
		return "", err
	}
	var alternatives []string
	for _, candidate := range addresses {
		if candidate != previous {
			alternatives = append(alternatives, candidate)
		}
	}
	if len(alternatives) != 0 {
		return discovery.NewMultiServerDiscovery(alternatives).Get(x.mode)
	}
	return address, nil // A single node can also recover between attempts.
}

func (x *XClient) callWithRetry(ctx context.Context, method string, args, reply interface{}) error {
	policy := x.options.Retry
	if policy.MaxAttempts <= 1 {
		address, err := x.selectAddress(ctx, "")
		if err != nil {
			return err
		}
		return x.call(address, ctx, method, args, reply)
	}
	var destination reflect.Value
	if reply != nil {
		pointer := reflect.ValueOf(reply)
		if pointer.Kind() != reflect.Ptr || pointer.IsNil() {
			return errors.New("rpc client: reply must be a non-nil pointer")
		}
		destination = pointer.Elem()
	}
	delay, maxDelay, multiplier := policy.InitialBackoff, policy.MaxBackoff, policy.Multiplier
	if delay <= 0 {
		delay = 100 * time.Millisecond
	}
	if maxDelay <= 0 {
		maxDelay = time.Second
	}
	if !(multiplier >= 1) { // Also rejects NaN.
		multiplier = 2
	}
	if delay > maxDelay {
		delay = maxDelay
	}
	previous := ""
	for attempt := 0; attempt < policy.MaxAttempts; attempt++ {
		address, err := x.selectAddress(ctx, previous)
		if err != nil {
			return err
		}
		var result reflect.Value
		var target interface{}
		if destination.IsValid() {
			result = reflect.New(destination.Type())
			target = result.Interface()
		}
		err = x.call(address, ctx, method, args, target)
		if err == nil {
			if destination.IsValid() {
				destination.Set(result.Elem())
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt+1 == policy.MaxAttempts || !policy.allows(method, err) {
			return err
		}
		previous = address
		// Equal jitter keeps a minimum delay while avoiding synchronized retries.
		wait := delay/2 + time.Duration(rand.Int63n(int64(delay-delay/2)+1))
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
		if float64(delay)*multiplier >= float64(maxDelay) {
			delay = maxDelay
		} else {
			delay = time.Duration(float64(delay) * multiplier)
		}
	}
	panic("unreachable")
}
