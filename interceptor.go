package joeyrpc

import "context"

type ClientInvoker func(context.Context, string, interface{}, interface{}) error
type ClientInterceptor func(context.Context, string, interface{}, interface{}, ClientInvoker) error
type ServerHandler func(context.Context, interface{}, interface{}) error
type ServerInterceptor func(context.Context, string, interface{}, interface{}, ServerHandler) error

// Local options are never sent in the connection's JSON Option.
type ClientOptions struct {
	Interceptors []ClientInterceptor
}

type ServerOptions struct {
	Interceptors []ServerInterceptor
}

func chainClient(interceptors []ClientInterceptor, invoke ClientInvoker) ClientInvoker {
	for i := len(interceptors) - 1; i >= 0; i-- {
		interceptor, next := interceptors[i], invoke
		invoke = func(ctx context.Context, method string, args, reply interface{}) error {
			return interceptor(ctx, method, args, reply, next)
		}
	}
	return invoke
}

func chainServer(interceptors []ServerInterceptor, method string, handler ServerHandler) ServerHandler {
	for i := len(interceptors) - 1; i >= 0; i-- {
		interceptor, next := interceptors[i], handler
		handler = func(ctx context.Context, args, reply interface{}) error {
			return interceptor(ctx, method, args, reply, next)
		}
	}
	return handler
}
