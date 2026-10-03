package joeyrpc

import (
	"fmt"
	"reflect"
	"sync/atomic"
	"unicode"
	"unicode/utf8"
)

var errorType = reflect.TypeOf((*error)(nil)).Elem()

type methodType struct {
	numCalls  uint64
	ArgType   reflect.Type
	ReplyType reflect.Type
	invoke    reflect.Value
}

func (m *methodType) NumCalls() uint64 { return atomic.LoadUint64(&m.numCalls) }

func (m *methodType) newArgv() reflect.Value {
	if m.ArgType.Kind() == reflect.Ptr {
		return reflect.New(m.ArgType.Elem())
	}
	return reflect.New(m.ArgType).Elem()
}

func (m *methodType) newReplyv() reflect.Value {
	destination := reflect.New(m.ReplyType.Elem())
	switch destination.Elem().Kind() {
	case reflect.Map:
		destination.Elem().Set(reflect.MakeMap(destination.Elem().Type()))
	case reflect.Slice:
		destination.Elem().Set(reflect.MakeSlice(destination.Elem().Type(), 0, 0))
	}
	return destination
}

type service struct {
	name   string
	method map[string]*methodType
	err    error
}

func exportedName(name string) bool {
	first, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(first)
}

func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.PkgPath() == "" || exportedName(t.Name())
}

// Bind methods once at registration instead of assembling receiver arguments
// for every invocation. Invalid receivers become registration errors.
func newService(receiver interface{}) *service {
	s := &service{method: make(map[string]*methodType)}
	value := reflect.ValueOf(receiver)
	if !value.IsValid() || (value.Kind() == reflect.Ptr && value.IsNil()) {
		s.err = fmt.Errorf("rpc server: invalid service receiver")
		return s
	}
	receiverType := value.Type()
	for receiverType.Kind() == reflect.Ptr {
		receiverType = receiverType.Elem()
	}
	s.name = receiverType.Name()
	if !exportedName(s.name) {
		s.err = fmt.Errorf("rpc server: %s is not a valid service name", s.name)
		return s
	}
	for index := 0; index < value.NumMethod(); index++ {
		bound := value.Method(index)
		signature := bound.Type()
		if signature.NumIn() != 2 || signature.NumOut() != 1 || signature.Out(0) != errorType {
			continue
		}
		argument, reply := signature.In(0), signature.In(1)
		if reply.Kind() != reflect.Ptr || !isExportedOrBuiltinType(argument) || !isExportedOrBuiltinType(reply) {
			continue
		}
		name := value.Type().Method(index).Name
		s.method[name] = &methodType{ArgType: argument, ReplyType: reply, invoke: bound}
	}
	return s
}

func (s *service) call(method *methodType, argument, reply reflect.Value) (err error) {
	atomic.AddUint64(&method.numCalls, 1)
	defer func() {
		if failure := recover(); failure != nil {
			err = fmt.Errorf("rpc server: method panic: %v", failure)
		}
	}()
	result := method.invoke.Call([]reflect.Value{argument, reply})[0]
	if !result.IsNil() {
		return result.Interface().(error)
	}
	return nil
}
