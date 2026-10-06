//go:build windows
// +build windows

package main

import (
	"syscall"
	"time"
	"unsafe"
)

type stamp int64

var (
	qpcDLL       = syscall.NewLazyDLL("kernel32.dll")
	qpcCounter   = qpcDLL.NewProc("QueryPerformanceCounter")
	qpcFrequency = qpcDLL.NewProc("QueryPerformanceFrequency")
	frequency    = readFrequency()
)

func readFrequency() int64 {
	var value int64
	ok, _, err := qpcFrequency.Call(uintptr(unsafe.Pointer(&value)))
	if ok == 0 || value <= 0 {
		panic("QueryPerformanceFrequency: " + err.Error())
	}
	return value
}

func startStamp() stamp {
	var value int64
	ok, _, err := qpcCounter.Call(uintptr(unsafe.Pointer(&value)))
	if ok == 0 {
		panic("QueryPerformanceCounter: " + err.Error())
	}
	return stamp(value)
}

func elapsedStamp(start stamp) time.Duration {
	return time.Duration(float64(int64(startStamp()-start)) * float64(time.Second) / float64(frequency))
}
