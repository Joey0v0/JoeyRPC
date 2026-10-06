//go:build !windows
// +build !windows

package main

import "time"

type stamp time.Time

func startStamp() stamp                      { return stamp(time.Now()) }
func elapsedStamp(start stamp) time.Duration { return time.Since(time.Time(start)) }
