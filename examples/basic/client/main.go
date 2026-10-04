package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/Joey0v0/JoeyRPC"
)

type Args struct{ A, B int }

func main() {
	address := flag.String("addr", "127.0.0.1:8000", "RPC server address")
	a := flag.Int("a", 10, "first number")
	b := flag.Int("b", 20, "second number")
	flag.Parse()
	client, err := joeyrpc.Dial("tcp", *address)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var reply int
	if err := client.Call(ctx, "Calculator.Sum", Args{A: *a, B: *b}, &reply); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%d + %d = %d\n", *a, *b, reply)
}
