package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Joey0v0/JoeyRPC"
)

type Calculator struct{}
type Args struct{ A, B int }

func (Calculator) Sum(args Args, reply *int) error {
	*reply = args.A + args.B
	return nil
}

func main() {
	address := flag.String("addr", "127.0.0.1:8000", "RPC listen address")
	flag.Parse()
	server := joeyrpc.NewServer()
	if err := server.Register(Calculator{}); err != nil {
		log.Fatal(err)
	}
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	go server.Accept(listener)
	log.Printf("JoeyRPC listening on %s; press Ctrl+C to stop", listener.Addr())
	<-signals
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("shutdown: %v", err)
		return
	}
	log.Println("server shut down gracefully")
}
