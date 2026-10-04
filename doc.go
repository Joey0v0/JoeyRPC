// Package joeyrpc provides reflection-based RPC over persistent connections.
//
// Register a receiver with Server.Register and serve a listener with Server.Accept.
// Clients use Dial or DialHTTP, then Call or Go to invoke exported methods.
// The discovery, registry, and xclient packages provide optional multi-server support.
// Per-request Metadata and synchronous call interceptors are optional. XClient
// supports opt-in retries, and Server.Shutdown drains handlers and responses.
package joeyrpc
