// Package registry implements the JoeyRPC HTTP endpoint registration protocol.
package registry

import (
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultPath    = "/_joeyrpc_/registry"
	defaultTimeout = 5 * time.Minute
)

// ServerItem records one endpoint and its most recent renewal.
type ServerItem struct {
	Addr  string
	start time.Time
}
type Registry struct {
	mu      sync.Mutex
	timeout time.Duration
	servers map[string]*ServerItem
}

func New(timeout time.Duration) *Registry {
	return &Registry{timeout: timeout, servers: make(map[string]*ServerItem)}
}

var DefaultRegistry = New(defaultTimeout)

func (r *Registry) putServer(address string) {
	renewed := &ServerItem{Addr: address, start: time.Now()}
	r.mu.Lock()
	if r.servers == nil {
		r.servers = make(map[string]*ServerItem)
	}
	r.servers[address] = renewed
	r.mu.Unlock()
}

func (r *Registry) aliveServers() []string {
	now := time.Now()
	r.mu.Lock()
	addresses := make([]string, 0, len(r.servers))
	for address, lease := range r.servers {
		expired := r.timeout != 0 && !lease.start.Add(r.timeout).After(now)
		if expired {
			delete(r.servers, address)
		} else {
			addresses = append(addresses, address)
		}
	}
	r.mu.Unlock()
	sort.Strings(addresses)
	return addresses
}

func (r *Registry) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodGet {
		w.Header().Set("X-Joeyrpc-Servers", strings.Join(r.aliveServers(), ","))
		return
	}
	if request.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	address := request.Header.Get("X-Joeyrpc-Server")
	if address == "" {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	r.putServer(address)
}

func (r *Registry) HandleHTTP(path string) { http.Handle(path, r) }
func HandleHTTP()                          { DefaultRegistry.HandleHTTP(defaultPath) }
