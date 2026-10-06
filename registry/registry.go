// Package registry implements the JoeyRPC HTTP endpoint registration protocol.
package registry

import (
	"net/http"
	"sort"
	"strconv"
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
	Addr   string
	Weight int
	start  time.Time
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
	r.putServerWeighted(address, 1)
}

func (r *Registry) putServerWeighted(address string, weight int) {
	renewed := &ServerItem{Addr: address, Weight: weight, start: time.Now()}
	r.mu.Lock()
	if r.servers == nil {
		r.servers = make(map[string]*ServerItem)
	}
	r.servers[address] = renewed
	r.mu.Unlock()
}

func (r *Registry) aliveServers() []ServerItem {
	now := time.Now()
	r.mu.Lock()
	servers := make([]ServerItem, 0, len(r.servers))
	for address, lease := range r.servers {
		expired := r.timeout != 0 && !lease.start.Add(r.timeout).After(now)
		if expired {
			delete(r.servers, address)
		} else {
			servers = append(servers, *lease)
		}
	}
	r.mu.Unlock()
	sort.Slice(servers, func(i, j int) bool { return servers[i].Addr < servers[j].Addr })
	return servers
}

func (r *Registry) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodGet {
		servers := r.aliveServers()
		addresses := make([]string, len(servers))
		weights := make([]string, len(servers))
		for i, server := range servers {
			addresses[i] = server.Addr
			weights[i] = strconv.Itoa(server.Weight)
		}
		w.Header().Set("X-Joeyrpc-Servers", strings.Join(addresses, ","))
		w.Header().Set("X-Joeyrpc-Weights", strings.Join(weights, ","))
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
	weight := 1
	if raw := request.Header.Get("X-Joeyrpc-Weight"); raw != "" {
		var err error
		weight, err = strconv.Atoi(raw)
		if err != nil || weight < 1 || weight > 10000 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}
	r.putServerWeighted(address, weight)
}

func (r *Registry) HandleHTTP(path string) { http.Handle(path, r) }
func HandleHTTP()                          { DefaultRegistry.HandleHTTP(defaultPath) }
