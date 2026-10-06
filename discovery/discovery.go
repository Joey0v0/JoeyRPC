// Package discovery selects RPC endpoints without owning RPC connections.
package discovery

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type SelectMode int

const (
	RandomSelect SelectMode = iota
	RoundRobinSelect
	WeightedRoundRobinSelect
)

// Server is an endpoint with a positive, relative request weight.
type Server struct {
	Addr   string
	Weight int
}

type Discovery interface {
	Refresh() error
	Update([]string) error
	Get(SelectMode) (string, error)
	GetAll() ([]string, error)
}

// MultiServersDiscovery owns a snapshot of the caller's endpoint list.
type MultiServersDiscovery struct {
	mu           sync.RWMutex
	servers      []string
	weights      []int64
	current      []int64
	retryCurrent map[string][]int64
	random       *rand.Rand
	cursor       uint64
}

var _ Discovery = (*MultiServersDiscovery)(nil)

func NewMultiServerDiscovery(servers []string) *MultiServersDiscovery {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	d := &MultiServersDiscovery{random: random, cursor: uint64(random.Int63())}
	_ = d.Update(servers)
	return d
}
func (d *MultiServersDiscovery) Refresh() error { return nil }

func (d *MultiServersDiscovery) Update(servers []string) error {
	weighted := make([]Server, len(servers))
	for i, address := range servers {
		weighted[i] = Server{Addr: address, Weight: 1}
	}
	return d.UpdateWeighted(weighted)
}

// UpdateWeighted replaces the endpoint snapshot and resets weighted selection.
func (d *MultiServersDiscovery) UpdateWeighted(servers []Server) error {
	addresses, weights, err := checkedServers(servers)
	if err != nil {
		return err
	}
	d.mu.Lock()
	d.setWeighted(addresses, weights)
	d.mu.Unlock()
	return nil
}

func checkedServers(servers []Server) ([]string, []int64, error) {
	addresses := make([]string, len(servers))
	weights := make([]int64, len(servers))
	for i, server := range servers {
		if server.Weight < 1 || server.Weight > 10000 {
			return nil, nil, fmt.Errorf("rpc discovery: invalid server weight at index %d", i)
		}
		addresses[i], weights[i] = server.Addr, int64(server.Weight)
	}
	return addresses, weights, nil
}

// setWeighted must be called while d.mu is held.
func (d *MultiServersDiscovery) setWeighted(addresses []string, weights []int64) {
	d.servers = addresses
	d.weights = weights
	d.current = make([]int64, len(addresses))
	d.retryCurrent = nil
}

func (d *MultiServersDiscovery) Get(mode SelectMode) (string, error) {
	return d.GetExcluding(mode, "")
}

// GetExcluding selects from the current snapshot, omitting one failed endpoint
// when alternatives exist. Weighted retries keep a separate smooth-round-robin
// cursor so they do not disturb ordinary calls.
func (d *MultiServersDiscovery) GetExcluding(mode SelectMode, excluded string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	count := len(d.servers)
	if count == 0 {
		return "", errors.New("rpc discovery: no available servers")
	}
	eligible := make([]int, 0, count)
	for i, address := range d.servers {
		if address != excluded {
			eligible = append(eligible, i)
		}
	}
	if len(eligible) == 0 {
		for i := range d.servers {
			eligible = append(eligible, i)
		}
	}
	var position int
	switch mode {
	case RandomSelect:
		if d.random == nil {
			d.random = rand.New(rand.NewSource(time.Now().UnixNano()))
		}
		position = eligible[d.random.Intn(len(eligible))]
	case RoundRobinSelect:
		position = eligible[int(d.cursor%uint64(len(eligible)))]
		d.cursor++
	case WeightedRoundRobinSelect:
		current := d.current
		if excluded != "" && len(eligible) != count {
			if d.retryCurrent == nil {
				d.retryCurrent = make(map[string][]int64)
			}
			current = d.retryCurrent[excluded]
			if current == nil {
				current = make([]int64, count)
				d.retryCurrent[excluded] = current
			}
		}
		var total int64
		for _, i := range eligible {
			current[i] += d.weights[i]
			total += d.weights[i]
			if total == d.weights[i] || current[i] > current[position] {
				position = i
			}
		}
		current[position] -= total
	default:
		return "", errors.New("rpc discovery: not supported select mode")
	}
	return d.servers[position], nil
}

func (d *MultiServersDiscovery) GetAll() ([]string, error) {
	d.mu.RLock()
	snapshot := make([]string, len(d.servers))
	copy(snapshot, d.servers)
	d.mu.RUnlock()
	return snapshot, nil
}
