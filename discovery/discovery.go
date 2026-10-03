// Package discovery selects RPC endpoints without owning RPC connections.
package discovery

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

type SelectMode int

const (
	RandomSelect SelectMode = iota
	RoundRobinSelect
)

type Discovery interface {
	Refresh() error
	Update([]string) error
	Get(SelectMode) (string, error)
	GetAll() ([]string, error)
}

// MultiServersDiscovery owns a snapshot of the caller's endpoint list.
type MultiServersDiscovery struct {
	mu      sync.RWMutex
	servers []string
	random  *rand.Rand
	cursor  uint64
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
	snapshot := append([]string(nil), servers...)
	d.mu.Lock()
	d.servers = snapshot
	d.mu.Unlock()
	return nil
}

func (d *MultiServersDiscovery) Get(mode SelectMode) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	count := len(d.servers)
	if count == 0 {
		return "", errors.New("rpc discovery: no available servers")
	}
	var position int
	switch mode {
	case RandomSelect:
		if d.random == nil {
			d.random = rand.New(rand.NewSource(time.Now().UnixNano()))
		}
		position = d.random.Intn(count)
	case RoundRobinSelect:
		position = int(d.cursor % uint64(count))
		d.cursor = uint64((position + 1) % count)
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
