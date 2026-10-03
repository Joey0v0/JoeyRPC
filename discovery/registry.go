package discovery

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

const defaultUpdateTimeout = 10 * time.Second

// RegistryDiscovery serializes remote refreshes separately from list access.
// An explicit Update wins over a refresh that was already in flight.
type RegistryDiscovery struct {
	*MultiServersDiscovery
	registry   string
	timeout    time.Duration
	lastUpdate time.Time
	revision   uint64
	refreshMu  sync.Mutex
}

var _ Discovery = (*RegistryDiscovery)(nil)

func NewRegistryDiscovery(address string, timeout time.Duration) *RegistryDiscovery {
	if timeout == 0 {
		timeout = defaultUpdateTimeout
	}
	return &RegistryDiscovery{MultiServersDiscovery: NewMultiServerDiscovery(nil), registry: address, timeout: timeout}
}

func (d *RegistryDiscovery) Update(servers []string) error {
	snapshot := append([]string(nil), servers...)
	d.mu.Lock()
	d.servers = snapshot
	d.lastUpdate = time.Now()
	d.revision++
	d.mu.Unlock()
	return nil
}

func (d *RegistryDiscovery) Refresh() error {
	d.refreshMu.Lock()
	defer d.refreshMu.Unlock()
	d.mu.RLock()
	fresh := d.lastUpdate.Add(d.timeout).After(time.Now())
	revision := d.revision
	d.mu.RUnlock()
	if fresh {
		return nil
	}
	response, err := http.Get(d.registry)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("rpc registry: unexpected HTTP status %s", response.Status)
	}
	var addresses []string
	for _, value := range strings.Split(response.Header.Get("X-Joeyrpc-Servers"), ",") {
		if address := strings.TrimSpace(value); address != "" {
			addresses = append(addresses, address)
		}
	}
	d.mu.Lock()
	if d.revision == revision {
		d.servers = addresses
		d.lastUpdate = time.Now()
		d.revision++
	}
	d.mu.Unlock()
	return nil
}

func (d *RegistryDiscovery) Get(mode SelectMode) (string, error) {
	if err := d.Refresh(); err != nil {
		return "", err
	}
	return d.MultiServersDiscovery.Get(mode)
}
func (d *RegistryDiscovery) GetAll() ([]string, error) {
	if err := d.Refresh(); err != nil {
		return nil, err
	}
	return d.MultiServersDiscovery.GetAll()
}
