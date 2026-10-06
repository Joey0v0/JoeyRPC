package discovery

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const defaultUpdateTimeout = 10 * time.Second

// RegistryDiscovery serializes remote refreshes separately from list access.
// An explicit Update wins over a refresh that was already in flight.
type RegistryDiscovery struct {
	*MultiServersDiscovery
	registry    string
	timeout     time.Duration
	lastUpdate  time.Time
	revision    uint64
	refreshGate chan struct{}
}

var _ Discovery = (*RegistryDiscovery)(nil)

func NewRegistryDiscovery(address string, timeout time.Duration) *RegistryDiscovery {
	if timeout == 0 {
		timeout = defaultUpdateTimeout
	}
	return &RegistryDiscovery{MultiServersDiscovery: NewMultiServerDiscovery(nil), registry: address, timeout: timeout, refreshGate: make(chan struct{}, 1)}
}

func (d *RegistryDiscovery) Update(servers []string) error {
	snapshot := append([]string(nil), servers...)
	weights := make([]int64, len(snapshot))
	for i := range weights {
		weights[i] = 1
	}
	d.mu.Lock()
	d.setWeighted(snapshot, weights)
	d.lastUpdate = time.Now()
	d.revision++
	d.mu.Unlock()
	return nil
}

// UpdateWeighted manually replaces the cached registry snapshot.
func (d *RegistryDiscovery) UpdateWeighted(servers []Server) error {
	addresses, weights, err := checkedServers(servers)
	if err != nil {
		return err
	}
	d.mu.Lock()
	d.setWeighted(addresses, weights)
	d.lastUpdate = time.Now()
	d.revision++
	d.mu.Unlock()
	return nil
}

func (d *RegistryDiscovery) Refresh() error {
	return d.RefreshContext(context.Background())
}

func (d *RegistryDiscovery) RefreshContext(ctx context.Context) error {
	select {
	case d.refreshGate <- struct{}{}:
		defer func() { <-d.refreshGate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.RLock()
	fresh := d.lastUpdate.Add(d.timeout).After(time.Now())
	revision := d.revision
	d.mu.RUnlock()
	if fresh {
		return nil
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, d.registry, nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
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
	weights := make([]int64, len(addresses))
	for i := range weights {
		weights[i] = 1
	}
	if raw := response.Header.Get("X-Joeyrpc-Weights"); raw != "" {
		parts := strings.Split(raw, ",")
		if len(parts) != len(addresses) {
			return fmt.Errorf("rpc registry: weight count does not match server count")
		}
		for i, part := range parts {
			weight, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil || weight < 1 || weight > 10000 {
				return fmt.Errorf("rpc registry: invalid weight for server %d", i)
			}
			weights[i] = int64(weight)
		}
	}
	d.mu.Lock()
	if d.revision == revision {
		d.setWeighted(addresses, weights)
		d.lastUpdate = time.Now()
		d.revision++
	}
	d.mu.Unlock()
	return nil
}

func (d *RegistryDiscovery) Get(mode SelectMode) (string, error) {
	return d.GetContext(context.Background(), mode)
}

func (d *RegistryDiscovery) GetContext(ctx context.Context, mode SelectMode) (string, error) {
	if err := d.RefreshContext(ctx); err != nil {
		return "", err
	}
	return d.MultiServersDiscovery.Get(mode)
}

func (d *RegistryDiscovery) GetExcludingContext(ctx context.Context, mode SelectMode, excluded string) (string, error) {
	if err := d.RefreshContext(ctx); err != nil {
		return "", err
	}
	return d.MultiServersDiscovery.GetExcluding(mode, excluded)
}
func (d *RegistryDiscovery) GetAll() ([]string, error) {
	return d.GetAllContext(context.Background())
}

func (d *RegistryDiscovery) GetAllContext(ctx context.Context) ([]string, error) {
	if err := d.RefreshContext(ctx); err != nil {
		return nil, err
	}
	return d.MultiServersDiscovery.GetAll()
}
