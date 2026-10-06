package discovery

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSmoothWeightedRoundRobin(t *testing.T) {
	d := NewMultiServerDiscovery(nil)
	if err := d.UpdateWeighted([]Server{{"A", 5}, {"B", 1}, {"C", 1}}); err != nil {
		t.Fatal(err)
	}
	want := []string{"A", "A", "B", "A", "C", "A", "A"}
	for cycle := 0; cycle < 2; cycle++ {
		got := make([]string, len(want))
		for i := range got {
			var err error
			got[i], err = d.Get(WeightedRoundRobinSelect)
			if err != nil {
				t.Fatal(err)
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("weighted sequence: %v, want %v", got, want)
		}
	}
	if err := d.UpdateWeighted([]Server{{"A", 1}, {"B", 2}}); err != nil {
		t.Fatal(err)
	}
	got := make([]string, 3)
	for i := range got {
		got[i], _ = d.Get(WeightedRoundRobinSelect)
	}
	if !reflect.DeepEqual(got, []string{"B", "A", "B"}) {
		t.Fatalf("updated weights: %v", got)
	}
}

func TestWeightedRetryExcludesFailedNode(t *testing.T) {
	d := NewMultiServerDiscovery(nil)
	if err := d.UpdateWeighted([]Server{{"A", 9}, {"B", 3}, {"C", 1}}); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for i := 0; i < 4; i++ {
		address, err := d.GetExcluding(WeightedRoundRobinSelect, "A")
		if err != nil {
			t.Fatal(err)
		}
		counts[address]++
	}
	if counts["A"] != 0 || counts["B"] != 3 || counts["C"] != 1 {
		t.Fatalf("retry distribution: %v", counts)
	}
	if err := d.UpdateWeighted([]Server{{"A", 0}}); err == nil {
		t.Fatal("zero weight accepted")
	}
}

func TestRegistryWeightedRefresh(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Joeyrpc-Servers", "A,B,C")
		w.Header().Set("X-Joeyrpc-Weights", "5,1,1")
	}))
	defer ts.Close()
	d := NewRegistryDiscovery(ts.URL, time.Minute)
	want := []string{"A", "A", "B", "A", "C", "A", "A"}
	for _, expected := range want {
		got, err := d.Get(WeightedRoundRobinSelect)
		if err != nil || got != expected {
			t.Fatalf("selected %q, want %q: %v", got, expected, err)
		}
	}
}

func TestStaticSelection(t *testing.T) {
	d := NewMultiServerDiscovery([]string{"tcp@one", "tcp@two"})
	first, err := d.Get(RoundRobinSelect)
	if err != nil {
		t.Fatal(err)
	}
	second, _ := d.Get(RoundRobinSelect)
	third, _ := d.Get(RoundRobinSelect)
	if first == second || first != third {
		t.Fatalf("invalid round robin sequence: %s, %s, %s", first, second, third)
	}
	for i := 0; i < 10; i++ {
		address, err := d.Get(RandomSelect)
		if err != nil || (address != "tcp@one" && address != "tcp@two") {
			t.Fatalf("invalid random selection: %q %v", address, err)
		}
	}
	servers, _ := d.GetAll()
	servers[0] = "changed"
	again, _ := d.GetAll()
	if again[0] == "changed" {
		t.Fatal("GetAll exposed the internal list")
	}
	if _, err := d.Get(SelectMode(99)); err == nil {
		t.Fatal("expected unsupported selection error")
	}
	if err := d.Update(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Get(RandomSelect); err == nil {
		t.Fatal("expected empty discovery error")
	}
}

func TestConcurrentRefreshSharesOneRequest(t *testing.T) {
	var requests int32
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		once.Do(func() { close(entered) })
		<-release
		w.Header().Set("X-Joeyrpc-Servers", "tcp@one")
	}))
	defer ts.Close()
	d := NewRegistryDiscovery(ts.URL, time.Minute)
	results := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() { _, err := d.GetAll(); results <- err }()
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("refresh did not reach registry")
	}
	close(release)
	for i := 0; i < 8; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Fatalf("concurrent refresh issued %d requests", got)
	}
}

func TestExplicitUpdateWinsDuringRefresh(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.Header().Set("X-Joeyrpc-Servers", "tcp@stale")
	}))
	defer ts.Close()
	defer unblock()
	d := NewRegistryDiscovery(ts.URL, time.Minute)
	refreshed := make(chan error, 1)
	go func() { refreshed <- d.Refresh() }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not start")
	}
	updated := make(chan error, 1)
	go func() { updated <- d.Update([]string{"tcp@manual"}) }()
	select {
	case err := <-updated:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("network refresh blocked an explicit update")
	}
	unblock()
	if err := <-refreshed; err != nil {
		t.Fatal(err)
	}
	servers, err := d.GetAll()
	if err != nil || len(servers) != 1 || servers[0] != "tcp@manual" {
		t.Fatalf("update was overwritten: %v %v", servers, err)
	}
}

func TestUpdateOwnsItsAddressSnapshot(t *testing.T) {
	servers := []string{"tcp@one"}
	d := NewMultiServerDiscovery(servers)
	servers[0] = "tcp@changed"
	address, err := d.Get(RandomSelect)
	if err != nil || address != "tcp@one" {
		t.Fatalf("constructor retained caller's slice: %s %v", address, err)
	}
	servers[0] = "tcp@two"
	d.Update(servers)
	servers[0] = "tcp@changed"
	address, err = d.Get(RoundRobinSelect)
	if err != nil || address != "tcp@two" {
		t.Fatalf("update retained caller's slice: %s %v", address, err)
	}
}

func TestRegistryRefreshCache(t *testing.T) {
	var requests int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.Header().Set("X-Joeyrpc-Servers", " tcp@one, ,tcp@two ")
	}))
	defer ts.Close()
	d := NewRegistryDiscovery(ts.URL, time.Minute)
	for i := 0; i < 2; i++ {
		servers, err := d.GetAll()
		if err != nil || len(servers) != 2 || servers[0] != "tcp@one" || servers[1] != "tcp@two" {
			t.Fatalf("discovered servers: %v %v", servers, err)
		}
	}
	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Fatalf("cache did not suppress repeated requests: %d", got)
	}
	d.lastUpdate = time.Now().Add(-2 * time.Minute)
	if _, err := d.Get(RandomSelect); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&requests); got != 2 {
		t.Fatalf("expired cache was not refreshed: %d", got)
	}
}
