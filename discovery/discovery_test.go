package discovery

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

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

func TestRegistryRefreshCache(t *testing.T) {
	var requests int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.Header().Set("X-Geerpc-Servers", " tcp@one, ,tcp@two ")
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
