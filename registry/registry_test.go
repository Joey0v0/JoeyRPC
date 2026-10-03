package registry

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHeartbeatAndExpiration(t *testing.T) {
	r := New(time.Minute)
	ts := httptest.NewServer(r)
	defer ts.Close()
	if err := sendHeartbeat(ts.URL, "tcp@live"); err != nil {
		t.Fatal(err)
	}
	r.putServer("tcp@expired")
	r.mu.Lock()
	r.servers["tcp@expired"].start = time.Now().Add(-2 * time.Minute)
	r.mu.Unlock()
	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("X-Geerpc-Servers"); got != "tcp@live" {
		t.Fatalf("live server list: %q", got)
	}
	r.mu.Lock()
	_, exists := r.servers["tcp@expired"]
	r.mu.Unlock()
	if exists {
		t.Fatal("expired server was not removed")
	}
}
