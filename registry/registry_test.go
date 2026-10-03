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
	if got := resp.Header.Get("X-Joeyrpc-Servers"); got != "tcp@live" {
		t.Fatalf("live server list: %q", got)
	}
	r.mu.Lock()
	_, exists := r.servers["tcp@expired"]
	r.mu.Unlock()
	if exists {
		t.Fatal("expired server was not removed")
	}
}

func TestRegistryHTTPContract(t *testing.T) {
	r := New(0)
	missing := httptest.NewRecorder()
	r.ServeHTTP(missing, httptest.NewRequest(http.MethodPost, "/", nil))
	if missing.Code != http.StatusInternalServerError {
		t.Fatalf("missing address status: %d", missing.Code)
	}
	unsupported := httptest.NewRecorder()
	r.ServeHTTP(unsupported, httptest.NewRequest(http.MethodDelete, "/", nil))
	if unsupported.Code != http.StatusMethodNotAllowed {
		t.Fatalf("unsupported method status: %d", unsupported.Code)
	}
	r.putServer("tcp@two")
	r.putServer("tcp@one")
	r.mu.Lock()
	r.servers["tcp@one"].start = time.Now().Add(-24 * time.Hour)
	r.mu.Unlock()
	listed := httptest.NewRecorder()
	r.ServeHTTP(listed, httptest.NewRequest(http.MethodGet, "/", nil))
	if listed.Header().Get("X-Joeyrpc-Servers") != "tcp@one,tcp@two" {
		t.Fatalf("zero timeout/sorted list: %v", listed.Header())
	}
}

func TestHeartbeatRejectsFailedRegistration(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer ts.Close()
	if err := sendHeartbeat(ts.URL, "tcp@one"); err == nil {
		t.Fatal("failed registration was treated as success")
	}
	if err := sendHeartbeat("://invalid", "tcp@one"); err == nil {
		t.Fatal("invalid registry URL was accepted")
	}
}
