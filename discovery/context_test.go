package discovery

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestContextCancelsWaitingForAnotherRefresh(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.Header().Set("X-Joeyrpc-Servers", "tcp@localhost:8001")
	}))
	defer server.Close()
	defer close(release)
	d := NewRegistryDiscovery(server.URL, 0)
	first := make(chan error, 1)
	go func() { first <- d.Refresh() }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("refresh did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	if _, err := d.GetContext(ctx, RandomSelect); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting refresh did not honor context: %v", err)
	}
}
