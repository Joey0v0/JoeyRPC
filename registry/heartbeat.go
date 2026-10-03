package registry

import (
	"fmt"
	"log"
	"net/http"
	"time"
)

var heartbeatHTTP = &http.Client{}

// Heartbeat registers immediately, then renews at the requested interval.
// As before, a failed renewal ends this heartbeat loop.
func Heartbeat(address, endpoint string, interval time.Duration) {
	if interval == 0 {
		interval = defaultTimeout - time.Minute
	}
	if err := sendHeartbeat(address, endpoint); err != nil {
		log.Printf("rpc server: heart beat err: %v", err)
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			if err := sendHeartbeat(address, endpoint); err != nil {
				log.Printf("rpc server: heart beat err: %v", err)
				return
			}
		}
	}()
}

func sendHeartbeat(address, endpoint string) error {
	request, err := http.NewRequest(http.MethodPost, address, nil)
	if err != nil {
		return err
	}
	request.Header.Set("X-Joeyrpc-Server", endpoint)
	response, err := heartbeatHTTP.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("rpc registry: unexpected HTTP status %s", response.Status)
	}
	return nil
}
