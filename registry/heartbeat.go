package registry

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"
)

var heartbeatHTTP = &http.Client{}

// Heartbeat registers immediately, then renews at the requested interval.
// As before, a failed renewal ends this heartbeat loop.
func Heartbeat(address, endpoint string, interval time.Duration) {
	HeartbeatWithWeight(address, endpoint, 1, interval)
}

// HeartbeatWithWeight registers an endpoint with a relative request weight.
// Weight must be between 1 and 10000; the original Heartbeat uses weight 1.
func HeartbeatWithWeight(address, endpoint string, weight int, interval time.Duration) {
	if weight < 1 || weight > 10000 {
		log.Printf("rpc server: invalid heartbeat weight %d", weight)
		return
	}
	if interval == 0 {
		interval = defaultTimeout - time.Minute
	}
	if err := sendHeartbeatWithWeight(address, endpoint, weight); err != nil {
		log.Printf("rpc server: heart beat err: %v", err)
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			if err := sendHeartbeatWithWeight(address, endpoint, weight); err != nil {
				log.Printf("rpc server: heart beat err: %v", err)
				return
			}
		}
	}()
}

func sendHeartbeat(address, endpoint string) error {
	return sendHeartbeatWithWeight(address, endpoint, 1)
}

func sendHeartbeatWithWeight(address, endpoint string, weight int) error {
	request, err := http.NewRequest(http.MethodPost, address, nil)
	if err != nil {
		return err
	}
	request.Header.Set("X-Joeyrpc-Server", endpoint)
	request.Header.Set("X-Joeyrpc-Weight", strconv.Itoa(weight))
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
