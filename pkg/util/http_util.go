package util

import (
	"crypto/tls"
	"net/http"
	"sync"
	"time"
)

var (
	client *http.Client
	once   sync.Once
)

func GetHttpClient() *http.Client {
	once.Do(func() {
		client = buildHttpClient()
	})
	return client
}

func buildHttpClient() *http.Client {

	transport := &http.Transport{
		// Connection pooling optimizations for high concurrency
		MaxIdleConns:        200, // Increased from default 100 for better concurrency
		MaxIdleConnsPerHost: 50,  // Increased from default 2 for better per-host performance

		// TLS configuration for Doris HTTP endpoints
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // Allow insecure connections for Doris HTTP endpoints
		},
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   120 * time.Second, // Total request timeout
	}

	return client
}
