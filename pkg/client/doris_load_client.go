// Package client provides the main client interface for Doris Stream Load
package client

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/bingquanzhao/doris-stream-load-client/pkg/config"
	"github.com/bingquanzhao/doris-stream-load-client/pkg/load"
	"github.com/bingquanzhao/doris-stream-load-client/pkg/log"
)

// Pre-compiled error patterns for efficient matching
var (
	retryableErrorPatterns = []string{
		"connection refused",
		"connection reset",
		"connection timeout",
		"timeout",
		"network is unreachable",
		"no such host",
		"temporary failure",
		"dial tcp",
		"i/o timeout",
		"eof",
		"broken pipe",
		"connection aborted",
		"307 temporary redirect",
		"302 found",
		"301 moved permanently",
	}

	retryableResponsePatterns = []string{
		"connect",
		"unavailable",
		"timeout",
		"redirect",
	}

	// Pool for string builders to reduce allocations
	stringBuilderPool = sync.Pool{
		New: func() interface{} {
			return &strings.Builder{}
		},
	}
)

// DorisLoadClient is the main client interface for loading data into Doris
type DorisLoadClient struct {
	streamLoader *load.StreamLoader
	loadSettings *config.LoadSetting
}

// NewDorisClient creates a new DorisLoadClient with the specified settings
// The client is thread-safe and can be used concurrently from multiple goroutines
// Automatically validates the configuration and returns an error if invalid
func NewDorisClient(loadSettings *config.LoadSetting) (*DorisLoadClient, error) {
	// Automatically validate the configuration
	if err := loadSettings.ValidateInternal(); err != nil {
		return nil, fmt.Errorf("invalid load settings: %w", err)
	}

	return &DorisLoadClient{
		streamLoader: load.NewStreamLoader(loadSettings),
		loadSettings: loadSettings,
	}, nil
}

// isRetryableError determines if an error should trigger a retry
// Only network/connection issues should be retried
// Optimized to reduce memory allocations
func isRetryableError(err error, response *load.LoadResponse) bool {
	if err != nil {
		// Avoid ToLower allocation by checking original error first
		errStr := err.Error()

		// Check net.Error interface first (most efficient)
		if netErr, ok := err.(net.Error); ok {
			if netErr.Timeout() || netErr.Temporary() {
				return true
			}
		}

		// Only convert to lowercase if necessary
		errStrLower := strings.ToLower(errStr)
		for _, pattern := range retryableErrorPatterns {
			if strings.Contains(errStrLower, pattern) {
				return true
			}
		}

		return false
	}

	// If no error but response indicates failure, check if it's a retryable response error
	if response != nil && response.Status == load.FAILURE && response.ErrorMessage != "" {
		errMsgLower := strings.ToLower(response.ErrorMessage)
		for _, pattern := range retryableResponsePatterns {
			if strings.Contains(errMsgLower, pattern) {
				return true
			}
		}
	}

	return false
}

// calculateBackoffInterval calculates exponential backoff interval
// Target: ~30 seconds total retry time with exponential backoff
// Intervals: 1s, 2s, 4s, 8s, 16s (total: 31s for 5 retries)
func calculateBackoffInterval(attempt int, baseIntervalMs int64) time.Duration {
	if attempt <= 0 {
		return 0
	}

	// Exponential backoff: baseInterval * 2^(attempt-1)
	multiplier := int64(1 << (attempt - 1)) // 2^(attempt-1)
	intervalMs := baseIntervalMs * multiplier

	// Cap the maximum interval to prevent too long waits
	const maxIntervalMs = 16000 // 16 seconds max
	if intervalMs > maxIntervalMs {
		intervalMs = maxIntervalMs
	}

	return time.Duration(intervalMs) * time.Millisecond
}

// Load sends data to Doris via HTTP stream load with retry logic
func (c *DorisLoadClient) Load(reader io.Reader) (*load.LoadResponse, error) {
	// Get retry configuration from settings
	retry := c.loadSettings.GetRetry()
	maxRetries := retry.GetMaxRetryTimes()
	baseIntervalMs := retry.GetRetryIntervalMs()

	log.Infof("Starting stream load operation")
	log.Infof("Target: %s.%s (endpoint: %s)", c.loadSettings.GetDatabase(), c.loadSettings.GetTable(), c.loadSettings.GetEndpoint())
	log.Infof("Label: %s", c.loadSettings.GetLabel())
	
	// Show the actual retry strategy to avoid confusion
	if maxRetries > 0 {
		// Calculate and show the actual retry intervals
		var intervals []string
		totalTimeMs := int64(0)
		for i := 1; i <= maxRetries; i++ {
			intervalMs := baseIntervalMs * int64(1<<(i-1)) // 2^(i-1)
			if intervalMs > 16000 { // Cap at 16 seconds
				intervalMs = 16000
			}
			intervals = append(intervals, fmt.Sprintf("%dms", intervalMs))
			totalTimeMs += intervalMs
		}
		log.Debugf("Retry strategy: exponential backoff with %d attempts, intervals: [%s], total max time: %dms", 
			maxRetries, strings.Join(intervals, ", "), totalTimeMs)
	} else {
		log.Debugf("Retry disabled (maxRetries=0)")
	}

	var lastErr error
	var response *load.LoadResponse

	// Try the operation with retries
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			log.Infof("Retry attempt %d/%d", attempt, maxRetries)
		} else {
			log.Infof("Initial load attempt")
		}

		// Calculate and apply backoff delay for retries
		if attempt > 0 {
			backoffInterval := calculateBackoffInterval(attempt, baseIntervalMs)
			log.Infof("Waiting %v before retry attempt", backoffInterval)
			time.Sleep(backoffInterval)
		}

		response, lastErr = c.streamLoader.Load(reader)

		// If successful, return immediately
		if lastErr == nil && response != nil && response.Status == load.SUCCESS {
			log.Infof("Stream load operation completed successfully on attempt %d", attempt+1)
			return response, nil
		}

		// Check if this error/response should be retried
		shouldRetry := isRetryableError(lastErr, response)

		if lastErr != nil {
			log.Errorf("Attempt %d failed with error: %v (retryable: %t)", attempt+1, lastErr, shouldRetry)
		} else if response != nil && response.Status == load.FAILURE {
			log.Errorf("Attempt %d failed with status: %s (retryable: %t)", attempt+1, response.Resp.Status, shouldRetry)
			if response.ErrorMessage != "" {
				log.Errorf("Error details: %s", response.ErrorMessage)
			}
		}

		// Early exit for non-retryable errors
		if !shouldRetry {
			log.Warnf("Error is not retryable, stopping retry attempts")
			break
		}

		// If this is the last attempt, don't continue
		if attempt == maxRetries {
			log.Warnf("Reached maximum retry attempts (%d), stopping", maxRetries)
			break
		}
	}

	// Final result logging
	if lastErr != nil {
		log.Errorf("Stream load operation failed after %d attempts: %v", maxRetries+1, lastErr)
		return response, lastErr
	}

	if response != nil {
		log.Errorf("Stream load operation failed with final status: %v", response.Status)
		return response, fmt.Errorf("load failed with status: %v", response.Status)
	}

	log.Errorf("Stream load operation failed with unknown error after %d attempts", maxRetries+1)
	return nil, fmt.Errorf("load failed: unknown error")
}
