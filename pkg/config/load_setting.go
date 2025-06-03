// Package config provides configuration structures for the Doris Stream Load client
package config

import (
	"fmt"
	"github.com/google/uuid"
	"log"
	"math/rand"
	"net/url"
	"strings"
	"time"
)

// Format defines the JSON format type
type Format int

const (
	// JsonObjectLine represents JSON objects separated by newlines
	JsonObjectLine Format = iota
	// JsonArray represents a JSON array
	JsonArray
)

// BatchMode defines the group commit mode
type BatchMode int

const (
	// SYNC represents synchronous group commit mode
	SYNC BatchMode = iota
	// ASYNC represents asynchronous group commit mode
	ASYNC
	// OFF represents disabled group commit mode
	OFF
)

// Retry contains configuration for retry attempts when loading data
type Retry struct {
	maxRetryTimes   int
	retryIntervalMs int64
}

// NewRetry creates a new Retry instance with the given retry times and interval
func NewRetry(maxRetryTimes int, retryIntervalMs int64) *Retry {
	return &Retry{
		maxRetryTimes:   maxRetryTimes,
		retryIntervalMs: retryIntervalMs,
	}
}

// NewDefaultRetry creates a new Retry instance with default values (5 retries, 1 second base interval)
// With exponential backoff: 1s, 2s, 4s, 8s, 16s = ~31 seconds total
func NewDefaultRetry() *Retry {
	return NewRetry(5, 1000)
}

// GetMaxRetryTimes returns the maximum number of retry attempts
func (r *Retry) GetMaxRetryTimes() int {
	return r.maxRetryTimes
}

// GetRetryIntervalMs returns the interval between retry attempts in milliseconds
func (r *Retry) GetRetryIntervalMs() int64 {
	return r.retryIntervalMs
}

// SetMaxRetryTimes sets the maximum number of retry attempts
func (r *Retry) SetMaxRetryTimes(maxRetryTimes int) *Retry {
	r.maxRetryTimes = maxRetryTimes
	return r
}

// SetRetryIntervalMs sets the interval between retry attempts in milliseconds
func (r *Retry) SetRetryIntervalMs(retryIntervalMs int64) *Retry {
	r.retryIntervalMs = retryIntervalMs
	return r
}

// IsRetryEnabled returns true if retry is enabled (maxRetryTimes > 0)
func (r *Retry) IsRetryEnabled() bool {
	return r.maxRetryTimes > 0
}

// LoadSetting contains settings for stream load operations
type LoadSetting struct {
	settings map[string]string
	//feNodes     string
	user        string
	password    string
	database    string
	table       string
	labelPrefix string
	retry       *Retry
	batchMode   BatchMode

	// Performance optimization fields - parsed once when feNodes is set
	parsedNodes []string
}

// NewLoadSetting creates a new LoadSetting instance
func NewLoadSetting() *LoadSetting {
	ls := &LoadSetting{
		settings: make(map[string]string),
		retry:    NewDefaultRetry(), // 使用默认重试配置

		// Initialize performance optimization fields
		parsedNodes: nil, // Will be lazily initialized
	}
	
	// Set default batch mode and update settings accordingly
	ls.BatchMode(ASYNC) // 默认使用异步批量模式
	
	return ls
}

// GetEndpoint returns a randomly selected FE node from the feNodes list
func (ls *LoadSetting) GetEndpoint() string {
	if len(ls.parsedNodes) == 0 {
		log.Fatalf("LoadSetting endpoint required")
	}

	// Simple random selection - thread-safe
	randomIndex := rand.Intn(len(ls.parsedNodes))
	return ls.parsedNodes[randomIndex]
}

// AddFeNodes sets the feNodes and immediately parses them for performance
func (ls *LoadSetting) AddFeNodes(feNodes string) *LoadSetting {

	// Parse and cache nodes immediately since feNodes won't change
	if feNodes != "" {
		ls.parsedNodes = strings.Split(feNodes, ",")
		// Trim spaces during parsing
		for i, node := range ls.parsedNodes {
			parse, err := url.Parse(node)
			if err != nil {
				log.Fatalf("feNodes format error failed: %v", err)
			}
			ls.parsedNodes[i] = parse.Host
		}
	} else {
		ls.parsedNodes = nil
	}

	return ls
}

// Database sets the database and returns the LoadSetting for method chaining
func (ls *LoadSetting) Database(database string) *LoadSetting {
	ls.database = database
	return ls
}

// GetUser returns the user
func (ls *LoadSetting) GetUser() string {
	return ls.user
}

// SetUser sets the user and returns the LoadSetting for method chaining
func (ls *LoadSetting) SetUser(user string) *LoadSetting {
	ls.user = user
	return ls
}

// GetPassword returns the password
func (ls *LoadSetting) GetPassword() string {
	return ls.password
}

// SetPassword sets the password and returns the LoadSetting for method chaining
func (ls *LoadSetting) SetPassword(password string) *LoadSetting {
	ls.password = password
	return ls
}

// Table sets the table and returns the LoadSetting for method chaining
func (ls *LoadSetting) Table(table string) *LoadSetting {
	ls.table = table
	return ls
}

// Retry sets the retry configuration and returns the LoadSetting for method chaining
func (ls *LoadSetting) Retry(retry *Retry) *LoadSetting {
	ls.retry = retry
	return ls
}

// JsonFormat sets the format to JSON and configures JSON-specific options
func (ls *LoadSetting) JsonFormat(format Format) *LoadSetting {
	ls.settings["format"] = "json"
	if format == JsonObjectLine {
		ls.settings["strip_outer_array"] = "false"
		ls.settings["read_json_by_line"] = "true"
	}
	if format == JsonArray {
		ls.settings["strip_outer_array"] = "true"
	}
	return ls
}

// CsvFormat sets the format to CSV and configures CSV-specific options
func (ls *LoadSetting) CsvFormat(columnSeparator, lineDelimiter string) *LoadSetting {
	ls.settings["format"] = "csv"
	ls.settings["column_separator"] = columnSeparator
	ls.settings["line_delimiter"] = lineDelimiter
	return ls
}

// GetLabel generates a new unique label each time it's called (thread-safe)
func (ls *LoadSetting) GetLabel() string {
	return ls.generateLabel(ls.labelPrefix, ls.database, ls.table)
}

// GetLabelPrefix returns the label prefix
func (ls *LoadSetting) GetLabelPrefix() string {
	return ls.labelPrefix
}

// SetLabelPrefix sets the label prefix and returns the LoadSetting for method chaining
func (ls *LoadSetting) SetLabelPrefix(prefix string) *LoadSetting {
	ls.labelPrefix = prefix
	return ls
}

// AddOptions adds multiple options at once
func (ls *LoadSetting) AddOptions(options map[string]string) *LoadSetting {
	for k, v := range options {
		ls.settings[k] = v
	}
	return ls
}

// AddOption adds a single option
func (ls *LoadSetting) AddOption(key, value string) *LoadSetting {
	ls.settings[key] = value
	return ls
}

// BatchMode sets the batch mode (sync, async, or off) and immediately updates the settings
func (ls *LoadSetting) BatchMode(mode BatchMode) *LoadSetting {
	ls.batchMode = mode
	
	// Immediately update the settings map based on batch mode
	switch mode {
	case SYNC:
		ls.settings["group_commit"] = "sync_mode"
	case ASYNC:
		ls.settings["group_commit"] = "async_mode"
	case OFF:
		// Remove group_commit setting if it exists
		delete(ls.settings, "group_commit")
	}
	
	return ls
}

// GetBatchMode returns the current batch mode
func (ls *LoadSetting) GetBatchMode() BatchMode {
	return ls.batchMode
}

// GetOptions returns a copy of all configured options
// All settings including batch mode are pre-configured when set, so this method
// simply returns a thread-safe copy without any modifications
func (ls *LoadSetting) GetOptions() map[string]string {
	// Create a copy of the settings map - no dynamic modifications needed
	result := make(map[string]string, len(ls.settings))
	for k, v := range ls.settings {
		result[k] = v
	}
	
	return result
}

// generateLabel creates a unique label for the load job
func (ls *LoadSetting) generateLabel(labelPrefix, database, table string) string {
	// Get current time in milliseconds since Unix epoch
	currentTimeMillis := time.Now().UnixMilli()

	// Generate a random UUID
	id := uuid.New()

	// Format the label string
	prefix := ""
	if labelPrefix != "" {
		prefix = labelPrefix
	}

	return fmt.Sprintf("%s_%s_%s_%d_%s", prefix, database, table, currentTimeMillis, id.String())
}

// GetDatabase returns the database name
func (ls *LoadSetting) GetDatabase() string {
	return ls.database
}

// GetTable returns the table name
func (ls *LoadSetting) GetTable() string {
	return ls.table
}

// GetRetry returns the retry configuration
func (ls *LoadSetting) GetRetry() *Retry {
	return ls.retry
}

// ValidateInternal validates the load setting configuration internally
// This is only used by internal packages and is not part of the public API
func (ls *LoadSetting) ValidateInternal() error {
	return ls.validate()
}

// validate validates the load setting configuration internally
// This is called automatically when creating clients, users don't need to call it manually
func (ls *LoadSetting) validate() error {
	if ls.user == "" {
		return fmt.Errorf("user cannot be empty")
	}

	if ls.password == "" {
		return fmt.Errorf("password cannot be empty")
	}

	if ls.database == "" {
		return fmt.Errorf("database cannot be empty")
	}

	if ls.table == "" {
		return fmt.Errorf("table cannot be empty")
	}

	// Validate that feNodes is set
	if len(ls.parsedNodes) == 0 {
		return fmt.Errorf("feNodes cannot be empty, please call AddFeNodes() first")
	}

	// Validate retry configuration
	if ls.retry != nil {
		if ls.retry.maxRetryTimes < 0 {
			return fmt.Errorf("maxRetryTimes cannot be negative")
		}
		if ls.retry.retryIntervalMs < 0 {
			return fmt.Errorf("retryIntervalMs cannot be negative")
		}
	}

	return nil
}

// Clone creates a deep copy of the LoadSetting for concurrent use
// This is the recommended approach for high-performance concurrent scenarios
// where each goroutine should have its own LoadSetting instance
func (ls *LoadSetting) Clone() *LoadSetting {
	
	// Create a new LoadSetting instance
	cloned := &LoadSetting{
		settings:    make(map[string]string),
		user:        ls.user,
		password:    ls.password,
		database:    ls.database,
		table:       ls.table,
		labelPrefix: ls.labelPrefix,
		retry:       ls.retry, // Retry is immutable after creation, safe to share
		batchMode:   ls.batchMode,
		parsedNodes: make([]string, len(ls.parsedNodes)), // Deep copy the slice
	}
	
	// Deep copy the settings map
	for k, v := range ls.settings {
		cloned.settings[k] = v
	}
	
	// Deep copy the parsedNodes slice
	copy(cloned.parsedNodes, ls.parsedNodes)
	
	return cloned
}

// CloneForConcurrency is an alias for Clone() with a more descriptive name
func (ls *LoadSetting) CloneForConcurrency() *LoadSetting {
	return ls.Clone()
}
