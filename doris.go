// Package doris_stream_load_client is the main package for the Doris Stream Load client
package doris_stream_load_client

import (
	"bytes"
	"io"
	"os"
	"strings"

	"github.com/bingquanzhao/doris-stream-load-client/pkg/client"
	"github.com/bingquanzhao/doris-stream-load-client/pkg/config"
	"github.com/bingquanzhao/doris-stream-load-client/pkg/load"
	"github.com/bingquanzhao/doris-stream-load-client/pkg/log"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// Client aliases
type DorisLoadClient = client.DorisLoadClient

// NewLoadSetting creates a new load setting configuration
func NewLoadSetting() *LoadSetting {
	return config.NewLoadSetting()
}

// NewLoadClient creates a new client with automatic validation
// Returns an error if the configuration is invalid
func NewLoadClient(setting *LoadSetting) (*DorisLoadClient, error) {
	return client.NewDorisClient(setting)
}

// Config aliases
type LoadSetting = config.LoadSetting
type Format = config.Format
type BatchMode = config.BatchMode
type Retry = config.Retry

// Log aliases
type LogLevel = log.Level
type LogFunc = log.LogFunc
type ContextLogger = log.ContextLogger

// Load aliases
type LoadResponse = load.LoadResponse
type LoadStatus = load.LoadStatus
type RespContent = load.RespContent

// Constants
const (
	// JSON format constants
	JsonObjectLine = config.JsonObjectLine
	JsonArray      = config.JsonArray

	// Batch mode constants
	SYNC  = config.SYNC
	ASYNC = config.ASYNC
	OFF   = config.OFF
	
	// Load status constants
	SUCCESS = load.SUCCESS
	FAILURE = load.FAILURE
	
	// Log level constants
	LogLevelDebug = log.LevelDebug
	LogLevelInfo  = log.LevelInfo
	LogLevelWarn  = log.LevelWarn
	LogLevelError = log.LevelError
)

// NewRetry creates a new retry configuration
func NewRetry(maxRetryTimes int, retryIntervalMs int64) *Retry {
	return config.NewRetry(maxRetryTimes, retryIntervalMs)
}

// NewDefaultRetry creates a new retry configuration with default values (5 retries, 1 second base interval)
// Uses exponential backoff: 1s, 2s, 4s, 8s, 16s = ~31 seconds total retry time
func NewDefaultRetry() *Retry {
	return config.NewDefaultRetry()
}

// Data conversion helpers for common use cases
// These functions convert various data types to io.Reader for use with Load()

// StringReader converts string data to io.Reader
func StringReader(data string) io.Reader {
	return strings.NewReader(data)
}

// BytesReader converts byte data to io.Reader
func BytesReader(data []byte) io.Reader {
	return bytes.NewReader(data)
}

// JSONReader converts any JSON-serializable object to io.Reader
func JSONReader(data interface{}) (io.Reader, error) {
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return strings.NewReader(string(jsonBytes)), nil
}

// Usage Examples:
//
// The client uses io.Reader interface for maximum flexibility:
//
// 1. String data:
//    client.Load(doris.StringReader("1,Alice,25\n2,Bob,30"))
//
// 2. Byte data:
//    client.Load(doris.BytesReader([]byte{...}))
//
// 3. JSON data:
//    reader, err := doris.JSONReader(myStruct)
//    if err == nil {
//        client.Load(reader)
//    }
//
// 4. File data:
//    file, _ := os.Open("data.csv")
//    defer file.Close()
//    client.Load(file)
//
// 5. Any io.Reader:
//    client.Load(strings.NewReader("your data"))
//
// This design follows Go's philosophy of simple, composable interfaces. 

// ================================
// Log Control Functions
// ================================
// These functions provide unified access to logging configuration
// without requiring users to import the internal log package

// SetLogLevel sets the minimum log level for the SDK
// Available levels: LogLevelDebug, LogLevelInfo, LogLevelWarn, LogLevelError
//
// Example:
//   doris.SetLogLevel(doris.LogLevelError) // Only show errors
//   doris.SetLogLevel(doris.LogLevelInfo)  // Show info, warn, and error (recommended for production)
func SetLogLevel(level LogLevel) {
	log.SetLevel(level)
}

// SetLogOutput sets the output destination for SDK logs
//
// Example:
//   file, _ := os.OpenFile("doris-sdk.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
//   doris.SetLogOutput(file)
func SetLogOutput(output *os.File) {
	log.SetOutput(output)
}

// DisableLogging completely disables all SDK logging
// This is equivalent to setting a very high log level
func DisableLogging() {
	log.SetLevel(log.Level(999))
}

// SetCustomLogFunc allows users to integrate their own logging systems
// 
// Example with logrus:
//   logger := logrus.New()
//   doris.SetCustomLogFunc(doris.LogLevelError, logger.Errorf)
//   doris.SetCustomLogFunc(doris.LogLevelInfo, logger.Infof)
func SetCustomLogFunc(level LogLevel, fn LogFunc) {
	switch level {
	case log.LevelDebug:
		log.SetDebugFunc(fn)
	case log.LevelInfo:
		log.SetInfoFunc(fn)
	case log.LevelWarn:
		log.SetWarnFunc(fn)
	case log.LevelError:
		log.SetErrorFunc(fn)
	}
}

// SetCustomLogFuncs allows setting all log functions at once
//
// Example:
//   logger := logrus.New()
//   doris.SetCustomLogFuncs(logger.Debugf, logger.Infof, logger.Warnf, logger.Errorf)
func SetCustomLogFuncs(debugFn, infoFn, warnFn, errorFn LogFunc) {
	if debugFn != nil {
		log.SetDebugFunc(debugFn)
	}
	if infoFn != nil {
		log.SetInfoFunc(infoFn)
	}
	if warnFn != nil {
		log.SetWarnFunc(warnFn)
	}
	if errorFn != nil {
		log.SetErrorFunc(errorFn)
	}
}

// NewContextLogger creates a context logger with the given context string
// This is useful for adding context information to logs in concurrent scenarios
//
// Example:
//   workerLogger := doris.NewContextLogger("Worker-1")
//   workerLogger.Infof("Processing batch %d", batchID)
func NewContextLogger(context string) *ContextLogger {
	return log.NewContextLogger(context)
} 