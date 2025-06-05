// Package examples demonstrates production-level single-threaded large batch loading
// This example shows how to efficiently load large amounts of data (100,000 records)
// Best practices: batch size optimization, memory efficiency, proper error handling
// Uses unified orders schema for consistency across all examples
package examples

import (
	"fmt"
	"time"

	doris "github.com/bingquanzhao/go-doris-sdk"
)

const (
	// Production-level batch size - recommended for optimal performance
	BATCH_SIZE = 100000 // 10万条记录
)

// RunSingleBatchExample demonstrates production-level single-threaded large batch loading
func RunSingleBatchExample() {
	fmt.Println("=== Production-Level Large Batch Loading Demo ===")

	// Production logging level
	doris.SetLogLevel(doris.LogLevelInfo)

	logger := doris.NewContextLogger("SingleBatch")
	logger.Infof("Starting large batch loading demo with %d order records", BATCH_SIZE)

	// Production-level configuration using direct struct construction
	config := &doris.Config{
		Endpoints:   []string{"http://10.16.10.6:8630"},
		User:        "root",
		Password:    "123456",
		Database:    "test",
		Table:       "orders", // Unified orders table
		LabelPrefix: "prod_batch",
		Format:      doris.DefaultCSVFormat(), // Default CSV format
		Retry:       doris.NewRetry(3, 2000),  // 3 retries with 2s base interval
		GroupCommit: doris.ASYNC,              // ASYNC mode for better performance
	}

	// Create client with automatic validation
	client, err := doris.NewLoadClient(config)
	if err != nil {
		logger.Errorf("Failed to create load client: %v", err)
		return
	}

	logger.Infof("✅ Load client created successfully")

	// Generate large batch of realistic order data using unified data generator
	genConfig := DataGeneratorConfig{
		BatchSize:   BATCH_SIZE,
		ContextName: "SingleBatch-DataGen",
	}
	data := GenerateOrderCSV(genConfig)

	// Perform the load operation
	logger.Infof("Starting load operation for %d order records...", BATCH_SIZE)
	loadStart := time.Now()

	response, err := client.Load(doris.StringReader(data))

	loadTime := time.Since(loadStart)

	// Simple response handling
	if err != nil {
		fmt.Printf("❌ Load failed: %v\n", err)
		return
	}

	if response != nil && response.Status == doris.SUCCESS {
		fmt.Printf("🎉 Load completed successfully!\n")
		fmt.Printf("📊 Records: %d, Size: %.1f MB, Time: %v\n", BATCH_SIZE, float64(len(data))/1024/1024, loadTime)
		fmt.Printf("📈 Rate: %.0f records/sec, %.1f MB/sec\n", float64(BATCH_SIZE)/loadTime.Seconds(), float64(len(data))/1024/1024/loadTime.Seconds())
		if response.Resp.Label != "" {
			fmt.Printf("📋 Label: %s, Loaded: %d rows\n", response.Resp.Label, response.Resp.NumberLoadedRows)
		}
	} else {
		fmt.Printf("❌ Load failed with status: %v\n", response.Status)
	}

	fmt.Println("=== Demo Complete ===")
}
