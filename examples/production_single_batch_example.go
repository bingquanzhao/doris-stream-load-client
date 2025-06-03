// Package examples demonstrates production-level single-threaded large batch loading
// This example shows how to efficiently load large amounts of data (100,000 records)
// Best practices: batch size optimization, memory efficiency, proper error handling
// Uses unified orders schema for consistency across all examples
package examples

import (
	"fmt"
	"time"

	doris "github.com/bingquanzhao/doris-stream-load-client"
	"github.com/bingquanzhao/doris-stream-load-client/pkg/log"
)

const (
	// Production-level batch size - recommended for optimal performance
	BATCH_SIZE = 100000 // 10万条记录
)

// RunSingleBatchExample demonstrates production-level single-threaded large batch loading
func RunSingleBatchExample() {
	fmt.Println("=== Production-Level Large Batch Loading Demo ===")
	
	// Production logging level
	log.SetLevel(log.LevelInfo)
	
	log.Infof("Starting large batch loading demo with %d order records", BATCH_SIZE)
	
	// Production-level configuration
	setting := doris.NewLoadSetting().
		AddFeNodes("http://10.16.10.6:8630").
		SetUser("root").
		SetPassword("123456").
		Database("test").
		Table("orders"). // Unified orders table
		SetLabelPrefix("prod_batch").
		CsvFormat(",", "\\n").
		// Production retry configuration: more aggressive for large batches
		Retry(doris.NewRetry(3, 2000)). // 3 retries with 2s base interval
		// Batch mode for better performance with large datasets
		BatchMode(doris.ASYNC)
	
	// Create client with automatic validation
	client, err := doris.NewLoadClient(setting)
	if err != nil {
		log.Errorf("Failed to create load client: %v", err)
		return
	}
	
	log.Infof("✅ Load client created successfully")
	
	// Generate large batch of realistic order data using unified data generator
	config := DataGeneratorConfig{
		BatchSize:   BATCH_SIZE,
		ContextName: "SingleBatch-DataGen",
	}
	data := GenerateOrderCSV(config)
	
	// Perform the load operation
	log.Infof("Starting load operation for %d order records...", BATCH_SIZE)
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