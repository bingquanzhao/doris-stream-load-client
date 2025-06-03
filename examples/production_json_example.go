// Package examples demonstrates production-level JSON data loading
// This example shows how to efficiently load large amounts of structured JSON data (50,000 records)
// Best practices: JSON optimization, structured data, memory efficiency
// Uses unified orders schema for consistency across all examples
package examples

import (
	"fmt"
	"time"

	doris "github.com/bingquanzhao/doris-stream-load-client"
	"github.com/bingquanzhao/doris-stream-load-client/pkg/log"
)

const (
	// Production-level JSON batch size
	JSON_BATCH_SIZE = 50000 // 5万条JSON记录
)

// RunJSONExample demonstrates production-level JSON data loading
func RunJSONExample() {
	fmt.Println("=== Production-Level JSON Data Loading Demo ===")
	
	log.SetLevel(log.LevelInfo)
	
	log.Infof("Starting JSON loading demo with %d order records", JSON_BATCH_SIZE)
	
	// Production-level JSON configuration
	setting := doris.NewLoadSetting().
		AddFeNodes("http://10.16.10.6:8630").
		SetUser("root").
		SetPassword("123456").
		Database("test").
		Table("orders"). // Unified orders table
		SetLabelPrefix("prod_json").
		// JSON Lines format configuration
		JsonFormat(doris.JsonObjectLine). // Each line is a JSON object
		// Production retry configuration
		Retry(doris.NewRetry(3, 2000)).
		// ASYNC batch mode for better JSON processing performance
		BatchMode(doris.ASYNC)
	
	// Create client with automatic validation
	client, err := doris.NewLoadClient(setting)
	if err != nil {
		log.Errorf("Failed to create load client: %v", err)
		return
	}
	
	log.Infof("✅ JSON load client created successfully")
	
	// Generate realistic JSON order data using unified data generator
	config := DataGeneratorConfig{
		BatchSize:   JSON_BATCH_SIZE,
		ContextName: "JSON-DataGen",
	}
	jsonData := GenerateOrderJSON(config)
	
	// Perform the JSON load operation
	log.Infof("Starting JSON load operation for %d order records...", JSON_BATCH_SIZE)
	loadStart := time.Now()
	
	response, err := client.Load(doris.StringReader(jsonData))
	
	loadTime := time.Since(loadStart)
	
	// Simple response handling
	if err != nil {
		fmt.Printf("❌ JSON load failed: %v\n", err)
		return
	}
	
	if response != nil && response.Status == doris.SUCCESS {
		fmt.Printf("🎉 JSON load completed successfully!\n")
		fmt.Printf("📊 JSON Records: %d, Size: %.1f MB, Time: %v\n", JSON_BATCH_SIZE, float64(len(jsonData))/1024/1024, loadTime)
		fmt.Printf("📈 JSON Rate: %.0f records/sec, %.1f MB/sec\n", float64(JSON_BATCH_SIZE)/loadTime.Seconds(), float64(len(jsonData))/1024/1024/loadTime.Seconds())
		if response.Resp.Label != "" {
			fmt.Printf("📋 Label: %s, Loaded: %d rows\n", response.Resp.Label, response.Resp.NumberLoadedRows)
			if response.Resp.LoadBytes > 0 {
				avgBytesPerRecord := float64(response.Resp.LoadBytes) / float64(response.Resp.NumberLoadedRows)
				fmt.Printf("📏 Average bytes per JSON record: %.1f\n", avgBytesPerRecord)
			}
		}
	} else {
		fmt.Printf("❌ JSON load failed with status: %v\n", response.Status)
	}
	
	fmt.Println("=== JSON Demo Complete ===")
} 