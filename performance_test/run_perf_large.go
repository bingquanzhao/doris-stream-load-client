package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bingquanzhao/go-doris-sdk"
)

func main() {
	fmt.Printf("🔥 ==================== SDK性能极限测试 (大批次) ====================\n")
	fmt.Printf("🎯 目标: 使用更大批次测量真实的数据写入极限\n\n")

	// Doris配置
	config := &doris.Config{
		Endpoints:   []string{"http://10.16.10.6:8630"},
		User:        "root",
		Password:    "123456",
		Database:    "test",
		Table:       "orders",
		Format:      doris.DefaultCSVFormat(),
		Retry:       &doris.Retry{MaxRetryTimes: 2, BaseIntervalMs: 200, MaxTotalTimeMs: 10000},
		GroupCommit: doris.ASYNC,
		Options: map[string]string{
			"timeout": "60", // 增加超时时间
		},
	}

	client, err := doris.NewLoadClient(config)
	if err != nil {
		fmt.Printf("❌ 创建客户端失败: %v\n", err)
		return
	}

	// 测试不同批次大小的影响
	fmt.Printf("🔬 ==================== 批次大小影响测试 ====================\n")
	batchSizes := []int{10000, 20000, 50000, 100000}
	concurrency := 4                // 固定4并发测试批次大小影响
	testDuration := 3 * time.Minute // 增加到3分钟，产生更多数据

	for _, batchSize := range batchSizes {
		fmt.Printf("🧪 测试批次大小: %d 条记录 (4并发, 3分钟)\n", batchSize)
		result := runBatchSizeTest(client, concurrency, batchSize, testDuration)
		printBatchResult(result, batchSize)
		fmt.Printf("   📊 预计总数据: %.1f GB (本轮)\n", float64(result.TotalBytes)/1024/1024/1024)
		time.Sleep(5 * time.Second)
	}

	// 使用最优批次大小进行并发测试
	optimalBatchSize := 50000 // 使用5万条，产生更大数据量
	fmt.Printf("\n🚀 ==================== 并发线性扩展性测试 ====================\n")
	fmt.Printf("📦 使用批次大小: %d 条记录\n", optimalBatchSize)
	fmt.Printf("🎯 重点指标: MB/s 吞吐量和并发线性扩展性\n")

	concurrencyLevels := []int{1, 2, 4, 8, 16, 32}
	testDuration = 5 * time.Minute // 每个并发级别测试5分钟，产生大量数据

	results := make(map[int]map[string]interface{})

	for _, concurrency := range concurrencyLevels {
		fmt.Printf("🚀 测试并发度: %d (5分钟测试)\n", concurrency)
		result := runConcurrencyTest(client, concurrency, optimalBatchSize, testDuration)
		printConcurrencyResult(result, concurrency)

		// 显示累计数据量
		totalGB := float64(result.TotalBytes) / 1024 / 1024 / 1024
		fmt.Printf("   📊 本轮总数据: %.2f GB | 累计运行: %.1f 分钟\n", totalGB, testDuration.Minutes())

		results[concurrency] = map[string]interface{}{
			"records_per_sec": result.RecordsPerSec,
			"mb_per_sec":      result.MBPerSec,
			"total_records":   result.TotalRecords,
			"total_bytes":     result.TotalBytes,
			"error_rate":      result.ErrorRate,
			"avg_latency":     result.AvgLatency,
		}

		// 如果错误率过高，停止测试
		if result.ErrorRate > 20 {
			fmt.Printf("⚠️  错误率过高 (%.1f%%)，停止后续测试\n", result.ErrorRate)
			break
		}

		time.Sleep(5 * time.Second)
	}

	// 最终分析
	analyzeResults(results, concurrencyLevels)
}

type TestResult struct {
	Concurrency     int
	BatchSize       int
	Duration        time.Duration
	TotalRecords    int64
	TotalBytes      int64
	TotalRequests   int64
	SuccessRequests int64
	FailedRequests  int64
	RecordsPerSec   float64
	MBPerSec        float64
	RequestsPerSec  float64
	AvgLatency      time.Duration
	ErrorRate       float64
}

func runBatchSizeTest(client *doris.DorisLoadClient, concurrency, batchSize int, duration time.Duration) TestResult {
	return runConcurrencyTest(client, concurrency, batchSize, duration)
}

func runConcurrencyTest(client *doris.DorisLoadClient, concurrency, batchSize int, duration time.Duration) TestResult {
	var totalRecords, totalBytes, totalRequests, successCount, failureCount int64
	var totalLatency int64

	startTime := time.Now()
	var wg sync.WaitGroup

	// 启动所有worker
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			endTime := time.Now().Add(duration)
			for time.Now().Before(endTime) {
				// 生成测试数据
				data := generateTestData(workerID, batchSize)
				dataSize := int64(len(data))

				// 执行加载
				reqStart := time.Now()
				response, err := client.Load(doris.StringReader(data))
				latency := time.Since(reqStart)

				// 更新统计
				atomic.AddInt64(&totalRequests, 1)
				atomic.AddInt64(&totalBytes, dataSize)
				atomic.AddInt64(&totalLatency, int64(latency))

				if err != nil || response == nil || response.Status != doris.SUCCESS {
					atomic.AddInt64(&failureCount, 1)
					if err != nil {
						fmt.Printf("   ❌ Worker %d 错误: %v\n", workerID, err)
					}
				} else {
					atomic.AddInt64(&successCount, 1)
					atomic.AddInt64(&totalRecords, int64(batchSize))
				}
			}
		}(i)
	}

	wg.Wait()
	actualDuration := time.Since(startTime)

	// 计算结果
	result := TestResult{
		Concurrency:     concurrency,
		BatchSize:       batchSize,
		Duration:        actualDuration,
		TotalRecords:    totalRecords,
		TotalBytes:      totalBytes,
		TotalRequests:   totalRequests,
		SuccessRequests: successCount,
		FailedRequests:  failureCount,
	}

	seconds := actualDuration.Seconds()
	if seconds > 0 {
		result.RecordsPerSec = float64(totalRecords) / seconds
		result.MBPerSec = float64(totalBytes) / 1024 / 1024 / seconds
		result.RequestsPerSec = float64(totalRequests) / seconds
	}

	if totalRequests > 0 {
		result.AvgLatency = time.Duration(totalLatency / totalRequests)
		result.ErrorRate = float64(failureCount) / float64(totalRequests) * 100
	}

	return result
}

func printBatchResult(result TestResult, batchSize int) {
	fmt.Printf("   📊 批次大小 %d 条:\n", batchSize)
	fmt.Printf("      📈 吞吐量: %.0f records/sec | %.2f MB/sec | %.1f requests/sec\n",
		result.RecordsPerSec, result.MBPerSec, result.RequestsPerSec)
	fmt.Printf("      ⏱️  平均延迟: %v | 请求效率: %.0f records/request\n",
		result.AvgLatency, float64(result.TotalRecords)/float64(result.TotalRequests))
	fmt.Printf("      ✅ 成功率: %.1f%% | 总数据: %d records\n",
		100-result.ErrorRate, result.TotalRecords)
	fmt.Printf("\n")
}

func printConcurrencyResult(result TestResult, concurrency int) {
	fmt.Printf("   📊 并发度 %d:\n", concurrency)
	fmt.Printf("      📈 吞吐量: %.0f records/sec | %.2f MB/sec\n",
		result.RecordsPerSec, result.MBPerSec)
	fmt.Printf("      ⏱️  延迟: %v | 成功率: %.1f%%\n",
		result.AvgLatency, 100-result.ErrorRate)
	fmt.Printf("      📦 总数据: %d records | %.1f MB\n",
		result.TotalRecords, float64(result.TotalBytes)/1024/1024)
	fmt.Printf("\n")
}

func analyzeResults(results map[int]map[string]interface{}, concurrencyLevels []int) {
	fmt.Printf("🎯 ==================== 性能分析报告 ====================\n")
	fmt.Printf("并发数 | Records/sec | MB/sec | 错误率(%%) | 平均延迟 | 总记录数\n")
	fmt.Printf("-------|-------------|--------|----------|----------|----------\n")

	var maxThroughput float64
	var optimalConcurrency int
	var singleConcThroughput float64

	for _, concurrency := range concurrencyLevels {
		result, exists := results[concurrency]
		if !exists {
			continue
		}

		recordsPerSec := result["records_per_sec"].(float64)
		mbPerSec := result["mb_per_sec"].(float64)
		errorRate := result["error_rate"].(float64)
		avgLatency := result["avg_latency"].(time.Duration)
		totalRecords := result["total_records"].(int64)

		fmt.Printf("%-6d | %-11.0f | %-6.2f | %-8.1f | %-8v | %d\n",
			concurrency, recordsPerSec, mbPerSec, errorRate, avgLatency, totalRecords)

		if concurrency == 1 {
			singleConcThroughput = recordsPerSec
		}

		if recordsPerSec > maxThroughput {
			maxThroughput = recordsPerSec
			optimalConcurrency = concurrency
		}
	}

	// 计算总数据量
	var totalDataGB float64
	for _, result := range results {
		if totalBytes, ok := result["total_bytes"].(int64); ok {
			totalDataGB += float64(totalBytes) / 1024 / 1024 / 1024
		}
	}

	// 性能总结
	fmt.Printf("\n🏆 ==================== 性能极限总结 ====================\n")
	fmt.Printf("🗄️ 测试总数据量: %.2f GB\n", totalDataGB)
	if singleConcThroughput > 0 {
		fmt.Printf("📊 单并发性能基准: %.0f records/sec | %.2f MB/sec\n",
			singleConcThroughput, results[1]["mb_per_sec"].(float64))
		fmt.Printf("🚀 最大吞吐量: %.0f records/sec (%d 并发)\n", maxThroughput, optimalConcurrency)
		scalingEfficiency := (maxThroughput / singleConcThroughput) / float64(optimalConcurrency) * 100
		fmt.Printf("📈 性能提升倍数: %.1fx | 扩展效率: %.1f%%\n", maxThroughput/singleConcThroughput, scalingEfficiency)
	}

	// 关键发现
	fmt.Printf("\n💡 关键发现:\n")
	if maxThroughput > 50000 {
		fmt.Printf("   🎉 性能优秀: 超过5万records/sec\n")
	} else if maxThroughput > 20000 {
		fmt.Printf("   👍 性能良好: 超过2万records/sec\n")
	} else {
		fmt.Printf("   ⚠️  性能一般: 需要进一步优化\n")
	}

	// 推荐配置
	var stableConcurrency int
	for _, concurrency := range concurrencyLevels {
		if result, exists := results[concurrency]; exists {
			if result["error_rate"].(float64) < 5 {
				stableConcurrency = concurrency
			}
		}
	}

	fmt.Printf("\n🛡️  生产环境建议:\n")
	if stableConcurrency > 0 {
		fmt.Printf("   稳定运行: %d 并发 (错误率 < 5%%)\n", stableConcurrency)
	}
	fmt.Printf("   峰值性能: %d 并发\n", optimalConcurrency)
	fmt.Printf("   建议批次: 10,000-50,000 records\n")
	fmt.Printf("   建议超时: 60-120秒\n")

	fmt.Printf("========================================================\n")
}

// generateTestData 生成测试数据 - 优化版本
func generateTestData(workerID, batchSize int) string {
	// 预分配字符串空间，提高性能
	estimatedSize := batchSize * 120 // 每条记录约120字节
	data := make([]byte, 0, estimatedSize)

	for i := 0; i < batchSize; i++ {
		orderID := fmt.Sprintf("PERF_W%d_R%d_%d", workerID, i, time.Now().UnixNano())
		record := fmt.Sprintf("%s,Customer_%d,Product_%d,Electronics,Brand_%d,1,99.99,99.99,Shipped,2024-01-01,Region_%d\n",
			orderID, i%1000, i%100, i%50, i%10)
		data = append(data, record...)
	}
	return string(data)
}
