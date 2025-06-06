package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bingquanzhao/go-doris-sdk"
)

func main() {
	fmt.Printf("🎯 ==================== SDK性能极限测试 (固定数据量) ====================\n")
	fmt.Printf("📊 测试目标: 固定1亿条数据，测试不同并发级别的完成时间和吞吐量\n")
	fmt.Printf("🔬 重点指标: 总完成时间、每秒写入条数、每秒写入 MB\n\n")

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
			"timeout": "60",
		},
	}

	client, err := doris.NewLoadClient(config)
	if err != nil {
		fmt.Printf("❌ 创建客户端失败: %v\n", err)
		return
	}

	// 测试参数
	totalRecords := int64(100_000_000) // 1亿条数据
	batchSize := 50000                 // 每批5万条，需要2000批次
	concurrencyLevels := []int{1, 4, 8, 12}

	fmt.Printf("📋 测试配置:\n")
	fmt.Printf("   总数据量: %s 条记录\n", formatNumber(totalRecords))
	fmt.Printf("   批次大小: %s 条/批\n", formatNumber(int64(batchSize)))
	fmt.Printf("   预估总批次: %s 批\n", formatNumber(totalRecords/int64(batchSize)))
	fmt.Printf("   并发级别: %v\n", concurrencyLevels)
	fmt.Printf("   预估数据大小: %.2f GB\n", float64(totalRecords*120)/1024/1024/1024)

	// 预生成测试数据（所有批次使用相同数据，确保测试一致性）
	fmt.Printf("🔧 预生成测试数据 (%s 条)...\n", formatNumber(int64(batchSize)))
	testData := generateTestData(0, 0, batchSize) // 使用固定参数生成标准数据
	dataSize := int64(len(testData))
	fmt.Printf("✅ 数据生成完成，单批数据大小: %.2f MB\n\n", float64(dataSize)/1024/1024)

	// 存储结果
	results := make([]TestResult, 0, len(concurrencyLevels))

	// 执行测试
	for _, concurrency := range concurrencyLevels {
		fmt.Printf("🚀 ==================== 并发级别: %d ====================\n", concurrency)
		fmt.Printf("⏰ 开始时间: %s\n", time.Now().Format("15:04:05"))

		result := runFixedVolumeTest(client, concurrency, batchSize, totalRecords, testData, dataSize)
		results = append(results, result)

		printResult(result)
		fmt.Printf("⏰ 完成时间: %s\n\n", time.Now().Format("15:04:05"))

		// 休息10秒再进行下一轮测试
		if concurrency < concurrencyLevels[len(concurrencyLevels)-1] {
			fmt.Printf("😴 休息10秒后进行下一轮测试...\n\n")
			time.Sleep(10 * time.Second)
		}
	}

	// 分析结果
	analyzeResults(results)
}

type TestResult struct {
	Concurrency      int
	BatchSize        int
	TotalRecords     int64
	TotalBytes       int64
	TotalBatches     int64
	SuccessBatches   int64
	FailedBatches    int64
	TotalDuration    time.Duration
	RecordsPerSecond float64
	MBPerSecond      float64
	BatchesPerSecond float64
	AvgBatchDuration time.Duration
	SuccessRate      float64
}

func runFixedVolumeTest(client *doris.DorisLoadClient, concurrency, batchSize int, totalRecords int64, testData string, dataSize int64) TestResult {
	var processedRecords, totalBytes, completedBatches, failedBatches int64
	var totalDuration int64

	startTime := time.Now()

	// 计算需要的批次数
	totalBatches := totalRecords / int64(batchSize)
	if totalRecords%int64(batchSize) != 0 {
		totalBatches++
	}

	fmt.Printf("📦 需要处理 %d 个批次，每批 %d 条记录\n", totalBatches, batchSize)

	// 使用channel分发任务
	batchChan := make(chan int64, totalBatches)
	for i := int64(0); i < totalBatches; i++ {
		batchChan <- i
	}
	close(batchChan)

	var wg sync.WaitGroup

	// 启动worker
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for batchID := range batchChan {
				// 计算这个批次的实际记录数
				currentBatchSize := batchSize
				var currentData string
				var currentDataSize int64

				if batchID == totalBatches-1 && totalRecords%int64(batchSize) != 0 {
					// 最后一个批次如果不足5万条，需要生成对应数量的数据
					currentBatchSize = int(totalRecords % int64(batchSize))
					currentData = generateTestData(0, 0, currentBatchSize)
					currentDataSize = int64(len(currentData))
				} else {
					// 使用预生成的标准数据
					currentData = testData
					currentDataSize = dataSize
				}

				// 执行加载
				batchStart := time.Now()
				response, err := client.Load(doris.StringReader(currentData))
				batchDuration := time.Since(batchStart)

				// 更新统计
				atomic.AddInt64(&totalBytes, currentDataSize)
				atomic.AddInt64(&totalDuration, int64(batchDuration))

				if err != nil || response == nil || response.Status != doris.SUCCESS {
					atomic.AddInt64(&failedBatches, 1)
					fmt.Printf("   ❌ Worker %d 批次 %d 失败: %v\n", workerID, batchID, err)
				} else {
					atomic.AddInt64(&completedBatches, 1)
					atomic.AddInt64(&processedRecords, int64(currentBatchSize))
				}

				// 进度显示（每100批次显示一次）
				if atomic.LoadInt64(&completedBatches)%100 == 0 {
					progress := float64(atomic.LoadInt64(&completedBatches)) / float64(totalBatches) * 100
					fmt.Printf("   📈 进度: %.1f%% (%d/%d 批次完成)\n",
						progress, atomic.LoadInt64(&completedBatches), totalBatches)
				}
			}
		}(i)
	}

	wg.Wait()
	actualDuration := time.Since(startTime)

	// 构建结果
	result := TestResult{
		Concurrency:    concurrency,
		BatchSize:      batchSize,
		TotalRecords:   processedRecords,
		TotalBytes:     totalBytes,
		TotalBatches:   completedBatches + failedBatches,
		SuccessBatches: completedBatches,
		FailedBatches:  failedBatches,
		TotalDuration:  actualDuration,
	}

	// 计算性能指标
	seconds := actualDuration.Seconds()
	if seconds > 0 {
		result.RecordsPerSecond = float64(processedRecords) / seconds
		result.MBPerSecond = float64(totalBytes) / 1024 / 1024 / seconds
		result.BatchesPerSecond = float64(completedBatches) / seconds
	}

	if completedBatches > 0 {
		result.AvgBatchDuration = time.Duration(totalDuration / completedBatches)
	}

	if result.TotalBatches > 0 {
		result.SuccessRate = float64(completedBatches) / float64(result.TotalBatches) * 100
	}

	return result
}

func printResult(result TestResult) {
	fmt.Printf("📊 ==================== 测试结果 ====================\n")
	fmt.Printf("🎯 并发级别: %d\n", result.Concurrency)
	fmt.Printf("⏱️  总耗时: %v\n", result.TotalDuration)
	fmt.Printf("📈 处理记录: %s 条 (成功率: %.2f%%)\n",
		formatNumber(result.TotalRecords), result.SuccessRate)
	fmt.Printf("📦 处理批次: %d 批 (平均耗时: %v/批)\n",
		result.SuccessBatches, result.AvgBatchDuration)
	fmt.Printf("💾 数据量: %.2f GB\n", float64(result.TotalBytes)/1024/1024/1024)
	fmt.Printf("🚀 吞吐量:\n")
	fmt.Printf("   📊 %s 条/秒\n", formatNumber(int64(result.RecordsPerSecond)))
	fmt.Printf("   💿 %.2f MB/秒\n", result.MBPerSecond)
	fmt.Printf("   📦 %.1f 批次/秒\n", result.BatchesPerSecond)

	if result.FailedBatches > 0 {
		fmt.Printf("⚠️  失败批次: %d\n", result.FailedBatches)
	}
}

func analyzeResults(results []TestResult) {
	fmt.Printf("🎯 ==================== 性能分析报告 ====================\n")
	fmt.Printf("并发数 | 总耗时     | 记录数/秒    | MB/秒   | 成功率   | 扩展效率\n")
	fmt.Printf("-------|------------|-------------|---------|----------|----------\n")

	var baselinePerformance float64

	for i, result := range results {
		// 计算扩展效率
		var efficiency float64
		if i == 0 {
			baselinePerformance = result.RecordsPerSecond
			efficiency = 100.0 // 基准为100%
		} else {
			theoreticalPerformance := baselinePerformance * float64(result.Concurrency)
			efficiency = (result.RecordsPerSecond / theoreticalPerformance) * 100
		}

		fmt.Printf("%-6d | %-10v | %-11s | %-7.2f | %-8.1f%% | %.1f%%\n",
			result.Concurrency,
			result.TotalDuration.Round(time.Second),
			formatNumber(int64(result.RecordsPerSecond)),
			result.MBPerSecond,
			result.SuccessRate,
			efficiency)
	}

	// 找出最佳性能
	var bestResult TestResult
	var bestThroughput float64

	for _, result := range results {
		if result.RecordsPerSecond > bestThroughput {
			bestThroughput = result.RecordsPerSecond
			bestResult = result
		}
	}

	fmt.Printf("\n🏆 ==================== 性能总结 ====================\n")
	fmt.Printf("📊 单并发基准: %s 条/秒 | %.2f MB/秒\n",
		formatNumber(int64(results[0].RecordsPerSecond)), results[0].MBPerSecond)
	fmt.Printf("🚀 最佳性能: %s 条/秒 (%d 并发) | %.2f MB/秒\n",
		formatNumber(int64(bestThroughput)), bestResult.Concurrency, bestResult.MBPerSecond)
	fmt.Printf("📈 性能提升: %.1fx\n", bestThroughput/results[0].RecordsPerSecond)

	fmt.Printf("\n💡 关键发现:\n")
	if bestThroughput > 100000 {
		fmt.Printf("   🎉 性能优秀: 超过10万条/秒\n")
	} else if bestThroughput > 50000 {
		fmt.Printf("   👍 性能良好: 超过5万条/秒\n")
	} else {
		fmt.Printf("   ⚠️  性能一般: 需要进一步优化\n")
	}

	// 推荐配置
	fmt.Printf("\n🛡️  生产环境建议:\n")
	for _, result := range results {
		if result.SuccessRate >= 99.0 {
			fmt.Printf("   推荐并发: %d (成功率: %.1f%%, 吞吐量: %s 条/秒)\n",
				result.Concurrency, result.SuccessRate, formatNumber(int64(result.RecordsPerSecond)))
		}
	}

	fmt.Printf("========================================================\n")
}

// generateTestData 生成测试数据
func generateTestData(workerID, batchID, batchSize int) string {
	estimatedSize := batchSize * 120
	data := make([]byte, 0, estimatedSize)

	for i := 0; i < batchSize; i++ {
		orderID := fmt.Sprintf("PERF_W%d_B%d_R%d_%d", workerID, batchID, i, time.Now().UnixNano()%1000000)
		record := fmt.Sprintf("%s,Customer_%d,Product_%d,Electronics,Brand_%d,1,99.99,99.99,Shipped,2024-01-01,Region_%d\n",
			orderID, i%1000, i%100, i%50, i%10)
		data = append(data, record...)
	}
	return string(data)
}

// formatNumber 格式化数字显示
func formatNumber(n int64) string {
	if n >= 1000000 {
		return fmt.Sprintf("%.1fM", float64(n)/1000000)
	} else if n >= 1000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	return fmt.Sprintf("%d", n)
}
