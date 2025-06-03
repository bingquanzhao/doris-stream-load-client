# Doris Stream Load Client for Go

功能完整、生产就绪的 Apache Doris Stream Load Go 客户端。

## 🚀 特性

- **🔄 自动重试** - 内置指数退避重试机制
- **📊 多格式支持** - 支持 JSON 和 CSV 格式
- **⚡ 高性能** - 优化的连接池和并发处理
- **🛠️ 简单易用** - 链式 API 配置
- **🔒 线程安全** - 支持并发使用

## 📦 安装

```bash
go get github.com/bingquanzhao/doris-stream-load-client
```

## 🎯 快速开始

### 基本使用

```go
package main

import (
	"fmt"
	doris "github.com/bingquanzhao/doris-stream-load-client"
)

func main() {
	// 配置客户端
	setting := doris.NewLoadSetting().
		AddFeNodes("http://127.0.0.1:8630").
		SetUser("root").
		SetPassword("password").
		Database("test_db").
		Table("test_table").
		CsvFormat(",", "\\n")

	// 创建客户端
	client, err := doris.NewLoadClient(setting)
	if err != nil {
		panic(err)
	}

	// 加载数据
	data := "1,Alice,25\n2,Bob,30\n3,Charlie,35"
	response, err := client.Load(doris.StringReader(data))
	
	if err != nil {
		fmt.Printf("加载失败: %v\n", err)
		return
	}

	if response.Status == doris.SUCCESS {
		fmt.Printf("✅ 加载成功: %d 行\n", response.Resp.NumberLoadedRows)
	} else {
		fmt.Printf("❌ 加载失败: %s\n", response.ErrorMessage)
	}
}
```

### JSON 数据加载

```go
// JSON Lines 格式（推荐）
setting := doris.NewLoadSetting().
	AddFeNodes("http://127.0.0.1:8630").
	SetUser("root").
	SetPassword("password").
	Database("test_db").
	Table("users").
	JsonFormat(doris.JsonObjectLine)

client, _ := doris.NewLoadClient(setting)

jsonData := `{"id":1,"name":"Alice","age":25}
{"id":2,"name":"Bob","age":30}`

response, err := client.Load(doris.StringReader(jsonData))
```

### 结构体数据加载

```go
type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

users := []User{
	{ID: 1, Name: "Alice", Age: 25},
	{ID: 2, Name: "Bob", Age: 30},
}

// 自动序列化为 JSON
response, err := client.Load(users)
```

## 📚 配置选项

### 基本配置

```go
setting := doris.NewLoadSetting().
	AddFeNodes("http://fe1:8630,http://fe2:8630").  // 多 FE 节点
	SetUser("username").
	SetPassword("password").
	Database("test_db").
	Table("test_table").
	SetLabelPrefix("my_app")  // 标签前缀
```

### 数据格式

```go
// CSV 格式
setting.CsvFormat(",", "\\n")  // 列分隔符, 行分隔符

// JSON 格式
setting.JsonFormat(doris.JsonObjectLine)  // 每行一个 JSON 对象
setting.JsonFormat(doris.JsonArray)       // JSON 数组
```

### 批量模式

```go
setting.BatchMode(doris.ASYNC)  // 异步模式（默认）
setting.BatchMode(doris.SYNC)   // 同步模式
```

### 重试配置

```go
// 使用默认重试（5次，指数退避：1s, 2s, 4s, 8s, 16s）
setting.Retry(doris.NewDefaultRetry())

// 自定义重试
setting.Retry(doris.NewRetry(3, 1000))  // 3次重试，1000ms基础间隔
```

### 高级选项

```go
setting.
	AddOption("timeout", "3600").              // 超时时间（秒）
	AddOption("max_filter_ratio", "0.1").      // 最大过滤比例
	AddOption("strict_mode", "true").          // 严格模式
	AddOption("timezone", "Asia/Shanghai")     // 时区
```

### 日志控制

**✨ 统一API设计** - 无需导入额外包，所有日志功能都通过主包提供：

```go
// 设置日志级别（只显示错误）
doris.SetLogLevel(doris.LogLevelError)

// 可用级别
doris.SetLogLevel(doris.LogLevelDebug)  // 显示所有日志
doris.SetLogLevel(doris.LogLevelInfo)   // 显示 Info, Warn, Error（生产推荐）
doris.SetLogLevel(doris.LogLevelWarn)   // 显示 Warn, Error
doris.SetLogLevel(doris.LogLevelError)  // 只显示 Error

// 完全禁用日志
doris.DisableLogging()

// 设置日志输出到文件
file, _ := os.OpenFile("doris-sdk.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
doris.SetLogOutput(file)

// 创建带上下文的日志记录器（适用于并发场景）
logger := doris.NewContextLogger("MyWorker-1")
logger.Infof("Processing batch %d", batchID)

// 集成自定义日志系统（如 logrus）
logger := logrus.New()
doris.SetCustomLogFunc(doris.LogLevelError, logger.Errorf)
doris.SetCustomLogFunc(doris.LogLevelInfo, logger.Infof)

// 或一次性设置所有级别
doris.SetCustomLogFuncs(logger.Debugf, logger.Infof, logger.Warnf, logger.Errorf)
```

## 🔄 并发使用

客户端是线程安全的，可以在多个 goroutine 中安全使用：

```go
func concurrentLoad(client *doris.DorisLoadClient, data string, wg *sync.WaitGroup) {
	defer wg.Done()
	
	response, err := client.Load(doris.StringReader(data))
	if err != nil || response.Status != doris.SUCCESS {
		fmt.Printf("加载失败: %v\n", err)
		return
	}
	
	fmt.Printf("✅ 成功加载 %d 行\n", response.Resp.NumberLoadedRows)
}

func main() {
	client, _ := doris.NewLoadClient(setting)
	
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go concurrentLoad(client, fmt.Sprintf("data_%d", i), &wg)
	}
	wg.Wait()
}
```

## 📊 响应处理

```go
response, err := client.Load(data)

// 检查系统错误
if err != nil {
	fmt.Printf("系统错误: %v\n", err)
	return
}

// 检查加载状态
switch response.Status {
case doris.SUCCESS:
	fmt.Printf("✅ 加载成功\n")
	fmt.Printf("加载行数: %d\n", response.Resp.NumberLoadedRows)
	fmt.Printf("加载字节: %d\n", response.Resp.LoadBytes)
	fmt.Printf("耗时: %d ms\n", response.Resp.LoadTimeMs)
	
case doris.FAILURE:
	fmt.Printf("❌ 加载失败: %s\n", response.ErrorMessage)
}
```

## 🛠️ 生产示例

查看 `examples/` 目录获取更多生产级示例：

```bash
# 运行示例
go run cmd/examples/main.go [single|concurrent|json|basic|all]

# 基础并发示例
go run cmd/examples/main.go basic

# 大规模单批次加载（10万条记录）
go run cmd/examples/main.go single

# 大规模并发加载（100万条记录，10个worker）
go run cmd/examples/main.go concurrent

# JSON 数据加载（5万条记录）
go run cmd/examples/main.go json
```

## 📖 文档

- [日志使用指南](docs/LOGGING.md) - 日志配置和集成
- [示例说明](examples/README.md) - 详细示例文档

## ⚠️ 注意事项

1. **网络连接**: 确保能访问 Doris FE 节点
2. **认证信息**: 提供正确的用户名和密码
3. **表结构**: 数据格式需与目标表结构匹配
4. **错误处理**: 始终检查返回的错误和响应状态

## 🔍 故障排查

### 常见问题

**连接超时**
```go
setting.AddOption("timeout", "7200")  // 增加超时时间
```

**数据格式错误**
```go
setting.AddOption("strict_mode", "true")  // 启用严格模式查看详细错误
```

**过滤率过高**
```go
setting.AddOption("max_filter_ratio", "0.3")  // 调整过滤比例
```

### 调试技巧

- 检查 `response.ErrorMessage` 和 `response.Resp.ErrorURL` 获取详细错误信息
- 大数据量时先用小批量测试
- 关注 `LoadTimeMs`、`NumberFilteredRows` 等指标

## 📄 许可证

Apache License 2.0 