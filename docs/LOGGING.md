# Logging 使用指南

Doris Stream Load Client 提供简单易用的日志系统，支持自定义集成。

## 基本使用

```go
import "github.com/apache/doris-stream-load-client/pkg/log"

// 基本日志记录
log.Debugf("Processing batch with %d records", count)
log.Infof("Load completed successfully")
log.Warnf("Retry attempt %d/%d", attempt, maxAttempts)
log.Errorf("Failed to connect: %v", err)

// 不带格式化
log.Debug("Starting load process")
log.Info("Load completed")
log.Warn("Connection unstable")
log.Error("Load failed")
```

## 设置日志级别

```go
import "github.com/apache/doris-stream-load-client/pkg/log"

// 设置最低日志级别
log.SetLevel(log.LevelInfo)  // 只记录 Info, Warn, Error
log.SetLevel(log.LevelWarn)  // 只记录 Warn, Error
log.SetLevel(log.LevelError) // 只记录 Error
```

可用级别：
- `log.LevelDebug` - Debug 及以上
- `log.LevelInfo` - Info 及以上（推荐生产环境使用）
- `log.LevelWarn` - Warn 及以上
- `log.LevelError` - 仅 Error

## 自定义输出

```go
import (
    "os"
    "github.com/apache/doris-stream-load-client/pkg/log"
)

// 输出到文件
file, _ := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
log.SetOutput(file)

// 输出到标准错误
log.SetOutput(os.Stderr)
```

## 集成现有日志系统

### 使用 log/slog (Go 1.21+)

```go
import (
    "log/slog"
    "github.com/apache/doris-stream-load-client/pkg/log"
)

logger := slog.Default()

log.SetInfoFunc(func(format string, args ...interface{}) {
    logger.Info(fmt.Sprintf(format, args...))
})

log.SetErrorFunc(func(format string, args ...interface{}) {
    logger.Error(fmt.Sprintf(format, args...))
})
```

### 使用 logrus

```go
import (
    "github.com/sirupsen/logrus"
    "github.com/apache/doris-stream-load-client/pkg/log"
)

logger := logrus.New()

log.SetInfoFunc(logger.Infof)
log.SetErrorFunc(logger.Errorf)
log.SetWarnFunc(logger.Warnf)
log.SetDebugFunc(logger.Debugf)
```

### 使用 zap

```go
import (
    "go.uber.org/zap"
    "github.com/apache/doris-stream-load-client/pkg/log"
)

logger, _ := zap.NewProduction()
sugar := logger.Sugar()

log.SetInfoFunc(sugar.Infof)
log.SetErrorFunc(sugar.Errorf)
log.SetWarnFunc(sugar.Warnf)
log.SetDebugFunc(sugar.Debugf)
```

## 禁用日志

```go
import "github.com/apache/doris-stream-load-client/pkg/log"

// 方法1：设置高级别
log.SetLevel(log.Level(999))

// 方法2：设置空函数
noOp := func(format string, args ...interface{}) {}
log.SetInfoFunc(noOp)
log.SetErrorFunc(noOp)
```

## 生产环境建议

1. **设置合适的日志级别**：
```go
log.SetLevel(log.LevelInfo) // 生产环境推荐
```

2. **添加上下文信息**：
```go
log.SetInfoFunc(func(format string, args ...interface{}) {
    myLogger.WithFields(map[string]interface{}{
        "component": "doris-client",
        "trace_id":  getCurrentTraceID(),
    }).Infof(format, args...)
})
```

3. **错误聚合**：
```go
log.SetErrorFunc(func(format string, args ...interface{}) {
    msg := fmt.Sprintf(format, args...)
    myLogger.Error(msg)
    errorTracker.CaptureException(errors.New(msg)) // 发送到错误追踪服务
})
```

## 日志输出示例

客户端会自动记录详细的加载操作信息：

```
[2025/06/02 23:32:16.062] [INFO ] [G-1] [main.go:60] Starting stream load operation
[2025/06/02 23:32:16.062] [INFO ] [G-1] [main.go:60] Target: test.orders (endpoint: 10.16.10.6:8630)
[2025/06/02 23:32:16.062] [INFO ] [G-1] [main.go:60] Label: demo_load_1748878336062_abc123
[2025/06/02 23:32:16.326] [INFO ] [G-1] [stream_loader.go:63] Stream Load Response: {
    "TxnId": 35038,
    "Label": "group_commit_304d09fde1248a70_faab9e33cf850189",
    "Status": "Success",
    "NumberLoadedRows": 100000,
    "LoadBytes": 9638381,
    "LoadTimeMs": 7470
}
[2025/06/02 23:32:16.327] [INFO ] [G-1] [stream_loader.go:63] Load operation completed successfully
```

日志格式包含：
- **时间戳**：毫秒级精度 `[2025/06/02 23:32:16.062]`
- **级别**：`[INFO]`, `[WARN]`, `[ERROR]`
- **Goroutine ID**：`[G-1]` 用于并发追踪
- **位置**：`[main.go:60]` 源码位置
- **消息**：具体日志内容 