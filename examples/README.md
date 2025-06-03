# Doris Stream Load Client - Examples

Production-ready examples demonstrating efficient data loading with unified orders schema.

## 🚀 Quick Start

```bash
# Run individual examples
go run cmd/examples/main.go single      # 100k records single batch
go run cmd/examples/main.go concurrent  # 1M records with 10 workers  
go run cmd/examples/main.go json        # 50k JSON records
go run cmd/examples/main.go basic       # Simple concurrent demo

# Run all examples
go run cmd/examples/main.go all
```

## 📊 Examples Overview

| Example | Records | Format | Workers | Best For |
|---------|---------|--------|---------|----------|
| `single` | 100,000 | CSV | 1 | Single-threaded batch loading |
| `concurrent` | 1,000,000 | CSV | 10 | High-throughput production |
| `json` | 50,000 | JSON Lines | 1 | Structured data loading |
| `basic` | 5 | CSV | 5 | Learning & development |

## 🗃️ Unified Data Schema

All examples use consistent **orders** table schema:

```sql
create table `orders`
(
    OrderID     varchar(200),
    CustomerID  varchar(200),
    ProductName varchar(200),
    Category    varchar(200),
    Brand       varchar(200),
    Quantity    varchar(200),
    UnitPrice   varchar(200),
    TotalAmount varchar(200),
    Status      varchar(200),
    OrderDate   varchar(200),
    Region      varchar(200)
) duplicate key(OrderID)
distributed by hash(OrderID) buckets 10
properties("replication_num"="1");
```

## ⚙️ Configuration

### Basic Setup

```go
setting := doris.NewLoadSetting().
    AddFeNodes("http://127.0.0.1:8630").
    SetUser("root").
    SetPassword("123456").
    Database("test").
    Table("orders").
    SetLabelPrefix("your_app").
    CsvFormat(",", "\\n").           // or JsonFormat(doris.JsonObjectLine)
    Retry(doris.NewRetry(3, 2000)).  // 3 retries, 2s base interval
    BatchMode(doris.ASYNC)           // ASYNC for better performance
```

### Production Settings

```go
// More aggressive retries for production
Retry(doris.NewRetry(5, 1000))  // 5 retries: [1s, 2s, 4s, 8s, 16s] = ~31s total

// Meaningful label prefixes
SetLabelPrefix("prod_orders")
```

### Log Control (Unified API)

All log control through main package - no additional imports needed:

```go
// Set log level for examples
doris.SetLogLevel(doris.LogLevelError)  // Only show errors
doris.SetLogLevel(doris.LogLevelInfo)   // Show info/warn/error (recommended)

// Create context loggers for concurrent scenarios
workerLogger := doris.NewContextLogger("Worker-1")
workerLogger.Infof("Processing %d records", count)

// Disable all logging
doris.DisableLogging()

// Integrate with custom logging systems
logger := logrus.New()
doris.SetCustomLogFuncs(logger.Debugf, logger.Infof, logger.Warnf, logger.Errorf)
```

## 📈 Performance

| Example | Generation Rate | Typical Load Time | Total Throughput |
|---------|----------------|-------------------|------------------|
| Single | ~960k records/sec | 2-5s | ~7k-15k records/sec |
| Concurrent | ~850k records/sec | 10-30s | ~11k-25k records/sec |
| JSON | ~855k records/sec | 2-4s | ~5k-10k records/sec |

## 🔧 Prerequisites

1. **Go 1.19+**
2. **Running Doris cluster** (or configure endpoints in examples)
3. **Database and table setup:**

```sql
-- Create database
CREATE DATABASE IF NOT EXISTS test;

-- Create orders table
USE test;
create table `orders`
(
    OrderID     varchar(200),
    CustomerID  varchar(200),
    ProductName varchar(200),
    Category    varchar(200),
    Brand       varchar(200),
    Quantity    varchar(200),
    UnitPrice   varchar(200),
    TotalAmount varchar(200),
    Status      varchar(200),
    OrderDate   varchar(200),
    Region      varchar(200)
) duplicate key(OrderID)
distributed by hash(OrderID) buckets 10
properties("replication_num"="1");
```

## 💡 Usage Tips

### Batch Size Recommendations
- **CSV**: 100k records (~10MB)
- **JSON**: 50k records (~12MB)  
- **Memory limit**: Keep batches under 15MB

### Concurrency Guidelines
- **Workers**: 1-2x CPU cores
- **Records per worker**: 50k-200k depending on complexity

### Error Handling
Examples include retry mechanisms and graceful error handling. Check logs for connection issues or data validation errors.

## 📝 Simple Output Format

Examples use simplified response handling:

```bash
🎉 Load completed successfully!
📊 Records: 100000, Size: 9.2 MB, Time: 2.5s
📈 Rate: 40000 records/sec, 3.7 MB/sec
📋 Label: prod_batch_test_orders_xxx, Loaded: 100000 rows
```

## 🏗️ Code Structure

```
examples/
├── cmd/examples/main.go           # Unified entry point
├── data_generator.go              # Unified data generation
├── production_single_batch_example.go
├── production_concurrent_example.go  
├── production_json_example.go
├── concurrent_load_example.go
└── README.md
```

For detailed implementation, see individual example files. 