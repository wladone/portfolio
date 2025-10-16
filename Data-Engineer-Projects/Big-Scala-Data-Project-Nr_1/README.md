# Log Analytics Pipeline

A production-ready real-time log analytics pipeline built with Apache Spark 3.5 (Scala 2.12) that processes Apache Combined Log Format and computes various metrics including requests per minute, error rates, status code distribution, and top endpoints.

## 🚀 Features

- **🔄 Real-time Processing**: Structured Streaming for continuous log processing with watermarking
- **📡 Multiple Sources**: File-based streaming and Kafka integration with robust error handling
- **📊 Rich Metrics**:
  - Requests per minute (1-minute tumbling windows)
  - 5xx errors per minute with precise classification
  - Status code distribution (1xx, 2xx, 3xx, 4xx, 5xx)
  - Top endpoints (5-minute sliding windows with 1-minute slides)
- **💾 Multiple Sinks**: Console output, Parquet files, and Cassandra storage with DataFrames API
- **⏰ Time-series Optimized**: Cassandra tables with TWCS, 7-day TTL, and daily bucketing
- **🧪 Production Ready**: Comprehensive testing (15+ test cases), CI/CD, Docker support
- **🛡️ Robust Parsing**: Resilient Apache Combined Log Format parser with edge case handling
- **🏗️ Clean Architecture**: Modular design with separated concerns and input validation

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Log Sources   │───▶│  Spark Streaming │───▶│   Metrics Sink  │
│                 │    │                  │    │                 │
│ • Web Servers   │    │ • Log Parser     │    │ • Console       │
│ • Applications  │    │ • Windowing      │    │ • Parquet       │
│ • File System   │    │ • Aggregations   │    │ • Cassandra     │
│ • Kafka Topics  │    │ • Watermarking   │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 📋 Prerequisites

- **Java 17** or higher
- **Scala 2.12.18**
- **SBT 1.8+** (Scala Build Tool)
- **Apache Spark 3.5.x** (for cluster deployment)

### Optional Dependencies

- **Apache Kafka** (for Kafka streaming)
- **Cassandra 4.x** (for time-series storage)
- **Docker & Docker Compose** (for local development)

## ✨ Recent Improvements

### 🔧 Code Quality & Architecture
- **🛡️ Enhanced Error Handling**: Robust parsing with graceful failure handling
- **🏗️ Modular Design**: Separated concerns with dedicated metric computation functions
- **✅ Input Validation**: Automatic directory creation and path validation
- **🧹 Clean Code**: Improved CLI argument parsing with default values
- **📝 Better Logging**: Enhanced visibility into application state and issues

### 🧪 Testing & Reliability
- **🎯 Comprehensive Testing**: 15+ test cases covering edge cases and boundary conditions
- **🔍 Resilient Parsing**: Enhanced LogParser with malformed input handling
- **⚡ Boundary Testing**: Complete status code classification coverage
- **🌍 Internationalization**: Unicode character support in log parsing
- **📊 Large Data Support**: Extreme value and long string handling

### 🚀 Performance & Operations
- **⚡ Optimized Streaming**: Efficient windowing and watermarking strategies
- **💾 Smart Storage**: Time-bucketed Cassandra tables with TWCS compaction
- **🔄 CI/CD Ready**: Automated formatting, testing, and security scanning
- **🐳 Docker Optimized**: Multi-stage builds and development environments

## 🛠️ Quick Start

### 1. Clone and Build

```bash
git clone <repository-url>
cd log-analytics-pipeline
sbt clean compile test assembly
```

### 2. Local Showcase (Windows)

For Windows users, use the PowerShell showcase script that demonstrates all project features:

```powershell
# Run the complete showcase (includes all features)
.\run_local_showcase.ps1

# Or with custom parameters
.\run_local_showcase.ps1 -LogLines 5000 -SparkMaster "local[2]" -Memory "4g"
```

The showcase script automatically:
- ✅ Generates realistic log data (Scala LogGenerator)
- ✅ Tests log parsing functionality (Scala LogParserTest)
- ✅ Runs unit tests
- ✅ Builds the application
- ✅ Executes performance benchmarks (Scala PerformanceBenchmark)
- ✅ Runs the streaming application
- ✅ Displays analytics results
- ✅ **Bonus**: Launches web dashboard (Scala LogAnalyticsDashboard)

### 3. Manual Setup (Linux/macOS)

#### Generate Sample Data

**Primary (Scala - Recommended):**
```bash
# Generate 1000 log entries in steady mode
sbt "runMain com.example.logstream.LogGenerator --output input/access.log --lines 1000 --mode steady"

# Or use the convenience script
./scripts/run_local.sh --app filestream --generate-logs --num-logs 1000
```

**Fallback (Python - Deprecated):**
```bash
# Only use if Scala is not available
python3 python-fallback/generate_logs.py --output input/access.log --lines 1000 --mode steady
```

#### Run File Stream Application

```bash
# Basic execution
./scripts/run_local.sh --app filestream

# With Cassandra enabled
./scripts/run_local.sh --app filestream --cassandra-enabled --cassandra-host 127.0.0.1

# Custom paths
./scripts/run_local.sh --app filestream --input /path/to/logs --output /path/to/output
```

#### Run Kafka Stream Application

```bash
# Start Kafka infrastructure
cd docker && docker-compose up -d

# Run Kafka streaming app
./scripts/run_local.sh --app kafkastream --cassandra-enabled
```

## 📊 Metrics Output

The pipeline computes and outputs the following metrics:

### 1. Requests Per Minute (RPM)
```json
{
  "window_start": "2023-12-01T10:00:00.000Z",
  "window_end": "2023-12-01T10:01:00.000Z",
  "request_count": 1250,
  "metric_type": "requests_per_minute"
}
```

### 2. 5xx Errors Per Minute
```json
{
  "window_start": "2023-12-01T10:00:00.000Z",
  "window_end": "2023-12-01T10:01:00.000Z",
  "error_count": 15,
  "metric_type": "errors_per_minute"
}
```

### 3. Status Code Distribution
```json
{
  "window_start": "2023-12-01T10:00:00.000Z",
  "window_end": "2023-12-01T10:01:00.000Z",
  "status_class": "2xx",
  "count": 1100,
  "metric_type": "status_distribution"
}
```

### 4. Top Endpoints
```json
{
  "window_start": "2023-12-01T10:00:00.000Z",
  "window_end": "2023-12-01T10:05:00.000Z",
  "endpoint": "/api/users",
  "request_count": 450,
  "metric_type": "top_endpoints"
}
```

## 🗄️ Cassandra Integration

### Schema Setup

1. **Start Cassandra**:
```bash
cd docker && docker-compose up -d cassandra
```

2. **Apply Schema**:
```bash
# Apply base schema
docker exec log-analytics-cassandra cqlsh -f /docker-entrypoint-initdb.d/01-schema.cql

# Apply time-bucketed schema
docker exec log-analytics-cassandra cqlsh -f /docker-entrypoint-initdb.d/02-schema-by-day.cql
```

### Time-Series Features

- **TWCS (Time Window Compaction Strategy)**: Optimized for time-series data
- **7-day TTL**: Automatic data expiration
- **Daily Bucketing**: Efficient partition management
- **Batch Processing**: foreachBatch sink for exactly-once processing

### Query Examples

```sql
-- Get requests per minute for a specific day
SELECT * FROM requests_per_minute_by_day
WHERE bucket_date = '2023-12-01'
ORDER BY window_start DESC;

-- Get error rate analysis
SELECT bucket_date, window_start,
       request_count, error_count,
       (error_count / request_count * 100) as error_rate
FROM requests_per_minute_by_day
WHERE bucket_date = '2023-12-01'
  AND request_count > 0;

-- Get top endpoints for an hour window
SELECT endpoint, request_count
FROM top_endpoints_by_day
WHERE bucket_date = '2023-12-01'
  AND window_start >= '2023-12-01 10:00:00'
  AND window_start < '2023-12-01 11:00:00'
ORDER BY request_count DESC
LIMIT 10;
```

## 🐳 Docker Development Environment

### Full Development Stack (Kafka + Cassandra)

Start all services including Kafka:

```bash
cd docker
docker-compose up -d
```

Services included:
- **Zookeeper** (port 2181)
- **Kafka** (port 9092)
- **Kafka UI** (port 8080) - Web interface for Kafka management
- **Cassandra** (port 9042)
- **Cassandra Web** (port 8081) - Web interface for Cassandra
- **Log Producer** - Generates sample data to Kafka topic

### Cassandra-Only Development Stack

For file-based streaming with Cassandra sink:

```bash
cd docker
docker-compose -f docker-compose.dev.yml up -d
```

### Build Application JAR

```bash
# Build the fat JAR with all dependencies
sbt clean assembly

# Verify JAR was created
ls -la target/scala-2.12/log-analytics-pipeline-1.0.0.jar
```

### Build Docker Image

```bash
# Build the application Docker image
cd docker
docker build -t log-analytics-pipeline:latest .

# Or build from project root
docker build -f docker/Dockerfile -t log-analytics-pipeline:latest .
```

### Run with Docker Compose

#### Cassandra-Only Stack

```bash
cd docker

# Start Cassandra and Spark driver
docker-compose -f docker-compose.dev.yml up -d cassandra spark-driver

# View logs
docker-compose -f docker-compose.dev.yml logs -f spark-driver

# Stop services
docker-compose -f docker-compose.dev.yml down
```

#### With Cassandra Web UI

```bash
cd docker

# Start with Cassandra management UI
docker-compose -f docker-compose.dev.yml --profile cassandra-ui up -d

# Access Cassandra Web UI: http://localhost:8081
```

#### Full Stack with Kafka

```bash
cd docker

# Start everything including Kafka
docker-compose -f docker-compose.dev.yml --profile kafka up -d

# Start Kafka UI as well
docker-compose -f docker-compose.dev.yml --profile kafka --profile kafka-ui up -d

# Access web interfaces:
# - Kafka UI: http://localhost:8080
# - Cassandra Web: http://localhost:8081
```

### Manual Docker Execution

```bash
# Run FileStreamApp with Cassandra
docker run -it --rm \
  --name log-analytics-app \
  -v $(pwd)/input:/app/input \
  -v $(pwd)/output:/app/output \
  -v $(pwd)/checkpoint:/app/checkpoint \
  -e CASSANDRA_HOST=cassandra \
  log-analytics-pipeline:latest \
  --input /app/input \
  --output /app/output \
  --checkpoint /app/checkpoint \
  --cassandra.enabled true \
  --cassandra.host cassandra \
  --cassandra.keyspace log_analytics

# Run KafkaStreamApp
docker run -it --rm \
  --name log-analytics-kafka \
  --network container:log-analytics-kafka-dev \
  log-analytics-pipeline:latest \
  com.example.logstream.KafkaStreamApp \
  --kafka.bootstrap.servers kafka:29092 \
  --kafka.topic log-events
```

### Access Web Interfaces

- **Kafka UI**: http://localhost:8080
- **Cassandra Web**: http://localhost:8081

### Kafka Topic Management

```bash
# Create log-events topic
docker exec log-analytics-kafka-dev kafka-topics --create --topic log-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# List topics
docker exec log-analytics-kafka-dev kafka-topics --list --bootstrap-server localhost:9092

# Consume from topic
docker exec log-analytics-kafka-dev kafka-console-consumer --topic log-events --bootstrap-server localhost:9092 --from-beginning
```

### Cassandra Schema Management

```bash
# Check if Cassandra is ready
docker exec log-analytics-cassandra-dev cqlsh -u cassandra -p cassandra -e "DESCRIBE KEYSPACES"

# View available tables
docker exec log-analytics-cassandra-dev cqlsh -u cassandra -p cassandra -e "USE log_analytics; DESCRIBE TABLES;"

# Query sample data
docker exec log-analytics-cassandra-dev cqlsh -u cassandra -p cassandra -e "USE log_analytics; SELECT * FROM requests_per_minute LIMIT 5;"
```

## 🔧 Configuration

### Application Arguments

#### FileStreamApp
```bash
--input PATH              # Input directory path (default: input/)
--output PATH             # Output directory path (default: output/)
--checkpoint PATH         # Checkpoint directory path (default: checkpoint/)
--cassandra.enabled BOOL  # Enable Cassandra sink (default: false)
--cassandra.host HOST     # Cassandra host (default: localhost)
--cassandra.keyspace KS   # Cassandra keyspace (default: log_analytics)
```

#### KafkaStreamApp
```bash
--kafka.bootstrap.servers SERVERS  # Kafka bootstrap servers (default: localhost:9092)
--kafka.topic TOPIC                # Kafka topic (default: log-events)
--output PATH                      # Output directory path (default: output/kafka/)
--checkpoint PATH                  # Checkpoint directory path (default: checkpoint/kafka/)
--cassandra.enabled BOOL           # Enable Cassandra sink (default: false)
--cassandra.host HOST              # Cassandra host (default: localhost)
--cassandra.keyspace KS            # Cassandra keyspace (default: log_analytics)
```

## 📁 Project Structure

```
log-analytics-pipeline/
├── 📄 build.sbt                          # SBT build configuration with Spark 3.5.x
├── 📄 .scalafmt.conf                     # Scala formatting rules
├── 📄 project/plugins.sbt                # SBT plugins (assembly, scalafmt)
├── 📄 README.md                          # Comprehensive documentation
├── 📄 run_local_showcase.ps1             # Windows PowerShell showcase script
├── � src/main/scala/com/example/logstream/
│   ├── 📄 LogParser.scala               # Apache Combined Log Format parser
│   ├── 📄 FileStreamApp.scala           # File-based streaming application
│   └── 📄 KafkaStreamApp.scala          # Kafka-based streaming application
├── 📁 src/test/scala/com/example/logstream/
│   └── 📄 LogParserSpec.scala           # Comprehensive unit tests (15+ test cases)
├── 📁 scripts/
│   ├── 📄 generate_logs.py              # Realistic log data generator
│   ├── 📄 run_local.sh                  # Local execution script with CLI args
│   ├── 📄 test_logparser.py             # Python log parsing test
│   └── 📄 benchmark_performance.py      # Performance benchmarking
├──  cassandra/
│   ├── 📄 schema.cql                    # Base Cassandra schema
│   └── 📄 schema_by_day.cql             # Time-bucketed schema with TWCS & TTL
├── 📁 docker/
│   ├── 📄 docker-compose.yml            # Full-stack development (Kafka + Cassandra)
│   ├── 📄 docker-compose.dev.yml        # Cassandra-only development stack
│   ├── 📄 Dockerfile                    # Multi-stage build (SBT → runtime)
│   └── 📄 Dockerfile.log-producer       # Kafka log producer container
└── 📁 .github/workflows/
    └── 📄 scala.yml                     # CI/CD pipeline (format, test, build, security)
```

## 🧪 Testing

### Run Unit Tests

```bash
sbt test
```

### Run with Coverage

```bash
sbt clean coverage test coverageReport
```

### Integration Testing

```bash
# Test with sample data
./scripts/run_local.sh --app filestream --generate-logs --num-logs 100

# Test Kafka streaming (requires running Kafka)
./scripts/run_local.sh --app kafkastream
```

## 🏗️ Development Practices

### Code Quality Standards

- **Scala Formatting**: All code follows `.scalafmt.conf` rules
- **Testing Coverage**: Comprehensive unit tests for all public methods
- **Error Handling**: Graceful degradation with meaningful error messages
- **Logging**: Structured logging with appropriate levels (ERROR, WARN, INFO, DEBUG)
- **Documentation**: Scaladoc comments for all public APIs

### Code Organization

- **Single Responsibility**: Each function has a single, well-defined purpose
- **Immutable Data**: Preference for immutable data structures
- **Type Safety**: Strong typing with case classes for configuration
- **Modular Design**: Separated concerns between parsing, computation, and I/O

### Commit Standards

```bash
# Format: <emoji> <type>: <description>

🚀 feat: add new streaming feature
🐛 fix: resolve parsing edge case
📚 docs: update README with new examples
🧪 test: add boundary condition tests
🔧 refactor: improve argument parsing
💄 style: format code according to scalafmt rules
```

### Pre-commit Checklist

- [ ] Code compiles without warnings (`sbt clean compile`)
- [ ] All tests pass (`sbt test`)
- [ ] Code is formatted (`sbt scalafmtCheckAll`)
- [ ] New features have corresponding tests
- [ ] Documentation is updated for API changes
- [ ] Docker setup tested if containers were modified

## 🐳 Docker Development Workflow

### Quick Start with Docker

1. **Build the Application JAR**
   ```bash
   sbt clean assembly
   ```

2. **Build Docker Image**
   ```bash
   cd docker
   docker build -t log-analytics-pipeline:latest .
   ```

3. **Start Development Environment**
   ```bash
   # Start Cassandra + Spark driver
   docker-compose -f docker-compose.dev.yml up -d cassandra spark-driver

   # Or with Kafka support
   docker-compose -f docker-compose.dev.yml --profile kafka up -d
   ```

4. **Generate Sample Data**
   ```bash
   # Generate logs for testing
   python3 scripts/generate_logs.py --output input/access.log --lines 1000
   ```

5. **Monitor the Application**
   ```bash
   # View Spark driver logs
   docker-compose -f docker-compose.dev.yml logs -f spark-driver

   # Access Spark UI (if available)
   # http://localhost:4040

   # Access Cassandra Web UI
   # http://localhost:8081
   ```

### Docker Commands Reference

```bash
# Build JAR
sbt clean assembly

# Build Docker image
docker build -f docker/Dockerfile -t log-analytics-pipeline:latest .

# Start services
cd docker
docker-compose -f docker-compose.dev.yml up -d

# View logs
docker-compose -f docker-compose.dev.yml logs -f

# Stop services
docker-compose -f docker-compose.dev.yml down

# Clean up volumes (removes data)
docker-compose -f docker-compose.dev.yml down -v
```

### Cassandra Integration in Docker

The Docker setup automatically:
- ✅ Initializes Cassandra schema on startup
- ✅ Creates time-series tables with TWCS and TTL
- ✅ Provides web UI for data exploration
- ✅ Mounts volumes for data persistence

```bash
# Check Cassandra status
docker exec log-analytics-cassandra-dev nodetool status

# Query data via CQL
docker exec log-analytics-cassandra-dev cqlsh -u cassandra -p cassandra \
  -e "USE log_analytics; SELECT * FROM requests_per_minute LIMIT 5;"
```

##  Deployment

### Local Mode

```bash
# File streaming
spark-submit \
  --class com.example.logstream.FileStreamApp \
  --master local[*] \
  target/scala-2.12/log-analytics-pipeline-1.0.0.jar \
  --input /path/to/logs \
  --output /path/to/output

# Kafka streaming
spark-submit \
  --class com.example.logstream.KafkaStreamApp \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  target/scala-2.12/log-analytics-pipeline-1.0.0.jar \
  --kafka.bootstrap.servers localhost:9092
```

### Cluster Mode

```bash
# Submit to Spark cluster
spark-submit \
  --class com.example.logstream.FileStreamApp \
  --master spark://master:7077 \
  --deploy-mode cluster \
  --driver-memory 2g \
  --executor-memory 2g \
  --executor-cores 2 \
  target/scala-2.12/log-analytics-pipeline-1.0.0.jar
```

### Docker Deployment

```bash
# Build application image
docker build -f docker/Dockerfile -t log-analytics-pipeline .

# Run with Docker
docker run -it --rm \
  -v /local/logs:/app/input \
  -v /local/output:/app/output \
  log-analytics-pipeline \
  --input /app/input \
  --output /app/output
```

## 📈 Monitoring and Visualization

### Parquet Output Structure

```
output/
├── requests_per_minute/
│   ├── window_start=2023-12-01 10:00:00/
│   │   ├── part-00000.parquet
│   │   └── part-00001.parquet
├── errors_per_minute/
├── status_distribution/
└── top_endpoints/
```

### Power BI Integration

1. **Connect to Parquet files**:
   - Open Power BI Desktop
   - Get Data → Folder
   - Point to output directory
   - Combine Parquet files

2. **Create Dashboard**:
   - Time-based charts for RPM trends
   - Error rate monitoring
   - Status code distribution pie charts
   - Top endpoints bar charts

### Grafana Integration

```bash
# Using Cassandra data source
SELECT
  window_start as time,
  request_count as value,
  'requests' as metric
FROM requests_per_minute_by_day
WHERE bucket_date = '2023-12-01'
ORDER BY window_start
```

## 🔄 CI/CD Pipeline

The project includes a comprehensive GitHub Actions workflow that:

- **Validates** Scala formatting
- **Compiles** the project
- **Runs tests** with coverage
- **Creates assembly JAR**
- **Performs security scanning**
- **Builds Docker images** (on main branch)

### Workflow Triggers

- Push to `main` or `develop` branches
- Pull requests to `main` or `develop`
- Manual trigger via workflow dispatch

## 📊 Performance Tuning

### Spark Configuration

```scala
// In application code
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.streaming.checkpointLocation", "/path/to/checkpoint")
```

### Memory Settings

```bash
# For large datasets
spark-submit \
  --driver-memory 4g \
  --executor-memory 4g \
  --conf spark.sql.shuffle.partitions=200 \
  ...
```

### Cassandra Optimization

- **Partitioning**: Data is partitioned by date for even distribution
- **Compaction**: TWCS ensures efficient time-series compaction
- **TTL**: 7-day retention prevents unbounded growth
- **Batch Size**: foreachBatch processes data in optimal chunks

## 🐛 Troubleshooting

### Common Issues

1. **No log data processed**
   ```bash
   # Check if input files exist and have correct format
   ls -la input/
   head -5 input/access.log
   ```

2. **Cassandra connection failed**
   ```bash
   # Verify Cassandra is running
   docker exec log-analytics-cassandra nodetool status

   # Check Cassandra logs
   docker logs log-analytics-cassandra
   ```

3. **Kafka consumer errors**
   ```bash
   # Check Kafka topics
   docker exec log-analytics-kafka kafka-topics --list --bootstrap-server localhost:9092

   # Check consumer groups
   docker exec log-analytics-kafka kafka-consumer-groups --list --bootstrap-server localhost:9092
   ```

### Debug Mode

```bash
# Enable debug logging
./scripts/run_local.sh --app filestream --log-level DEBUG

# Check Spark UI
# Access http://localhost:4040 when application is running
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Run tests (`sbt test`)
4. Format code (`sbt scalafmt`)
5. Commit changes (`git commit -m 'Add amazing feature'`)
6. Push to branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Apache Spark team for the excellent streaming framework
- DataStax for the Spark-Cassandra connector
- The Scala community for continuous improvements

## 🔌 API Reference

### Core Components

#### LogParser
```scala
object LogParser

// Parse Apache Combined Log Format line
def parseLogLine(logLine: String): Option[LogEntry]

// Extract HTTP method from request line
def extractHttpMethod(requestLine: String): String

// Extract endpoint from request line
def extractEndpoint(requestLine: String): String

// Get status class (1xx, 2xx, 3xx, 4xx, 5xx)
def getStatusClass(statusCode: Int): String

// Check if status code indicates an error (4xx, 5xx)
def isErrorStatus(statusCode: Int): Boolean

// Check if status code indicates server error (5xx)
def isServerError(statusCode: Int): Boolean
```

#### FileStreamApp
```scala
object FileStreamApp

// Main entry point with CLI argument parsing
def main(args: Array[String]): Unit

// Parse command line arguments
private def parseArgs(args: Array[String]): Config

// Validate and create input paths
private def validateInputPaths(inputPath: String): Unit

// Compute requests per minute metrics
private def computeRequestsPerMinute(stream: DataFrame): DataFrame

// Compute 5xx errors per minute metrics
private def computeErrorsPerMinute(stream: DataFrame): DataFrame

// Compute status code distribution metrics
private def computeStatusDistribution(stream: DataFrame): DataFrame

// Compute top endpoints metrics
private def computeTopEndpoints(stream: DataFrame): DataFrame
```

### Configuration Schema

```scala
case class Config(
  inputPath: String = "input/",
  outputPath: String = "output/",
  checkpointPath: String = "checkpoint/",
  cassandraEnabled: Boolean = false,
  cassandraHost: String = "localhost",
  cassandraKeyspace: String = "log_analytics",
  processingTime: String = "0 seconds"
)
```

### Data Schemas

#### LogEntry Schema
```scala
case class LogEntry(
  client_ip: String,
  remote_logname: String,
  user: String,
  timestamp: java.sql.Timestamp,
  request_line: String,
  status_code: Int,
  response_size: Long,
  referer: String,
  user_agent: String,
  processing_time_ms: Long = 0L
)
```

#### Metrics Output Schemas
All metrics include: `window_start`, `window_end`, `metric_type`, plus metric-specific fields.

## 🧪 Testing & Quality Assurance

### Test Coverage

The project includes **comprehensive test coverage** with 15+ test scenarios:

#### ✅ LogParser Testing
- **Valid Log Parsing**: Apache Combined Log Format compliance
- **Malformed Input Handling**: Graceful failure on invalid data
- **Edge Cases**: IPv6 addresses, Unicode characters, large response sizes
- **Boundary Conditions**: Status code classification limits
- **Timestamp Parsing**: Multiple format support and error handling

#### ✅ Unit Test Scenarios
```bash
# Run all tests
sbt test

# Run specific test class
sbt "testOnly com.example.logstream.LogParserSpec"

# Run with verbose output
sbt test:testQuick

# Generate test report
sbt test:testQuick test:compile
```

### Code Quality Validation

#### ✅ Formatting & Style
```bash
# Check code formatting
sbt scalafmtCheckAll

# Auto-format code
sbt scalafmtAll

# Check for unused imports
sbt clean compile
```

#### ✅ Compilation Verification
```bash
# Clean compile check
sbt clean compile

# Assembly JAR creation
sbt assembly

# Dependency analysis
sbt dependencyUpdates
```

### Integration Testing

#### File Stream Testing
```bash
# 1. Generate test data
python3 scripts/generate_logs.py --output input/access.log --lines 1000

# 2. Run application with test data
./scripts/run_local.sh --app filestream --input input/ --output output/

# 3. Verify output files created
ls -la output/requests_per_minute/
ls -la output/errors_per_minute/
ls -la output/status_distribution/
ls -la output/top_endpoints/
```

#### Kafka Stream Testing (Optional)
```bash
# 1. Start Kafka infrastructure
cd docker && docker-compose up -d zookeeper kafka

# 2. Create topic
docker exec log-analytics-kafka kafka-topics --create --topic log-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# 3. Run Kafka streaming app
./scripts/run_local.sh --app kafkastream --kafka.bootstrap.servers localhost:9092

# 4. Generate data to Kafka
docker run --network container:log-analytics-kafka log-analytics-producer:latest
```

#### Cassandra Integration Testing
```bash
# 1. Start Cassandra
cd docker && docker-compose -f docker-compose.dev.yml up -d cassandra

# 2. Verify schema initialization
docker exec log-analytics-cassandra-dev cqlsh -u cassandra -p cassandra -e "DESCRIBE KEYSPACES;"

# 3. Run app with Cassandra enabled
./scripts/run_local.sh --app filestream --cassandra-enabled --cassandra-host 127.0.0.1

# 4. Verify data insertion
docker exec log-analytics-cassandra-dev cqlsh -u cassandra -p cassandra \
  -e "USE log_analytics; SELECT COUNT(*) FROM requests_per_minute;"
```

### Debugging & Troubleshooting

#### ✅ Application Debugging
```bash
# Enable debug logging
./scripts/run_local.sh --app filestream --log-level DEBUG

# Check Spark UI (when running)
# Access http://localhost:4040

# Monitor application logs
tail -f logs/application.log
```

#### ✅ Docker Debugging
```bash
# View container logs
docker-compose -f docker-compose.dev.yml logs -f spark-driver

# Execute commands in containers
docker exec -it log-analytics-cassandra-dev cqlsh -u cassandra -p cassandra

# Check container health
docker-compose -f docker-compose.dev.yml ps

# View resource usage
docker stats
```

#### ✅ Common Issues & Solutions

**Issue**: `Connection refused` errors
```bash
# Solution: Wait for services to fully start
sleep 30
# Or check service health
docker-compose -f docker-compose.dev.yml ps
```

**Issue**: `Permission denied` on input/output directories
```bash
# Solution: Fix directory permissions
chmod 755 input/ output/ checkpoint/
```

**Issue**: Cassandra connection timeout
```bash
# Solution: Verify Cassandra is ready
docker exec log-analytics-cassandra-dev nodetool status
```

### Performance Testing

#### Load Testing
```bash
# Generate high-volume test data
python3 scripts/generate_logs.py --output input/access.log --lines 100000 --mode realistic

# Monitor resource usage during processing
htop  # or docker stats
```

#### Memory Testing
```bash
# Test with different memory settings
./scripts/run_local.sh --app filestream --spark-master "local[1]"  # Single core

# Monitor garbage collection
java -XX:+PrintGC -XX:+PrintGCDetails -jar target/scala-2.12/log-analytics-pipeline-1.0.0.jar
```

## 📞 Support

For questions, issues, or contributions, please:

1. Check existing [Issues](../../issues)
2. Create a new [Issue](../../issues/new) with detailed information
3. Join our [Discussions](../../discussions)

## 📋 Changelog

### v1.0.0 (Latest)
- **🚀 Initial Release**: Complete real-time log analytics pipeline
- **🧪 Enhanced Testing**: 15+ comprehensive test cases with edge case coverage
- **🐳 Docker Support**: Multi-stage builds and development environments
- **💾 Cassandra Integration**: Time-series optimization with TWCS and TTL
- **🔧 Code Quality**: Refactored architecture with modular design and input validation
- **📚 Documentation**: Comprehensive guides, API reference, and testing documentation

---

## 🛒 E-Commerce Ingestion

A comprehensive e-commerce data ingestion pipeline that collects product data from public APIs, processes it through a Spark-based ETL pipeline, and provides real-time order analytics.

### Features

- **🔄 Dual Ingestion Modes**: Standalone (no Spark) and Spark-based processing
- **📡 Multiple Data Sources**: DummyJSON and FakeStore APIs with pluggable architecture
- **⚡ Rate Limiting**: Configurable RPS with token bucket algorithm and exponential backoff
- **🛡️ Robust Error Handling**: Retry logic, timeouts, and graceful failure handling
- **📊 Real-time Analytics**: Structured Streaming for order events with watermarking
- **💾 Data Lake Architecture**: Bronze → Silver ETL with deduplication and partitioning
- **🐍 Python Fallback**: Both Scala and Python implementations for maximum compatibility
- **🪟 Windows-Friendly**: PowerShell and Batch scripts for easy execution

### Quick Start

#### 1. Download Product Data

**Scala (Recommended):**
```bash
# Download from DummyJSON API
sbt "runMain com.example.ecommerce.ingest.EcomIngestor --source dummyjson --out data/ecommerce/raw --page-size 100 --max-pages 2"
```

**Python (Fallback):**
```bash
python3 scripts/ecom_download.py --source dummyjson --out data/ecommerce/raw --page-size 100 --max-pages 2
```

**Windows Scripts:**
```powershell
# PowerShell
.\scripts\run_ecom_ingest.ps1 -Source dummyjson -PageSize 100 -MaxPages 2

# Batch
scripts\run_ecom_ingest.bat --source dummyjson --page-size 100 --max-pages 2
```

#### 2. Run Batch ETL

```bash
# Process raw data through Spark ETL
sbt "runMain com.example.ecommerce.spark.EcomBatchJob"

# Or use Windows script
.\scripts\run_ecom_batch.ps1
```

#### 4. Launch Analytics Dashboard

```bash
# Start the web dashboard for data visualization
sbt "runMain com.example.ecommerce.dashboard.EcommerceDashboard"

# Or use Windows script
.\scripts\run_ecommerce_dashboard.ps1
```

The dashboard will be available at **http://localhost:8081/** and provides:
- **Real-time Analytics**: Product statistics, price distributions, ratings
- **Interactive Charts**: Price ranges, rating distributions, top brands/categories
- **Sample Data**: View individual product details
- **Data Source Selection**: Switch between DummyJSON and FakeStore data

#### 5. Generate & Stream Orders (Optional)

```bash
# Generate synthetic order events
sbt "runMain com.example.ecommerce.generator.OrdersGenerator --rate 5 --out data/ecommerce/orders_incoming"

# Start streaming analytics
sbt "runMain com.example.ecommerce.spark.EcomOrdersStream"

# Windows scripts
.\scripts\run_orders_gen.ps1 -Rate 5
.\scripts\run_orders_stream.ps1
```

### Data Sources

| Source | API Endpoint | Products | Features |
|--------|-------------|----------|----------|
| **DummyJSON** | `https://dummyjson.com/products` | 100+ | Full product catalog with images |
| **FakeStore** | `https://fakestoreapi.com/products` | 20+ | E-commerce focused data |

### Folder Layout

```
data/ecommerce/
├── raw/                          # Bronze layer
│   └── source=dummyjson/
│       └── run_date=2024-01-01/
│           └── part-00000.ndjson
├── silver/                       # Silver layer
│   └── products/
│       └── ingest_date=2024-01-01/
│           ├── category=electronics/
│           └── category=clothing/
├── orders_incoming/              # Streaming input
│   ├── orders_00000.ndjson
│   └── orders_00001.ndjson
└── metrics/                      # Streaming output
    ├── gmv/
    ├── orders/
    └── top_products/
```

### Sample Analytics Queries

After running the batch ETL, you can analyze the data:

```sql
-- Top brands by product count
SELECT brand, COUNT(*) as product_count
FROM products
GROUP BY brand
ORDER BY product_count DESC
LIMIT 10;

-- Price distribution
SELECT
  CASE
    WHEN price < 10 THEN '< $10'
    WHEN price < 50 THEN '$10-$49'
    WHEN price < 100 THEN '$50-$99'
    ELSE '$100+'
  END as price_range,
  COUNT(*) as products
FROM products
GROUP BY price_range
ORDER BY price_range;

-- Average rating by category
SELECT category, AVG(rating) as avg_rating, COUNT(*) as products
FROM products
GROUP BY category
ORDER BY avg_rating DESC;
```

### Configuration

#### Environment Variables

```bash
# API Configuration
export ECOM_API_BASE="https://dummyjson.com"
export ECOM_API_KEY="your-api-key"  # If required
export RPS="2"                      # Max requests per second
export TIMEOUT="30"                 # Request timeout in seconds
```

#### CLI Arguments

**EcomIngestor:**
- `--source dummyjson|fakestore`: Data source
- `--out PATH`: Output directory
- `--page-size INT`: Items per page (default: 100)
- `--max-pages INT`: Maximum pages to fetch (default: 50)
- `--rps INT`: Max requests per second (default: 2)
- `--run-date YYYY-MM-DD`: Run date (default: today UTC)

**OrdersGenerator:**
- `--rate INT`: Events per second (default: 5)
- `--burst INT`: Burst factor (default: 0)
- `--out PATH`: Output directory
- `--duration INT`: Seconds to run (default: infinite)

### Rate Limits & Best Practices

- **DummyJSON**: No explicit rate limits, but respect 2 RPS default
- **FakeStore**: No rate limits mentioned, but use conservative settings
- **Exponential Backoff**: Automatic retry with increasing delays
- **Circuit Breaker**: Consider implementing for production use
- **Caching**: Add local caching for development/testing

### Streaming KPIs

The orders streaming pipeline computes:

1. **GMV per Minute**: Total order value in 1-minute windows
2. **Orders per Minute**: Order count in 1-minute windows
3. **Top Products**: Most ordered products in 5-minute sliding windows

### Troubleshooting

#### Common Issues

**Connection Timeout:**
```bash
# Increase timeout
export TIMEOUT="60"
sbt "runMain com.example.ecommerce.ingest.EcomIngestor --source dummyjson"
```

**Rate Limit Exceeded:**
```bash
# Reduce RPS
sbt "runMain com.example.ecommerce.ingest.EcomIngestor --source dummyjson --rps 1"
```

**Out of Memory (Spark):**
```bash
# Increase memory
sbt -Dspark.driver.memory=4g -Dspark.executor.memory=4g "runMain com.example.ecommerce.spark.EcomBatchJob"
```

#### API Errors

- **429 Too Many Requests**: Reduce RPS or implement backoff
- **500 Server Error**: Usually temporary, retry logic handles this
- **Invalid JSON**: Parser is robust, but check API changes

### Development

#### Adding New Data Sources

1. Extend `DataSource` trait in `EcomIngestor.scala`
2. Update `DataSource.fromString()` method
3. Add to README data sources table

#### Testing

```bash
# Unit tests
sbt "testOnly com.example.ecommerce.*"

# Integration test with sample data
python3 scripts/ecom_download.py --source dummyjson --max-pages 1
sbt "runMain com.example.ecommerce.spark.EcomBatchJob"
```

---

**Happy E-Commerce Analytics! 🛒✨**