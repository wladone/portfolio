# Fashion Retail ETL Pipeline v2

A production-style Apache Airflow ETL project for an international clothing retailer with local demo using MinIO + DuckDB and optional S3 + Snowflake deployment.

## Features

- **Data Ingestion**: Daily ingestion of 5,000 products and 50,000 transactions
- **Data Quality**: Comprehensive validation with configurable thresholds
- **ML Recommendations**: Top product recommendations per region based on sales
- **Dashboard**: Streamlit dashboard for KPIs and insights
- **Multi-Platform**: Local demo (MinIO + DuckDB) and production (S3 + Snowflake)
- **Docker Ready**: Complete containerized setup

## Architecture

```
Raw Data (MinIO/S3) → Staging (DuckDB/Snowflake) → Transformed (Dimensions/Facts) → Recommendations → Dashboard
```

## Quick Start (Local Demo)

### Prerequisites
- Docker and Docker Compose
- Python 3.9+ (optional, for local development)

### 1. Clone and Setup
```bash
git clone <repository-url>
cd fashion-retail-pipeline
```

### 2. Start Services
```bash
docker compose -f compose/docker-compose.local.yml up -d
```

### 3. Generate Sample Data
```bash
make demo
# This generates data for today and uploads to MinIO
```

### 4. Run Pipeline
1. Open Airflow UI: http://localhost:8080 (admin/admin)
2. Unpause DAG: `fashion_retail_etl_pipeline_v2`
3. Trigger a manual run or wait for scheduled run (02:00 UTC)

### 5. View Dashboard
```bash
make dashboard
# Opens Streamlit at http://localhost:8501
```

## Project Structure

```
fashion-retail-pipeline/
├─ dags/
│  ├─ fashion_retail_pipeline_v2.py    # Main DAG
│  └─ sql/                            # SQL scripts
├─ scripts/
│  ├─ generate_sample_data.py         # Data generator
│  ├─ load_to_duckdb.py              # DuckDB loaders
│  ├─ validate_data_quality.py       # DQ validation
│  └─ s3_minio_tools.py              # S3 utilities
├─ showcase/
│  └─ app.py                         # Streamlit dashboard
├─ config/
│  ├─ .env.example                   # Environment config
│  ├─ airflow_connections.json       # Airflow connections
│  └─ airflow_variables.json         # Airflow variables
├─ compose/
│  └─ docker-compose.local.yml       # Local stack
├─ tests/
│  ├─ test_dag_loads.py              # DAG tests
│  └─ test_sql_duckdb.py             # SQL tests
└─ docker/
   ├─ requirements.txt                # Python deps
   └─ Dockerfile.data_validation      # DQ container
```

## Configuration

### Local Demo (.env)
```bash
BACKEND=duckdb
DUCKDB_PATH=/data/warehouse.duckdb
DATA_BASE_PATH=/data
S3_ENDPOINT=http://minio:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
GZIP_FILES=false
```

### Production (.env)
```bash
BACKEND=snowflake
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_WAREHOUSE=FASHION_RETAIL_WH
SNOWFLAKE_DATABASE=FASHION_RETAIL_DB
SNOWFLAKE_SCHEMA=RETAIL_SCHEMA
AWS_S3_ACCESS_KEY_ID=your_key
AWS_S3_SECRET_ACCESS_KEY=your_secret
AWS_S3_REGION=us-east-1
GZIP_FILES=true
```

## Data Model

### Dimensions
- **products_dim**: Product catalog with pricing and metadata

### Facts
- **sales_fact**: Transaction-level sales data

### Aggregates
- **product_recommendations**: Top products per region (last 30 days)

## Pipeline Tasks

1. **ingest.wait_products/transactions**: Wait for files in MinIO/S3
2. **ensure_objects**: Create database schema
3. **ingest.load_*_stg**: Load raw data to staging
4. **dq_branch**: Basic data quality check
5. **transform.transform_products/sales**: Clean and transform data
6. **dq_validate**: Advanced data quality validation
7. **generate_recommendations**: Compute top products
8. **notify_success/failure**: Email notifications

## Data Quality Checks

- Non-positive prices/costs in products
- Non-positive quantities/prices/amounts in sales
- Orphan sales (product_id not in products_dim)

## Development

### Run Tests
```bash
make test
```

### Run Linting
```bash
flake8 scripts/ tests/ dags/ showcase/
```

### Generate Data Manually
```bash
python scripts/generate_sample_data.py --date 20231001 --gzip
```

## Deployment

### Local Demo
```bash
make init
docker compose -f compose/docker-compose.local.yml up -d
make demo
make run
make dashboard
```

### Production (Snowflake + S3)
1. Update `.env` with real credentials
2. Run Snowflake schema creation
3. Deploy to production environment
4. Update connection configurations

## Monitoring

- **Airflow UI**: Pipeline status and logs
- **MinIO Console**: Data lake browser (http://localhost:9001)
- **Streamlit Dashboard**: Business metrics
- **Logs**: Container logs via `docker compose logs`

## Performance

- **Parallel Processing**: Products and transactions loaded simultaneously
- **Compression**: Optional GZIP for large files
- **Idempotent Operations**: Safe reruns
- **Retries**: Exponential backoff on failures
- **SLAs**: Configurable task timeouts

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure CI passes
5. Submit a pull request

## License

MIT License - see LICENSE file for details.