# Helper script to run the API locally while using Dockerized infrastructure
# Usage: from project root in PowerShell: .\run_local.ps1

# Ensure Docker services are running first:
# docker compose -f infra/docker-compose.yml --profile dev up -d

# Set environment variables to talk to services exposed on localhost by Docker Compose
$env:APP_ENV = "dev"
$env:APP_LOG_LEVEL = "INFO"

# Database settings (connect to Postgres exposed on localhost)
$env:DATABASE_HOST = "localhost"
$env:DATABASE_PORT = "5432"
$env:DATABASE_USER = "app"
$env:DATABASE_PASSWORD = "app_password"
$env:DATABASE_NAME = "ecom"
$env:DATABASE_URL = "postgresql+psycopg://app:app_password@localhost:5432/ecom"

# Redis settings
$env:REDIS_URL = "redis://localhost:6379/0"

# MinIO settings
$env:MINIO_ENDPOINT = "http://localhost:9000"
$env:MINIO_ACCESS_KEY = "minioadmin"
$env:MINIO_SECRET_KEY = "minioadmin"

# Streaming/Kafka settings (important - must use localhost when running API locally)
$env:STREAMING_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:STREAMING_KAFKA_TOPIC_ORDERS = "orders"
$env:STREAMING_KAFKA_TOPIC_CUSTOMERS = "customers"
$env:STREAMING_KAFKA_TOPIC_PRODUCTS = "products"
$env:STREAMING_KAFKA_GROUP_ID = "streaming_group"
$env:STREAMING_BATCH_SIZE = "100"
$env:STREAMING_POLL_TIMEOUT_MS = "1000"

Write-Host "Environment variables set for local development against Docker services"
Write-Host "Starting uvicorn..."

# Run uvicorn through poetry with auto-reload
poetry run uvicorn backend.app.main:app --host 0.0.0.0 --port 8000 --reload
