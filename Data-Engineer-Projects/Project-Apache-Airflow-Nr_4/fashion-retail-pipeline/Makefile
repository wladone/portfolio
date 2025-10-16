.PHONY: init demo run dashboard test clean zip snowflake-init

# Default target
help:
	@echo "Available targets:"
	@echo "  init          - Create virtual environment and install dependencies"
	@echo "  demo          - Generate sample data and upload to MinIO"
	@echo "  run           - Unpause DAG and trigger today's run"
	@echo "  dashboard     - Open Streamlit dashboard"
	@echo "  test          - Run tests"
	@echo "  clean         - Clean up generated files"
	@echo "  zip           - Package project as zip file"
	@echo "  snowflake-init - Initialize Snowflake schema (for production)"

# Initialize environment
init:
	python -m venv venv
	./venv/bin/pip install --upgrade pip
	./venv/bin/pip install -r requirements.txt

# Generate demo data
demo:
	python scripts/generate_sample_data.py --date $$(date +%Y%m%d) --output_dir data
	python scripts/s3_minio_tools.py $$(date +%Y%m%d)

# Run the pipeline
run:
	@echo "Make sure Docker Compose is running with: docker compose -f compose/docker-compose.local.yml up -d"
	@echo "Then visit Airflow UI at http://localhost:8080"
	@echo "Unpause the DAG 'fashion_retail_etl_pipeline_v2' and trigger a run"

# Open dashboard
dashboard:
	@echo "Dashboard available at http://localhost:8501"
	@echo "Make sure Streamlit service is running in Docker Compose"

# Run tests
test:
	pytest tests/ -v

# Clean up
clean:
	rm -rf data/raw/
	rm -f data/warehouse.duckdb
	rm -rf __pycache__/
	rm -rf .pytest_cache/

# Package as zip
zip:
	zip -r fashion_retail_pipeline_v2.zip . -x "*.git*" "*__pycache__*" "*.pytest_cache*" "*data/*" "*venv/*"

# Snowflake initialization (placeholder)
snowflake-init:
	@echo "Initialize Snowflake warehouse, database, and schema"
	@echo "Run the create_schema_snowflake.sql manually in Snowflake"
	@echo "Update .env with real Snowflake credentials"