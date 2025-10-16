.PHONY: setup lint format test run-local run-flex run-direct publish-samples cleanup-plan cleanup-apply guardrails freeze update-readme set-bq-ttls set-gcs-lifecycle dlq-consumer deploy-monitoring validate-env setup-alerts e2e-tests load-test setup-secrets enable-compliance deploy-blue-green chaos-test cost-monitoring observability-setup
VENV=.venv
-include .env
PROJECT_ID ?= your-project-id
REGION ?= europe-central2
DATAFLOW_BUCKET ?= your-dataflow-bucket
BIGTABLE_INSTANCE ?= your-bigtable-instance
BIGTABLE_TABLE ?= product_stats
COMPOSER_ENV ?= your-composer-env
TEMPLATE_PATH ?= gs://$(DATAFLOW_BUCKET)/templates/streaming_pipeline_flex_template.json
PY=$(VENV)/bin/python
PIP=$(VENV)/bin/pip
setup:
	@if [ ! -d $(VENV) ]; then python3.10 -m venv $(VENV); fi
	$(PY) -m pip install --upgrade pip
	$(PIP) install -r requirements.txt -r requirements-dev.txt
lint:
	$(VENV)/bin/flake8 .
	$(VENV)/bin/black --check .
	$(VENV)/bin/isort --check-only .
format:
	$(VENV)/bin/black .
	$(VENV)/bin/isort .
test:
	$(VENV)/bin/pytest -q
run-local:
	python run.py local
run-flex:
	python run.py flex
run-direct:
	python run.py direct
publish-samples:
	bash scripts/publish_samples.sh
cleanup-plan:
	bash scripts/cleanup_plan.sh
cleanup-apply:
	CONFIRM=DELETE DRY_RUN=0 bash scripts/cleanup_apply.sh
guardrails:
	bash scripts/cost_guardrails.sh
set-bq-ttls:
	bash scripts/set_bq_ttls.sh
set-gcs-lifecycle:
	bash scripts/set_gcs_lifecycle.sh
dlq-consumer:
	python tools/dlq_consumer.py
deploy-monitoring:
	bash scripts/deploy_monitoring.sh
freeze:
	$(VENV)/bin/pip freeze > requirements.lock.txt
update-readme:
	python scripts/update_readme_sections.py --path README.md --verbose

# Production Readiness Commands
validate-env:
	python scripts/validate_environment.py

setup-alerts:
	python scripts/setup_monitoring_channels.py

e2e-tests:
	python tests/test_streaming_pipeline_e2e.py

load-test:
	python scripts/load_test.py --help

setup-secrets:
	python scripts/secret_management.py --setup --project $(PROJECT_ID)

enable-compliance:
	python scripts/compliance_audit.py --project $(PROJECT_ID) --enable-gdpr

deploy-blue-green:
	python scripts/blue_green_deploy.py --project $(PROJECT_ID) --new-version $$(date +%Y%m%d%H%M)

chaos-test:
	python scripts/chaos_engineering.py --project $(PROJECT_ID) --list-experiments

cost-monitoring:
	python scripts/cost_monitoring.py --project $(PROJECT_ID) --budget 200.00

observability-setup:
	python scripts/setup_observability.py --project $(PROJECT_ID)
validate-env:
	python scripts/validate_environment.py
setup-alerts:
	python scripts/setup_monitoring_channels.py
e2e-tests:
	python tests/test_streaming_pipeline_e2e.py
load-test:
	@echo "🚀 Load testing functionality coming soon..."
	@echo "This will test pipeline performance under high load"
