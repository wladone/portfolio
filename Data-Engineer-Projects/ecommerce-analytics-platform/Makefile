SEED_START_DATE ?= 2023-01-01
SEED_END_DATE ?= 2025-12-31
FACTORS ?= 64
REG ?= 0.02
ITER ?= 15
ALPHA ?= 40.0
SEED ?= 1337
LOOKBACK_DAYS ?= 365
MIN_USER ?= 3
MIN_ITEM ?= 5
E2E_API_BASE_URL ?= http://localhost:8000


.PHONY: setup lint typecheck test ci run-api compose-up compose-down obs-up obs-down docker-build-backend docker-build-etl docker-build-ml compose-logs etl-run train-docker seed-generate seed-dim-date seed-load seed train train-all e2e e2e-up create-admin create-analyst create-app inc-customers inc-products inc-orders inc-all cache-bust-sales cache-bust-recs cache-selective-example stream-orders-tail stream-orders-watch kafka-topics docs-serve docs-build smoke diag backup restore postman qa qa-junit qa-ci

POETRY := poetry

setup:
	$(POETRY) install
	$(POETRY) run pre-commit install

lint:
	$(POETRY) run ruff .
	$(POETRY) run black --check .

typecheck:
	$(POETRY) run mypy .

test:
	$(POETRY) run pytest --cov

ci: lint typecheck test
	$(POETRY) export -f requirements.txt --without-hashes -o .tmp-requirements.txt
	$(POETRY) run pip-audit -r .tmp-requirements.txt
	$(POETRY) run python -c "import pathlib; pathlib.Path('.tmp-requirements.txt').unlink(missing_ok=True)"

run-api:
	$(POETRY) run uvicorn backend.app.main:app --host 0.0.0.0 --port 8000 --reload

compose-up:
	docker compose -f infra/docker-compose.yml --profile dev up -d --build

compose-down:
	docker compose -f infra/docker-compose.yml down -v

obs-up:
	docker compose -f infra/docker-compose.yml --profile obs up -d

obs-down:
	docker compose -f infra/docker-compose.yml --profile obs down -v

docker-build-backend:
	docker build -t ecom/backend:dev -f backend/Dockerfile .

docker-build-etl:
	docker build -t ecom/etl:dev -f etl/Dockerfile .

docker-build-ml:
	docker build -t ecom/ml:dev -f ml/Dockerfile .

compose-logs:
	docker compose -f infra/docker-compose.yml logs -f api

etl-run:
	docker compose -f infra/docker-compose.yml run --rm etl orders --source infra/seed/data/orders_*.json --ensure-dim-date

train-docker:
	docker compose -f infra/docker-compose.yml run --rm -e ALS_LOOKBACK_DAYS=365 ml

seed-generate:
	$(POETRY) run python -m infra.seed.generate_seed --config infra/seed/seed_config.yaml --out infra/seed/data

train:
	$(POETRY) run python -m ml.als_train --lookback-days 365

train-all:
	$(POETRY) run python -m ml.als_train --factors $(FACTORS) --regularization $(REG) --iterations $(ITER) --alpha $(ALPHA) --seed $(SEED) --lookback-days $(LOOKBACK_DAYS) --min-purchases-per-user $(MIN_USER) --min-purchases-per-item $(MIN_ITEM)
create-admin:
	$(POETRY) run python -m backend.app.security.create_user --username admin --password admin --role admin

create-analyst:
	$(POETRY) run python -m backend.app.security.create_user --username analyst --password analyst --role analyst

create-app:
	$(POETRY) run python -m backend.app.security.create_user --username app --password app --role app

seed-dim-date:
	$(POETRY) run python -m infra.seed.seed_dim_date --start $(SEED_START_DATE) --end $(SEED_END_DATE)

seed-load:
	$(POETRY) run python -m etl.load products --source infra/seed/data/products.csv
	$(POETRY) run python -m etl.load customers --source infra/seed/data/customers_*.json
	$(POETRY) run python -m etl.load orders --source infra/seed/data/orders_*.json --ensure-dim-date

seed:
	$(MAKE) seed-dim-date
	$(MAKE) seed-generate
	$(MAKE) seed-load

inc-customers:
	$(POETRY) run python -m etl.incremental customers --source infra/seed/data/customers_*.json

inc-products:
	$(POETRY) run python -m etl.incremental products --source infra/seed/data/products.csv

inc-orders:
	$(POETRY) run python -m etl.incremental orders --source infra/seed/data/orders_*.json --ensure-dim-date

inc-all:
	$(POETRY) run python -m etl.incremental all --source infra/seed/data/ --ensure-dim-date

cache-bust-sales:
	$(POETRY) run python -c "from backend.app.core.cache import bump_namespace; import asyncio; asyncio.run(bump_namespace('ns:sales'))"

cache-bust-recs:
	$(POETRY) run python -c "from backend.app.core.cache import bump_namespace; import asyncio; asyncio.run(bump_namespace('ns:recs'))"

cache-selective-example:
	curl -X POST http://localhost:8000/api/v1/cache/_purge -H "Content-Type: application/json" -d '{"target": "sales", "strategy": "selective", "payload": {"channel": "online"}}'

stream-orders-tail:
	$(POETRY) run python -m backend.app.streaming.file_orders_worker tail $(SOURCE_FILE)

stream-orders-watch:
	$(POETRY) run python -m backend.app.streaming.file_orders_worker watch $(SOURCE_FILE)

kafka-topics:
	@docker compose -f infra/docker-compose.yml exec redpanda rpk topic list || true
	@docker compose -f infra/docker-compose.yml exec redpanda rpk topic create orders --replicas 1 --partitions 1 || true

e2e: seed train
	E2E_API_BASE_URL=$(E2E_API_BASE_URL) $(POETRY) run pytest -k test_e2e_pipeline -q

e2e-up:
	$(MAKE) compose-up
	$(POETRY) run alembic upgrade head
	$(MAKE) e2e
# Add QA targets after existing content

qa:
	$(POETRY) run python -m qa.runner -o markdown

qa-junit:
	$(POETRY) run python -m qa.runner -o junit

qa-ci:
	$(POETRY) run python -m qa.runner -o junit --fail-on-warn

ci: lint typecheck test qa-ci
