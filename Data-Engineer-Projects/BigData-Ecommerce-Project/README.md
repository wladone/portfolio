# Real-time E-commerce Streaming Pipeline on Google Cloud

This repository delivers an end-to-end, production-ready Apache Beam streaming pipeline for real-time e-commerce analytics on Google Cloud. It ingests click, transaction, and stock events from Pub/Sub, validates and aggregates them, writes insights to BigQuery, keeps hot counters in Cloud Bigtable, and exposes orchestration hooks for Cloud Composer.

## Architecture at a Glance
- **Region-first design:** All resources are expected in a single region. Defaults use `europe-central2` (Warsaw) to keep data sovereignty in the EU; update `.env` if you deploy elsewhere.
- **Ingestion:** Three Pub/Sub topics (`clicks`, `transactions`, `stock`) plus a dead-letter topic (`dead-letter`) and subscription for invalid payloads.
- **Processing:** Apache Beam (Python 3.10) running on Dataflow streaming mode (`save_main_session=True`).
- **Validation & Observability:** JSON schema checks, Beam Metrics counters per stream, structured logging for emitted aggregates, and a DLQ for diagnostics.
- **Aggregations:**
  - Product views: 60s fixed window, per-product counts.
  - Sales: 300s fixed window, per `(product_id, store_id)` totals plus roll-ups.
  - Stock: 300s fixed window, per `(product_id, warehouse_id)` deltas plus roll-ups.
- **Outputs:**
  - BigQuery tables (`product_views_summary`, `sales_summary`, `inventory_summary`) in dataset `ecommerce`. Tables auto-create with `DAY` partitioning on `window_start` and clustering on `product_id`.
  - Cloud Bigtable table `product_stats` (column family `stats`, column `view_count`) for the latest per-product views.
- **Orchestration:** Composer DAGs for direct Dataflow launches and Flex Templates. CLI invocations in `run.py` use argument arrays to prevent command injection.

## Repository Layout
```
beam/                      # Apache Beam pipeline + Flex container assets
composer/                  # Airflow DAGs for direct + Flex runs
scripts/                   # Bootstrap, guardrails, cleanup, lifecycle helpers
sql/                       # BigQuery DDL (including partitioned table templates)
tests/                     # Pytest coverage for orchestration + pipeline utilities
config.py                  # Centralised configuration with environment overrides
run.py                     # Local/Flex/Composer launch helper
README.md                  # You are here
```

## Configuration & Defaults
`config.py` centralises the dataset, table names, window sizes, Pub/Sub topics, and Bigtable settings. Every constant can be overridden with an environment variable (e.g. `DATASET_NAME`, `TOPIC_DLQ`, `BIGTABLE_TABLE`). The pipeline and orchestration scripts consume these values to keep dev/staging/prod environments consistent.

Key defaults:
- GCP identifiers: `PROJECT_ID=demo-project`, `DATAFLOW_BUCKET=demo-dataflow-bucket`, `COMPOSER_ENV=demo-composer-env`. Override via environment variables when deploying to real projects.
- Dataset: `ecommerce` (must exist before running the pipeline).
- Tables: `product_views_summary`, `sales_summary`, `inventory_summary`.
- Topics: `clicks`, `transactions`, `stock`, `dead-letter` (+ subscription `dead-letter-sub`).
- Bigtable: instance `local-bigtable`, table `product_stats`, column family `stats`, column `view_count`.
- Windows: 60s (views) and 300s (sales & stock).

## Prerequisites
Before deploying:
- Enable APIs: Pub/Sub, Dataflow, BigQuery, Bigtable, Cloud Composer, Cloud Build, Artifact Registry.
- Install CLIs: `gcloud`, `bq`, `cbt`, `gsutil`, Docker (or Cloud Build), plus `make` on macOS/Linux.
- Create a **regional BigQuery dataset** (same region as Dataflow) using `bq --location=REGION mk --dataset PROJECT_ID:ecommerce`.
- Provision Pub/Sub topics, a Bigtable instance/table/family, and a Composer environment in the same region.
- Populate `.env` (copy from `.env.sample`) with `PROJECT_ID`, `REGION`, `DATAFLOW_BUCKET`, `BIGTABLE_*`, and Composer environment name.

## Local Development (Python 3.10 virtualenv)
> `.venv/` is for local development only. Dataflow/Composer tasks install dependencies from `beam/requirements.txt`.

### macOS/Linux
```bash
chmod +x scripts/create_venv.sh && ./scripts/create_venv.sh
source .venv/bin/activate
make lint && make test
make run-local   # DirectRunner with local Pub/Sub topics
```

### Windows (PowerShell)
```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
./scripts/activate_venv.ps1
.\.venv\Scripts\Activate.ps1
pytest -q
```

### Pre-commit Hooks
```bash
pre-commit install
pre-commit run --all-files
```

## Useful Make Targets
- `make run-local` / `make run-flex` / `make run-direct` – launch via DirectRunner, Flex Template, or Composer DAG.
- `make publish-samples` – push sample events to the three topics for smoke testing.
- `make cleanup-plan` / `make cleanup-apply` – safe plan + guarded apply for draining jobs, pruning buckets, and optional resource deletes.
- `make guardrails` – nightly-friendly script for draining `streaming*` jobs, pruning buckets, updating Pub/Sub retention, and default TTLs.
- `make set-bq-ttls` – set dataset/table TTLs (3 days) to control BigQuery storage cost.
- `make set-gcs-lifecycle` – apply a lifecycle policy deleting `tmp/` and `staging/` objects older than 3 days.
- `make dlq-consumer` – inspect dead-letter messages using `tools/dlq_consumer.py` (read-only by default).
- `make deploy-monitoring` - apply Cloud Monitoring dashboard & alert policy scaffolding.
- `make freeze` – export dependency lockfile.

## Local Emulation (Docker Compose)
- Start the Pub/Sub and Bigtable emulators locally with `docker compose up -d`.
- Point application components to `localhost:8085` (Pub/Sub) and `localhost:8086` (Bigtable) via environment overrides.
- Stop emulators with `docker compose down`.

### Local dev mode (--local)
The pipeline now supports a `--local` flag to run without requiring external sinks. This is useful for running transforms and unit/integration tests locally without BigQuery/Bigtable.

Example (DirectRunner in local mode):
```bash
python -m beam.streaming_pipeline --local
```

Performance tests
- Performance tests are gated behind an env var to avoid running in CI. Run them with:
```bash
RUN_PERF_TESTS=1 pytest -m performance
```

## Running the Pipeline
### Local (DirectRunner)
```bash
python run.py local
```
This passes Beam pipeline options (`--project`, `--region`) and fully-qualified Pub/Sub topics using the project ID from the environment. Commands are executed with argument arrays (no `shell=True`) to avoid injection risks.

### Dataflow Flex Template
```bash
python run.py flex
```
Creates a job name with `datetime.now(timezone.utc)` and launches `gcloud dataflow flex-template run` using argument arrays. Parameters (topics, dataset, tables, Bigtable config) are bundled, and `streaming=true` is enforced.

### Composer-triggered
```bash
python run.py direct
```
Issues `gcloud composer environments run ... dags trigger` with project/region awareness. Requires the DAGs under `composer/dags/` to be deployed.

## BigQuery Tables & DDL
Tables are auto-created with partitioning and clustering when the pipeline first writes. For explicit provisioning (preferred in prod), use:
```bash
bq --location=REGION mk --dataset PROJECT_ID:ecommerce
bq query --use_legacy_sql=false < sql/create_tables_partitioned.sql
```
The DDL aligns with the pipeline schema (`window_start` partition, `product_id` cluster) to keep query and storage costs predictable.

## Dead-letter Handling
Invalid events are routed to the `dead-letter` topic and counted via Beam Metrics (`*_invalid`). A helper subscription (`dead-letter-sub`) is assumed. Inspect recent failures with:
```bash
PROJECT_ID=your-project make dlq-consumer
```
Pass `--ack` to delete after review. The script defaults to read-only mode to prevent accidental data loss.

## Bigtable Pre-flight
The pipeline checks that the target table and column family are configured before writing. `ToBigtableRow` batches rows into `DirectRow` mutations; Dataflow handles batching to avoid single-row throttling. Ensure the table `product_stats` and family `stats` exist via `cbt` commands (see Quickstart commands below).

## Smoke Validation
- Composer DAG `streaming_smoke_dag` (hourly) runs a BigQuery row-level check and ensures the Bigtable table/family exist; adjust `SMOKE_WINDOW_MINUTES` env var for stricter windows.
- Use this DAG as a guardrail alongside integration tests to verify end-to-end health after deployments.

## Observability

### Cloud Monitoring
- Data quality metrics are exported via Beam counters/distributions (e.g. `views_rows_emitted`, `views_per_window`).
- `scripts/deploy_monitoring.sh` applies the sample dashboard (`monitoring/dashboard_ecommerce_pipeline.json`) and alert policies (`monitoring/alert_dead_letter_policy.json`, `monitoring/alert_low_throughput_policy.json`).
- Update thresholds or notification channels before running in production; the script uses `gcloud monitoring dashboards` and `gcloud alpha monitoring policies`.
- Metrics rely on Beam counters (namespace `ecommerce_pipeline_*`) surfaced by Dataflow; ensure Dataflow job metrics export is enabled in your project.

- `scripts/deploy_monitoring.sh` applies the sample dashboard (`monitoring/dashboard_ecommerce_pipeline.json`) and alert policy (`monitoring/alert_dead_letter_policy.json`).
- Update thresholds or notification channels before running in production; the script uses `gcloud monitoring dashboards` and `gcloud alpha monitoring policies`.
- Metrics rely on Beam counters (namespace `ecommerce_pipeline_*`) surfaced by Dataflow; ensure Dataflow job metrics export is enabled in your project.

- **Logging:** Every emitted aggregate logs product ID and window start via `log_event_processing` for fast troubleshooting.
- **Metrics:** Beam counters for processed (`*_processed`) and invalid events (`*_invalid`) plus BigQuery sink counters (`views_rows_emitted`, etc.) surface in Dataflow > Metrics.
- **Dead-letter metrics:** Monitor `config.METRICS_NAMESPACE` (`ecommerce_pipeline` by default) in Cloud Monitoring for spikes.

## Cost Guardrails & Cleanup
- `scripts/set_bq_ttls.sh` and `scripts/set_gcs_lifecycle.sh` enforce 3-day retention on BigQuery tables and GCS staging/tmp folders.
- `scripts/cost_guardrails.sh` (also behind `make guardrails`) drains streaming jobs with a configurable prefix, prunes buckets, updates dataset TTLs, and sets Pub/Sub retention to 24h.
- `scripts/cleanup_plan.sh` / `scripts/cleanup_apply.sh` are idempotent, default `DRY_RUN=1`, and require `CONFIRM=DELETE` for destructive operations.

## Quickstart Command Reference
Create Pub/Sub topics:
```bash
gcloud pubsub topics create clicks
gcloud pubsub topics create transactions
gcloud pubsub topics create stock
gcloud pubsub topics create dead-letter
```

Optional DLQ subscription:
```bash
gcloud pubsub subscriptions create dead-letter-sub \
  --topic=dead-letter --ack-deadline=60
```

Bigtable setup:
```bash
cbt -project PROJECT_ID -instance BIGTABLE_INSTANCE createtable product_stats
cbt -project PROJECT_ID -instance BIGTABLE_INSTANCE createfamily product_stats stats
```

BigQuery dataset & tables:
```bash
bq --location=REGION mk --dataset PROJECT_ID:ecommerce
bq query --use_legacy_sql=false < sql/create_tables_partitioned.sql
```

Flex Template build & launch (simplified):
```bash
# Build image & template (see beam/Dockerfile + metadata.json for details)
# ...
python run.py flex
```

## Validation & Dead-letter Monitoring
`ParseAndValidateEvent` enforces required fields and numeric casts. Invalid payloads increment Beam metrics and route to the DLQ. Use `make dlq-consumer` to review payloads before reprocessing or deleting.

## Security Notes
- All subprocess invocations in `run.py` use argument arrays with `subprocess.run(..., check=True)` to eliminate command-injection risk.
- Runtime configuration is centralised in `config.py`, allowing environment-specific overrides instead of hardcoding IDs in source.
- Cleanup scripts guard destructive actions behind `DRY_RUN` and `CONFIRM=DELETE` gates.

## Testing
- Run `pytest -q` after activating the virtualenv. Integration coverage leverages Apache Beam's `TestPipeline` to exercise transforms end-to-end.
- Enable performance tests with `RUN_PERF_TESTS=1 pytest -m performance` (disabled by default).
- CI (`.github/workflows/ci.yml`) provisions `.venv/`, installs requirements, runs lint/format/test, mirroring local steps.

## Troubleshooting
- **Dead-letter spikes:** Check DLQ with `make dlq-consumer`. Most issues are missing fields or invalid numerics.
- **Bigtable errors:** Ensure the service account has `bigtable.user` (or finer) and that table/family exist.
- **Dataset missing:** Create `ecommerce` before running or pass `DATASET_NAME` env override.
- **Region mismatches:** All resources must be in the same region (`REGION` env). Composer/Dataflow cross-region writes are unsupported.
- **Cost surprises:** Apply `make set-bq-ttls` and `make set-gcs-lifecycle`, and schedule `make guardrails` nightly.

Happy streaming!
