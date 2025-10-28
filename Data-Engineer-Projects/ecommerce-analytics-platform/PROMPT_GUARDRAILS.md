# Prompt Guardrails

This project follows the constraints defined in Prompt 0. The summary below captures the non-negotiable rules for ongoing development.

## Technology & Tooling
- Python 3.11 with Poetry for dependency management (lock file required).
- Backend: FastAPI + Pydantic v2, SQLAlchemy/SQLModel, PostgreSQL, Alembic.
- Data & ML: pandas, SQLAlchemy, ALS via `implicit`, numpy/scipy.
- Quality: pytest (+ coverage), ruff, black, mypy (strict), pip-audit, pre-commit.
- Local services only: Postgres, Redis, MinIO; Kafka/Redpanda reserved for later phases.

## Configuration & Secrets
- Centralize settings via `pydantic_settings.BaseSettings`; load from environment and `.env`.
- Ship a full `.env.example`; ensure `.env` is ignored.
- Never hardcode secrets, credentials, or machine-specific paths.

## Architecture
- Monorepo structure: `backend/`, `etl/`, `ml/`, `infra/`, `docs/`.
- Respect service layering: router → service → repository → database.
- Warehouse schema: star model with SCD-1 dimensions and audited ETL processes.
- Logging: structlog JSON with correlation IDs; redact/mask PII.

## API & Security
- FastAPI endpoints with strict validation, pagination, filtering, sorting.
- `/health` and `/metrics` endpoints required.
- RBAC via JWT HS256, claims validation, and field masking for non-admin roles.
- Use parameterized queries only; avoid unsafe eval/exec/pickle usage.

## Performance & Observability
- Redis cache with TTL and invalidation post-ETL runs.
- Prometheus metrics for API latency, ETL timings, cache hit rates.
- Timers around expensive operations (ETL, recommendations).

## Testing & CI
- Minimum 80% coverage (enforced in later prompts).
- Full pipeline tests: seed → ETL → API → recommendations.
- `make ci` runs lint, format check, type check, pytest, and pip-audit.

## Operational Requirements
- Docker Compose spins up Postgres, Redis, MinIO, and API with health checks.
- Alembic migrations must run cleanly: `alembic upgrade head`.
- Idempotent, reproducible processes; fail fast with actionable errors.
- No cloud services; everything runs locally until subsequent prompts introduce streaming.
