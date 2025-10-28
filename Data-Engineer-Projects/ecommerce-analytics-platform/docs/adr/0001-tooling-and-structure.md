# ADR 0001: Tooling and Repository Structure

## Status
Accepted

## Context
We are building a local-first e-commerce analytics platform that spans ETL, API, ML, and infrastructure concerns. The solution must remain reproducible, type-safe, and secure, while supporting iterative expansion (batch → incremental → streaming).

## Decision
- Use **Python 3.11** with **Poetry** for dependency management and locking.
- Structure the monorepo with bounded directories: `backend/`, `etl/`, `ml/`, `infra/`, `docs/`.
- Adopt **FastAPI + Pydantic v2** for service contracts, **SQLAlchemy/SQLModel** for persistence, and **Alembic** for migrations.
- Standardize quality gates with **ruff**, **black**, **mypy (strict)**, **pytest**, and **pip-audit** via `make ci`.
- Provide local infrastructure through **Docker Compose** (Postgres, Redis, MinIO, API).
- Enforce configuration via `pydantic_settings.BaseSettings` and `.env` files.

## Consequences
- Consistent local environments via Poetry lock files and Make targets.
- Clear ownership by domain, enabling parallel development across API, ETL, and ML.
- Strict typing and linting increase upfront effort but reduce regressions later.
- Docker Compose adds setup overhead but guarantees parity between developers.
- Documentation (README, ADRs, diagrams) keeps architectural intent discoverable and auditable.
