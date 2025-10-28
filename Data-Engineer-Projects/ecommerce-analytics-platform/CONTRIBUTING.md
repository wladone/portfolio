# Contributing Guidelines

Thank you for supporting the e-commerce analytics platform! This document explains how to work effectively within the repository while respecting the architecture and quality guardrails.

## Environment Setup
1. Install Python 3.11 and Poetry.
2. Clone the repository and copy the environment template:
   ```bash
   cp .env.example .env
   ```
3. Install dependencies and hooks:
   ```bash
   make setup
   ```

## Development Workflow
- **Branching**: follow feature branches (`feature/<topic>`), request reviews via pull requests.
- **Formatting & Linting**: run `make lint` before committing; hooks enforce ruff + black + mypy.
- **Type Checking**: `make typecheck` must pass (mypy strict mode).
- **Testing**: add or update tests; ensure `make test` succeeds with coverage ≥ 80% (enforced in later prompts).
- **CI Bundle**: run `make ci` locally before opening a PR.

## Commit Standards
- Write meaningful commit messages (`<type>: <summary>`, e.g., `feat: add sales summary endpoint`).
- Keep commits focused; avoid mixing refactors with new features.
- Never commit secrets or machine-specific paths.

## Code Style
- Follow PEP 8, Pydantic v2 models, and FastAPI best practices.
- Ensure JSON logging via structlog; do not log PII (mask sensitive fields).
- Maintain clear service boundaries: routers → services → repositories → database.

## Testing Strategy
- Unit tests for pure logic (services, utils).
- Integration tests for DB interactions and ETL pipelines.
- End-to-end tests covering seed → ETL → API → recommendations.

## Security & Compliance
- JWT-based RBAC (HS256) with claims validation.
- Parameterized queries only; avoid raw SQL concatenation.
- No usage of `eval`, `exec`, or unsafe serialization.
- Keep `.env` out of version control; use `settings.py` with `pydantic_settings.BaseSettings`.

## Communication & ADRs
- Record major decisions under `docs/adr/`.
- When in doubt about requirements, document assumptions in the PR description.

Happy building!
