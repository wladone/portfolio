#!/usr/bin/env bash
set -euo pipefail

echo "[bootstrap] Installing dependencies via Poetry..."
poetry install --no-interaction --no-ansi

echo "[bootstrap] Applying database migrations..."
poetry run alembic upgrade head || true

echo "[bootstrap] Ready. Use 'make compose-up' to start services."
