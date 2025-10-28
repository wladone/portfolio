#!/bin/bash

# entrypoint_api.sh: Script that waits for Postgres and Redis, optionally runs alembic upgrade, then starts uvicorn
# Usage: entrypoint_api.sh [run_migrations]
# If run_migrations is provided and non-empty, alembic upgrade will be run

set -euo pipefail

RUN_MIGRATIONS="${1:-}"

# Wait for Postgres
echo "Waiting for Postgres..."
./infra/scripts/wait_for.sh "${POSTGRES_HOST:-postgres}" "${POSTGRES_PORT:-5432}"

# Wait for Redis
echo "Waiting for Redis..."
./infra/scripts/wait_for.sh "${REDIS_HOST:-redis}" "${REDIS_PORT:-6379}"

# Optionally run Alembic migrations
if [ -n "$RUN_MIGRATIONS" ]; then
    echo "Running Alembic migrations..."
    alembic upgrade head
fi

# Start Uvicorn
echo "Starting Uvicorn..."
exec uvicorn backend.app.main:app --host 0.0.0.0 --port 8000 --reload
