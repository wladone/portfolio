#!/bin/bash

# entrypoint_etl.sh: Script that waits for Postgres, then runs ETL load command
# Usage: entrypoint_etl.sh [etl_args...]

set -euo pipefail

# Wait for Postgres
echo "Waiting for Postgres..."
./infra/scripts/wait_for.sh "${POSTGRES_HOST:-postgres}" "${POSTGRES_PORT:-5432}"

# Run ETL load
echo "Starting ETL load..."
exec python -m etl.load "$@"
