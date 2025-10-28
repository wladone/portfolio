#!/bin/bash
set -euo pipefail

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <db_backup_file> <redis_backup_file>"
    exit 1
fi

DB_BACKUP=$1
REDIS_BACKUP=$2

# Safety check
read -p "This will overwrite existing data. Are you sure? [y/N] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
fi

# Stop API service
echo "Stopping API service..."
docker compose stop api

# Database restore
echo "Restoring PostgreSQL backup..."
export PGPASSWORD="${POSTGRES_PASSWORD:-}"
pg_restore -h "${POSTGRES_HOST:-localhost}" \
           -U "${POSTGRES_USER:-postgres}" \
           -d "${POSTGRES_DB:-ecommerce}" \
           --clean --if-exists \
           "$DB_BACKUP"

# Redis restore
echo "Restoring Redis backup..."
redis-cli -h "${REDIS_HOST:-localhost}" -p "${REDIS_PORT:-6379}" FLUSHALL
redis-cli -h "${REDIS_HOST:-localhost}" -p "${REDIS_PORT:-6379}" --rdb "$REDIS_BACKUP"

# Restart API service
echo "Starting API service..."
docker compose up -d api

echo "Restore completed successfully"
