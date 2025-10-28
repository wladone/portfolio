#!/bin/bash
set -euo pipefail

# Configuration
BACKUP_DIR=".dist/backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
mkdir -p "$BACKUP_DIR"

# Database backup
echo "Starting PostgreSQL backup..."
export PGPASSWORD="${POSTGRES_PASSWORD:-}"
pg_dump -h "${POSTGRES_HOST:-localhost}" \
        -U "${POSTGRES_USER:-postgres}" \
        -d "${POSTGRES_DB:-ecommerce}" \
        --clean --if-exists \
        --format=custom \
        -f "$BACKUP_DIR/db_$TIMESTAMP.backup"

# Redis backup
echo "Starting Redis backup..."
redis-cli -h "${REDIS_HOST:-localhost}" -p "${REDIS_PORT:-6379}" save
redis-cli -h "${REDIS_HOST:-localhost}" -p "${REDIS_PORT:-6379}" --rdb "$BACKUP_DIR/redis_$TIMESTAMP.rdb"

# Optional MinIO backup
if [ -n "${MINIO_ACCESS_KEY:-}" ] && [ -n "${MINIO_SECRET_KEY:-}" ]; then
    echo "Uploading backups to MinIO..."
    mc alias set minio http://${MINIO_HOST:-localhost:9000} "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY"
    mc cp "$BACKUP_DIR/db_$TIMESTAMP.backup" "minio/backups/"
    mc cp "$BACKUP_DIR/redis_$TIMESTAMP.rdb" "minio/backups/"
fi

echo "Backup completed successfully:"
echo "- Database: $BACKUP_DIR/db_$TIMESTAMP.backup"
echo "- Redis: $BACKUP_DIR/redis_$TIMESTAMP.rdb"
