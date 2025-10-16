#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ -f "$ROOT_DIR/.env" ]] && source "$ROOT_DIR/.env"
: "${PROJECT_ID:?Set PROJECT_ID in .env or environment}"
: "${DATASET_NAME:=ecommerce}"
TTL_DAYS="${TTL_DAYS:-3}"
TTL_SECONDS=$((TTL_DAYS * 24 * 60 * 60))
VIEWS_TABLE="${VIEWS_TABLE:-product_views_summary}"
SALES_TABLE="${SALES_TABLE:-sales_summary}"
STOCK_TABLE="${STOCK_TABLE:-inventory_summary}"
command -v bq >/dev/null || { echo "bq CLI is required"; exit 1; }

echo "=== Setting default TTLs for dataset ${PROJECT_ID}:${DATASET_NAME} (TTL=${TTL_DAYS}d) ==="
if ! bq --project_id="$PROJECT_ID" ls -d "${PROJECT_ID}:${DATASET_NAME}" >/dev/null 2>&1; then
  echo "Dataset ${PROJECT_ID}:${DATASET_NAME} does not exist. Create it before running this script." >&2
  exit 2
fi

bq --project_id="$PROJECT_ID" update \
  --default_table_expiration "$TTL_SECONDS" \
  --default_partition_expiration "$TTL_SECONDS" \
  "${PROJECT_ID}:${DATASET_NAME}" >/dev/null && \
  echo "Default TTLs applied"

for table in "$VIEWS_TABLE" "$SALES_TABLE" "$STOCK_TABLE"; do
  TABLE_REF="${PROJECT_ID}:${DATASET_NAME}.${table}"
  if bq --project_id="$PROJECT_ID" show "$TABLE_REF" >/dev/null 2>&1; then
    bq --project_id="$PROJECT_ID" update --expiration "$TTL_SECONDS" "$TABLE_REF" >/dev/null && \
      echo "Updated TTL for $TABLE_REF"
  else
    echo "Skipping $TABLE_REF (table not found)"
  fi
done

echo "Completed TTL configuration."
