#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ -f "$ROOT_DIR/.env" ]] && source "$ROOT_DIR/.env"
: "${PROJECT_ID:?Set PROJECT_ID in .env or environment}"
DASHBOARD_PATH="${ROOT_DIR}/monitoring/dashboard_ecommerce_pipeline.json"
ALERT_DEAD_LETTER_PATH="${ROOT_DIR}/monitoring/alert_dead_letter_policy.json"
ALERT_LOW_THROUGHPUT_PATH="${ROOT_DIR}/monitoring/alert_low_throughput_policy.json"
command -v gcloud >/dev/null || { echo "gcloud CLI is required"; exit 1; }

echo "Deploying Cloud Monitoring dashboard"
gcloud monitoring dashboards create \
  --project="$PROJECT_ID" \
  --config-from-file="$DASHBOARD_PATH" \
  || { echo "Dashboard creation failed. Consider using dashboards update with an existing ID."; }

echo "Creating dead-letter alert policy"
gcloud alpha monitoring policies create \
  --project="$PROJECT_ID" \
  --policy-from-file="$ALERT_DEAD_LETTER_PATH" \
  || { echo "Dead-letter policy creation failed. Update the file and retry."; }

echo "Creating low-throughput alert policy"
gcloud alpha monitoring policies create \
  --project="$PROJECT_ID" \
  --policy-from-file="$ALERT_LOW_THROUGHPUT_PATH" \
  || { echo "Low-throughput policy creation failed. Update the file and retry."; }

echo "Monitoring assets deployed (if no errors above). Add notification channels via gcloud or Console."
