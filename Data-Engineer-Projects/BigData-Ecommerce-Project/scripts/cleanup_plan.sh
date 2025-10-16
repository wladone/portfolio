#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ -f "$ROOT_DIR/.env" ]] && source "$ROOT_DIR/.env"
: "${PROJECT_ID:?Set PROJECT_ID in .env}"
: "${REGION:=europe-central2}"
: "${DATAFLOW_BUCKET:?Set DATAFLOW_BUCKET in .env}"
: "${BIGTABLE_INSTANCE:?Set BIGTABLE_INSTANCE in .env}"
: "${BIGTABLE_TABLE:=product_stats}"
command -v gcloud >/dev/null || { echo "gcloud is required"; exit 1; }
echo "=== COSTS & CLEANUP PLAN (DRY RUN) ==="
echo "Project: $PROJECT_ID | Region: $REGION"
echo "Bucket: gs://${DATAFLOW_BUCKET} | Bigtable: ${BIGTABLE_INSTANCE}/${BIGTABLE_TABLE}"
echo
echo "• Dataflow jobs (Running/Draining):"
gcloud dataflow jobs list --region="$REGION" --format="table(JOB_ID,NAME,TYPE,STATE,CREATED_TIME)" \
  --filter="STATE=Running OR STATE=Draining" || true
echo
echo "• GCS artifacts (tmp/staging/templates) in gs://${DATAFLOW_BUCKET}:"
gsutil ls -r "gs://${DATAFLOW_BUCKET}/tmp/**" 2>/dev/null || true
gsutil ls -r "gs://${DATAFLOW_BUCKET}/staging/**" 2>/dev/null || true
gsutil ls -r "gs://${DATAFLOW_BUCKET}/templates/**" 2>/dev/null || true
echo
echo "• BigQuery dataset tables (ecommerce):"
bq ls -n 100 "${PROJECT_ID}:ecommerce" 2>/dev/null || echo "(dataset may not exist)"
echo
echo "• Pub/Sub topics:"
for t in clicks transactions stock dead-letter; do
  gcloud pubsub topics describe "$t" --format="value(name)" 2>/dev/null || true
done
echo
echo "• Bigtable table presence:"
cbt -project "$PROJECT_ID" -instance "$BIGTABLE_INSTANCE" ls 2>/dev/null | grep -E "^${BIGTABLE_TABLE}$" || echo "(table may not exist)"
echo
echo "This is a PLAN only. Use scripts/cleanup_apply.sh to act."
