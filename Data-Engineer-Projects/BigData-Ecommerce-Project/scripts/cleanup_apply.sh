#!/usr/bin/env bash
# Destructive actions guarded by DRY_RUN=1 (default) and CONFIRM=DELETE
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ -f "$ROOT_DIR/.env" ]] && source "$ROOT_DIR/.env"
: "${PROJECT_ID:?Set PROJECT_ID in .env}"
: "${REGION:=europe-central2}"
: "${DATAFLOW_BUCKET:?Set DATAFLOW_BUCKET in .env}"
: "${BIGTABLE_INSTANCE:?Set BIGTABLE_INSTANCE in .env}"
: "${BIGTABLE_TABLE:=product_stats}"
DRY_RUN="${DRY_RUN:-1}"
DELETE_TOPICS="${DELETE_TOPICS:-0}"
DELETE_BQ_DATASET="${DELETE_BQ_DATASET:-0}"
DELETE_BIGTABLE_TABLE="${DELETE_BIGTABLE_TABLE:-0}"
KEEP_TEMPLATES="${KEEP_TEMPLATES:-1}"
[[ "$PROJECT_ID" == "your-project-id" ]] && { echo "Refusing to run with placeholder PROJECT_ID."; exit 2; }
run() { if [[ "$DRY_RUN" == "1" ]]; then echo "DRY_RUN: $*"; else eval "$@"; fi; }
must_confirm() { [[ "${CONFIRM:-}" == "DELETE" ]] || { echo "Set CONFIRM=DELETE to proceed."; exit 3; }; }
echo "=== APPLY CLEANUP (DRY_RUN=${DRY_RUN}) ==="
# 1) Drain/cancel Dataflow jobs
for id in $(gcloud dataflow jobs list --region="$REGION" --filter="STATE=Running OR STATE=Draining" --format="value(JOB_ID)" || true); do
  run "gcloud dataflow jobs drain \"$id\" --region=\"$REGION\" || gcloud dataflow jobs cancel \"$id\" --region=\"$REGION\" || true"
done
# 2) GCS tmp/staging purge
run "gsutil -m rm -r \"gs://${DATAFLOW_BUCKET}/tmp/**\" || true"
run "gsutil -m rm -r \"gs://${DATAFLOW_BUCKET}/staging/**\" || true"
if [[ "$KEEP_TEMPLATES" != "1" ]]; then
  run "gsutil -m rm -r \"gs://${DATAFLOW_BUCKET}/templates/**\" || true"
fi
# 3) Pub/Sub topics (optional)
if [[ "$DELETE_TOPICS" == "1" ]]; then
  must_confirm
  for t in clicks transactions stock dead-letter; do
    run "gcloud pubsub topics delete \"$t\" || true"
  done
fi
# 4) BigQuery dataset (optional)
if [[ "$DELETE_BQ_DATASET" == "1" ]]; then
  must_confirm
  run "bq rm -r -f -d \"${PROJECT_ID}:ecommerce\" || true"
fi
# 5) Bigtable table (optional)
if [[ "$DELETE_BIGTABLE_TABLE" == "1" ]]; then
  must_confirm
  run "cbt -project \"$PROJECT_ID\" -instance \"$BIGTABLE_INSTANCE\" deletetable \"$BIGTABLE_TABLE\" || true"
fi
echo "✔ Cleanup completed (DRY_RUN=${DRY_RUN})."
