#!/usr/bin/env bash
# Light-weight, safe cost controls you can cron nightly.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ -f "$ROOT_DIR/.env" ]] && source "$ROOT_DIR/.env"
: "${PROJECT_ID:?Set PROJECT_ID in .env}"
: "${REGION:=europe-central2}"
: "${DATAFLOW_BUCKET:?Set DATAFLOW_BUCKET in .env}"
JOB_NAME_PREFIX="${JOB_NAME_PREFIX:-streaming}"   # Only touch jobs with this prefix
DRY_RUN="${DRY_RUN:-1}"
run() { if [[ "$DRY_RUN" == "1" ]]; then echo "DRY_RUN: $*"; else eval "$@"; fi; }
echo "=== COST GUARDRAILS (DRY_RUN=${DRY_RUN}) ==="
# A) Drain running Dataflow jobs with the given name prefix (safer than stopping all)
while read -r jid name; do
  [[ -z "$jid" || -z "$name" ]] && continue
  if [[ "$name" == ${JOB_NAME_PREFIX}* ]]; then
    run "gcloud dataflow jobs drain \"$jid\" --region=\"$REGION\" || true"
  fi
done < <(gcloud dataflow jobs list --region="$REGION" --filter="STATE=Running" --format="value(JOB_ID,NAME)")
# B) Prune GCS tmp/staging
run "gsutil -m rm -r \"gs://${DATAFLOW_BUCKET}/tmp/**\" || true"
run "gsutil -m rm -r \"gs://${DATAFLOW_BUCKET}/staging/**\" || true"
# C) BigQuery dataset TTLs (30 days default if not already set)
run "bq update --default_table_expiration 2592000 --default_partition_expiration 2592000 \"${PROJECT_ID}:ecommerce\" || true"
# D) Pub/Sub message retention to 24h (keeps cost low, adjust if needed)
for t in clicks transactions stock dead-letter; do
  run "gcloud pubsub topics update \"$t\" --message-retention-duration=86400s || true"
done
echo "✔ Guardrails done. Review Bigtable nodes separately (billing is per node)."
