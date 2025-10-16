#!/usr/bin/env bash
set -euo pipefail
: "${PROJECT_ID:?Set PROJECT_ID in .env or environment}"
: "${REGION:=europe-central2}"
: "${DATAFLOW_BUCKET:?Set DATAFLOW_BUCKET in .env or environment}"
gcloud config set project "$PROJECT_ID"
gcloud config set compute/region "$REGION"
# Regional GCS bucket (co-located)
gsutil mb -l "$REGION" "gs://${DATAFLOW_BUCKET}" || true
# Regional BigQuery dataset (matches REGION)
bq --location="${REGION}" mk --dataset "${PROJECT_ID}:ecommerce" || true
# Pub/Sub topics
for t in clicks transactions stock dead-letter; do
  gcloud pubsub topics create "$t" || true
done
echo "✔ Romania/EU setup complete in region: $REGION"
echo "Create Bigtable instance in the SAME region and add column family 'stats'."
