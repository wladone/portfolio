#!/usr/bin/env bash
set -euo pipefail
[[ -f ".env" ]] && source ".env"
: "${PROJECT_ID:?Set PROJECT_ID in .env}"
gcloud pubsub topics publish clicks --message='{"event_time":"2025-01-01T12:00:00Z","product_id":"P123","user_id":"U9"}' --project "$PROJECT_ID"
gcloud pubsub topics publish transactions --message='{"event_time":"2025-01-01T12:00:10Z","product_id":"P123","store_id":"S1","qty":1}' --project "$PROJECT_ID"
gcloud pubsub topics publish stock --message='{"event_time":"2025-01-01T12:00:20Z","product_id":"P123","warehouse_id":"W1","delta":5}' --project "$PROJECT_ID"
echo "✔ Sample messages published."
