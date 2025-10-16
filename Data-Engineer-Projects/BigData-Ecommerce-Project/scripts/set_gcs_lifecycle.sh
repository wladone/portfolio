#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ -f "$ROOT_DIR/.env" ]] && source "$ROOT_DIR/.env"
: "${DATAFLOW_BUCKET:?Set DATAFLOW_BUCKET in .env or environment}"
TTL_DAYS="${TTL_DAYS:-3}"
command -v gsutil >/dev/null || { echo "gsutil is required"; exit 1; }

tmp_json="$(mktemp)"
trap 'rm -f "$tmp_json"' EXIT

cat > "$tmp_json" <<JSON
{
  "rule": [
    {
      "action": {"type": "Delete"},
      "condition": {
        "age": ${TTL_DAYS},
        "matchesPrefix": ["tmp/", "staging/"]
      }
    }
  ]
}
JSON

echo "Applying lifecycle policy to gs://${DATAFLOW_BUCKET} (TTL=${TTL_DAYS}d for tmp/ and staging/)"
gsutil lifecycle set "$tmp_json" "gs://${DATAFLOW_BUCKET}"
echo "Lifecycle policy updated."
