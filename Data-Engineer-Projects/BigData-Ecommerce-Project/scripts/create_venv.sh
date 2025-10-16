#!/usr/bin/env bash
set -euo pipefail
PY=${PYTHON:-python3.10}
$PY -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r beam/requirements.txt -r requirements-dev.txt
echo "✅ venv ready. Activate with: source .venv/bin/activate"
