#!/usr/bin/env bash
set -euo pipefail
MODE="${1:-local}"
python run.py "$MODE"
