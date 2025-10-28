#!/bin/bash
set -euo pipefail

DIAG_DIR=".dist/diag_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$DIAG_DIR"

# Collect environment info
env | grep -E "^(POSTGRES_|REDIS_|KAFKA_|API_)" > "$DIAG_DIR/env.txt"

# Collect version info
{
    echo "Python version:"
    python --version
    echo
    echo "Poetry version:"
    poetry --version
    echo
    echo "Docker version:"
    docker --version
    echo
    echo "Docker Compose version:"
    docker compose version
} > "$DIAG_DIR/versions.txt"

# Database info (mask passwords in output)
export PGPASSWORD="${POSTGRES_PASSWORD:-}"
psql -h "${POSTGRES_HOST:-localhost}" -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-ecommerce}" \
    -c "\dt dw.*" 2>/dev/null | sed 's/password=[^ ]*/password=****/g' > "$DIAG_DIR/tables.txt"

# Collect container logs (last 1000 lines)
docker compose logs --tail=1000 api > "$DIAG_DIR/api_logs.txt"
docker compose logs --tail=1000 etl > "$DIAG_DIR/etl_logs.txt"

# Collect metrics snapshot
curl -s http://localhost:8000/metrics > "$DIAG_DIR/metrics.txt"

# Create archive
cd .dist
tar czf "diag_$(date +%Y%m%d_%H%M%S).tar.gz" "diag_$(date +%Y%m%d_%H%M%S)"
cd -

echo "Diagnostic info collected in $DIAG_DIR"
echo "Archive created at .dist/diag_$(date +%Y%m%d_%H%M%S).tar.gz"
