#!/bin/bash

# wait_for.sh: POSIX script that waits for host:port to be healthy with configurable timeout
# Usage: wait_for.sh <host> <port> [timeout_seconds]
# Default timeout is 30 seconds if not provided

set -euo pipefail

HOST="$1"
PORT="$2"
TIMEOUT="${3:-30}"

if [ -z "$HOST" ] || [ -z "$PORT" ]; then
    echo "Usage: $0 <host> <port> [timeout_seconds]"
    exit 1
fi

echo "Waiting for $HOST:$PORT to be available (timeout: ${TIMEOUT}s)..."

for ((i=1; i<=TIMEOUT; i++)); do
    if nc -z "$HOST" "$PORT" 2>/dev/null; then
        echo "$HOST:$PORT is available"
        exit 0
    fi
    sleep 1
done

echo "Timeout: $HOST:$PORT is not available after ${TIMEOUT}s"
exit 1
