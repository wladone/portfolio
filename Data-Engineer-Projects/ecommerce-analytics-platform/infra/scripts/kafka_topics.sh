#!/bin/bash
set -e

# Create orders topic if it doesn't exist
echo "Creating orders topic..."
rpk topic create orders --brokers redpanda:9092 --replicas 1 --partitions 1 || true

echo "Listing available topics:"
rpk topic list --brokers redpanda:9092
