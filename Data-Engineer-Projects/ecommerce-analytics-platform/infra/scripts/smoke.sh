#!/bin/bash
set -euo pipefail

echo "Starting smoke tests..."

# Check health endpoint
echo "Checking health endpoint..."
HEALTH_RESPONSE=$(curl -s http://localhost:8000/health)
if [[ ! $HEALTH_RESPONSE =~ "status\":\"pass" ]]; then
    echo "Health check failed"
    exit 1
fi

# Check readiness
echo "Checking readiness..."
READY_RESPONSE=$(curl -s http://localhost:8000/readyz)
if [[ ! $READY_RESPONSE =~ "status\":\"pass" ]]; then
    echo "Readiness check failed"
    exit 1
fi

# Create test admin token
echo "Creating test token..."
ADMIN_TOKEN=$(poetry run python scripts/create_token.py admin)

# Test DW query
echo "Testing DW query..."
SALES_RESPONSE=$(curl -s -H "Authorization: Bearer $ADMIN_TOKEN" \
  "http://localhost:8000/api/v1/sales/summary?start_date=2023-01-01&end_date=2023-01-31")
if [[ ! $SALES_RESPONSE =~ "total_sales" ]]; then
    echo "DW query test failed"
    exit 1
fi

# Test recommendations
echo "Testing recommendations API..."
RECS_RESPONSE=$(curl -s -H "Authorization: Bearer $ADMIN_TOKEN" \
  "http://localhost:8000/api/v1/recs/user/1?k=3")
if [[ ! $RECS_RESPONSE =~ "items" ]]; then
    echo "Recommendations test failed"
    exit 1
fi

echo "SMOKE OK"
exit 0
