# Day 1 Operations

This guide walks you through setting up the E-commerce Analytics Platform from scratch on a fresh machine.

## Prerequisites

1. Docker Engine 24.0+
2. Docker Compose v2.10+
3. Python 3.10+
4. Poetry 1.5+
5. Make

## Initial Setup

1. Clone the repository and navigate to it:
   ```bash
   git clone <repository-url>
   cd ecommerce-analytics
   ```

2. Copy and configure environment variables:
   ```bash
   cp .env.example .env
   ```

   Required adjustments in `.env`:
   ```ini
   # Database
   POSTGRES_PASSWORD=<strong-password>

   # Redis
   REDIS_PASSWORD=<strong-password>

   # JWT
   JWT_SECRET_KEY=<generate-random-key>

   # API
   API_ADMIN_EMAIL=admin@example.com
   API_ADMIN_PASSWORD=<admin-password>
   ```

3. Install dependencies:
   ```bash
   make setup
   ```

## Infrastructure Startup

1. Start all services:
   ```bash
   make compose-up
   ```

2. Apply database migrations:
   ```bash
   poetry run alembic upgrade head
   ```

3. Generate and load seed data:
   ```bash
   make seed
   ```

4. Train initial recommendation model:
   ```bash
   make train
   ```

## User Setup

1. Create admin user:
   ```bash
   make create-admin
   ```

2. Create analyst user (optional):
   ```bash
   make create-analyst
   ```

## Validation

1. Run smoke tests:
   ```bash
   make smoke
   ```

   Expected output:
   ```
   SMOKE OK
   ```

2. Check health endpoints:
   ```bash
   curl http://localhost:8000/health
   curl http://localhost:8000/readyz
   ```

3. Verify metrics endpoint:
   ```bash
   curl http://localhost:8000/metrics
   ```

4. Test initial API queries:
   ```bash
   # Get admin token
   TOKEN=$(poetry run python scripts/create_token.py admin)

   # Test sales summary
   curl -H "Authorization: Bearer $TOKEN" \
     "http://localhost:8000/api/v1/sales/summary?start_date=2023-01-01&end_date=2023-01-31"

   # Test recommendations
   curl -H "Authorization: Bearer $TOKEN" \
     "http://localhost:8000/api/v1/recs/user/1?k=3"
   ```

## Security Checks

1. Verify JWT key setup:
   ```bash
   scripts/rotate_jwt_keys.py list
   ```

2. Check key rotation:
   ```bash
   scripts/rotate_jwt_keys.py rotate --kid $(date +%Y-%m-%d)
   ```

3. Reload API keys:
   ```bash
   curl -X POST -H "Authorization: Bearer $TOKEN" \
     http://localhost:8000/auth/_reload-keys
   ```

## Production Considerations

1. Password Security:
   - Use generated strong passwords in `.env`
   - Store secrets in vault/KMS in production
   - Rotate JWT keys regularly

2. Network Security:
   - Configure firewall rules
   - Use TLS termination
   - Set proper CORS headers

3. Monitoring Setup:
   - Configure Prometheus scraping
   - Set up Grafana dashboards
   - Enable logging to central system

4. Backup Configuration:
   - Schedule regular backups (see Day-2 ops)
   - Test restore procedure
   - Set up MinIO credentials if using object storage

## Next Steps

1. Review [Day-2 Operations](day2.md)
2. Set up [Monitoring](../sre/slis_slos.md)
3. Configure [Backup Schedule](../data/retention_policy.md)
