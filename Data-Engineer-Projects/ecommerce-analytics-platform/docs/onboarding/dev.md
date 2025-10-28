# Developer Onboarding Guide

Welcome to the E-commerce Analytics Platform development team! This guide will help you get set up and productive quickly.

## Development Environment Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd ecommerce-analytics
```

### 2. Install Prerequisites

- Python 3.10+
- Poetry 1.5+
- Docker Engine 24.0+
- Docker Compose v2.10+
- Make

### 3. Configure Environment

```bash
# Install dependencies
make setup

# Set up pre-commit hooks
poetry run pre-commit install

# Copy environment file
cp .env.example .env
```

### 4. Start Development Services

```bash
# Start infrastructure
make compose-up

# Apply migrations
poetry run alembic upgrade head

# Load seed data
make seed

# Train initial model
make train
```

## Development Workflow

### 1. Code Organization

```
backend/
├── app/               # FastAPI application
│   ├── api/          # API routes and handlers
│   ├── core/         # Core functionality
│   ├── models/       # SQLAlchemy models
│   └── services/     # Business logic
├── tests/            # Test suite
└── alembic/          # Database migrations

etl/
├── extractors/       # Data extraction
├── transformers/     # Data transformation
└── loaders/         # Data loading

ml/
├── als_train.py     # Recommendation model
└── serve.py         # Model serving
```

### 2. Common Development Tasks

#### Running Tests

```bash
# All tests
make test

# Single test file
poetry run pytest tests/test_sales_api.py -v

# With coverage
poetry run pytest --cov=backend
```

#### Database Migrations

```bash
# Create migration
poetry run alembic revision -m "add_user_preferences"

# Apply migrations
poetry run alembic upgrade head

# Rollback one version
poetry run alembic downgrade -1
```

#### Code Quality

```bash
# Run linting
make lint

# Run type checking
make typecheck

# Format code
poetry run black backend/
poetry run isort backend/
```

### 3. API Development

#### Adding New Endpoints

1. Create route in `backend/app/api/v1/`:
```python
from fastapi import APIRouter, Depends
from app.core.auth import get_current_user

router = APIRouter()

@router.get("/my_endpoint")
async def my_endpoint(
    user = Depends(get_current_user)
):
    return {"message": "Hello"}
```

2. Register in `backend/app/api/router.py`:
```python
from app.api.v1 import my_module

router.include_router(
    my_module.router,
    prefix="/v1/my_module",
    tags=["my_module"]
)
```

#### Authentication & Authorization

```python
# Require specific role
from app.core.auth import require_role

@router.post("/admin_only")
@require_role("admin")
async def admin_endpoint():
    pass
```

### 4. ETL Development

#### Adding New Pipeline

1. Create extractor in `etl/extractors/`:
```python
from etl.extractors import BaseExtractor

class MyExtractor(BaseExtractor):
    async def extract(self):
        return await self.conn.fetch("SELECT...")
```

2. Create transformer in `etl/transformers/`:
```python
from etl.transformers import BaseTransformer

class MyTransformer(BaseTransformer):
    def transform(self, data):
        return [...]
```

3. Register in `etl/pipeline.py`

### 5. Testing Guidelines

1. Use fixtures from `tests/conftest.py`
2. Mock external services
3. Use proper test isolation
4. Follow naming convention: `test_<what>_<scenario>`

Example:
```python
async def test_sales_summary_filters_dates(
    client: AsyncClient,
    admin_token: str
):
    response = await client.get(
        "/api/v1/sales/summary",
        headers={"Authorization": f"Bearer {admin_token}"},
        params={"start_date": "2023-01-01"}
    )
    assert response.status_code == 200
```

## Code Review Process

1. Create feature branch from `main`
2. Make changes and test locally
3. Push and create PR
4. Ensure CI passes
5. Get approval from 1 team member
6. Squash and merge

## Troubleshooting

### Common Issues

1. Database connection:
```bash
make compose-down
rm -rf .postgres_data
make compose-up
```

2. Redis cache:
```bash
redis-cli -h localhost FLUSHALL
```

3. Kafka topics:
```bash
rpk topic list
rpk topic create my_topic -p 3
```

### Development Tools

1. Prometheus UI: http://localhost:9090
2. Grafana: http://localhost:3000
3. API Docs: http://localhost:8000/docs

## Additional Resources

1. [Architecture Documentation](../docs/architecture.md)
2. [API Guidelines](../docs/api/guidelines.md)
3. [ETL Best Practices](../docs/etl/best_practices.md)
4. [Testing Strategy](../docs/testing.md)

## Getting Help

- Slack: #team-ecommerce
- Tech Lead: @techlead
- Standups: Daily 10:00 AM
- Office Hours: Wednesday 2-4 PM
