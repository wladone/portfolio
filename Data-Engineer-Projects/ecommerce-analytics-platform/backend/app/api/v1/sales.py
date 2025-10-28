"""Sales analytics endpoints."""

from __future__ import annotations

import time
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from prometheus_client import Counter, Histogram
from sqlalchemy.orm import Session

from backend.app.config import get_settings
from backend.app.core.cache import RedisCache
from backend.app.core.db import get_db
from backend.app.schemas.sales import (
    SalesSummaryParams,
    SalesSummaryResponse,
    TopProductsParams,
    TopProductsResponse,
)
from backend.app.security.dependencies import require_roles
from backend.app.services.sales_service import SalesService

router = APIRouter(
    prefix="/sales",
    tags=["sales"],
    dependencies=[Depends(require_roles("analyst", "admin"))],
)

# Prometheus metrics
settings = get_settings()
REQUEST_COUNT = Counter(
    "sales_requests_total",
    "Total number of requests to sales endpoints",
    ["endpoint", "status"],
    namespace=settings.prometheus_namespace,
)
REQUEST_DURATION = Histogram(
    "sales_request_duration_seconds",
    "Duration of requests to sales endpoints",
    ["endpoint"],
    namespace=settings.prometheus_namespace,
)


def get_sales_service(db: Annotated[Session, Depends(get_db)]) -> SalesService:
    """Dependency to get SalesService instance."""
    from backend.app.config import get_settings

    settings = get_settings()
    cache = RedisCache(settings.redis_url)
    return SalesService(db, cache)


@router.get(
    "/summary",
    response_model=SalesSummaryResponse,
    status_code=status.HTTP_200_OK,
    summary="Get sales summary analytics",
    description="Retrieve aggregated sales metrics with optional filtering by date range, channel, and granularity.",
    responses={
        200: {
            "description": "Sales summary data retrieved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "rows": [
                            {
                                "date": "2023-01-01",
                                "channel_code": "online",
                                "orders": 150,
                                "items": 450,
                                "gross": 15000.00,
                                "discount": 500.00,
                                "net": 14500.00,
                                "avg_order_value": 96.67,
                            }
                        ],
                        "from_": "2023-01-01",
                        "to": "2023-01-31",
                        "channel": None,
                        "granularity": "day",
                    }
                }
            },
        },
        400: {"description": "Invalid request parameters"},
        422: {"description": "Validation error"},
        500: {"description": "Internal server error"},
    },
)
async def get_sales_summary(
    request: Request,
    response: Response,
    params: Annotated[SalesSummaryParams, Depends()],
    service: Annotated[SalesService, Depends(get_sales_service)],
) -> SalesSummaryResponse:
    """Get sales summary analytics with optional filtering."""
    start_time = time.time()
    endpoint = "summary"
    try:
        result = await service.get_sales_summary(params)
        REQUEST_COUNT.labels(endpoint=endpoint, status="200").inc()
        return result
    except Exception as e:
        REQUEST_COUNT.labels(endpoint=endpoint, status="500").inc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        ) from e
    finally:
        REQUEST_DURATION.labels(endpoint=endpoint).observe(time.time() - start_time)


@router.get(
    "/top-products",
    response_model=TopProductsResponse,
    status_code=status.HTTP_200_OK,
    summary="Get top products analytics",
    description="Retrieve top-selling products with optional filtering by date range, channel, and sorting metric.",
    responses={
        200: {
            "description": "Top products data retrieved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "rows": [
                            {
                                "sku": "PROD-001",
                                "name": "Wireless Headphones",
                                "category": "Electronics",
                                "items": 250.0,
                                "gross": 50000.00,
                                "net": 47500.00,
                            }
                        ],
                        "metric": "net",
                        "limit": 50,
                        "offset": 0,
                        "total": 100,
                    }
                }
            },
        },
        400: {"description": "Invalid request parameters"},
        422: {"description": "Validation error"},
        500: {"description": "Internal server error"},
    },
)
async def get_top_products(
    request: Request,
    response: Response,
    params: Annotated[TopProductsParams, Depends()],
    service: Annotated[SalesService, Depends(get_sales_service)],
) -> TopProductsResponse:
    """Get top products analytics with optional filtering."""
    start_time = time.time()
    endpoint = "top_products"
    try:
        result = await service.get_top_products(params)
        REQUEST_COUNT.labels(endpoint=endpoint, status="200").inc()
        return result
    except Exception as e:
        REQUEST_COUNT.labels(endpoint=endpoint, status="500").inc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        ) from e
    finally:
        REQUEST_DURATION.labels(endpoint=endpoint).observe(time.time() - start_time)
