"""Recommendations endpoints."""

from __future__ import annotations

import time
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from prometheus_client import Counter, Histogram
from sqlalchemy.orm import Session

from backend.app.config import get_settings
from backend.app.core.cache import RedisCache
from backend.app.core.db import get_db
from backend.app.schemas.recs import (
    SimilarParams,
    SimilarResponse,
    UserRecsParams,
    UserRecsResponse,
)
from backend.app.security.dependencies import require_roles
from backend.app.services.recs_service import RecsService

router = APIRouter(prefix="/recs", tags=["recs"])

RecsGuard = Annotated[None, Depends(require_roles("app", "analyst", "admin"))]
AdminGuard = Annotated[None, Depends(require_roles("admin"))]

# Prometheus metrics
settings = get_settings()
REQUEST_COUNT = Counter(
    "recs_requests_total",
    "Total number of requests to recommendations endpoints",
    ["endpoint", "status"],
    namespace=settings.prometheus_namespace,
)
REQUEST_DURATION = Histogram(
    "recs_request_duration_seconds",
    "Duration of requests to recommendations endpoints",
    ["endpoint"],
    namespace=settings.prometheus_namespace,
)


def get_recs_service(db: Annotated[Session, Depends(get_db)]) -> RecsService:
    """Dependency to get RecsService instance."""
    cache = RedisCache(settings.redis_url)
    return RecsService(db, cache)


@router.get(
    "/user/{user_id}",
    response_model=UserRecsResponse,
    status_code=status.HTTP_200_OK,
    summary="Get user recommendations",
    description="Retrieve personalized product recommendations for a user based on ALS model with optional filtering.",
    responses={
        200: {
            "description": "User recommendations retrieved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "user_id": 123,
                        "k": 10,
                        "model_version": "v1.0.0",
                        "items": [
                            {
                                "sku": "PROD-001",
                                "name": "Wireless Headphones",
                                "category": "Electronics",
                                "score": 0.95,
                                "reason": "als",
                            }
                        ],
                    }
                }
            },
        },
        400: {"description": "Invalid request parameters"},
        422: {"description": "Validation error"},
        500: {"description": "Internal server error"},
    },
)
async def get_user_recs(
    request: Request,
    response: Response,
    user_id: int,
    params: Annotated[UserRecsParams, Depends()],
    service: Annotated[RecsService, Depends(get_recs_service)],
    _: RecsGuard,
) -> UserRecsResponse:
    """Get personalized recommendations for a user."""
    start_time = time.time()
    endpoint = "user_recs"
    try:
        items, model_version = await service.recommend_for_user(
            user_id, params.k, params.exclude_seen, params.fallback_strategy
        )
        result = UserRecsResponse(
            user_id=user_id,
            k=params.k,
            model_version=model_version,
            items=items,
        )
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
    "/similar-products/{sku}",
    response_model=SimilarResponse,
    status_code=status.HTTP_200_OK,
    summary="Get similar products",
    description="Retrieve products similar to the given SKU based on collaborative filtering.",
    responses={
        200: {
            "description": "Similar products retrieved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "sku": "PROD-001",
                        "k": 10,
                        "model_version": "v1.0.0",
                        "items": [
                            {
                                "sku": "PROD-002",
                                "name": "Bluetooth Speaker",
                                "category": "Electronics",
                                "score": 0.87,
                                "reason": "similarity",
                            }
                        ],
                    }
                }
            },
        },
        400: {"description": "Invalid request parameters"},
        422: {"description": "Validation error"},
        500: {"description": "Internal server error"},
    },
)
async def get_similar_products(
    request: Request,
    response: Response,
    sku: str,
    params: Annotated[SimilarParams, Depends()],
    service: Annotated[RecsService, Depends(get_recs_service)],
    _: RecsGuard,
) -> SimilarResponse:
    """Get products similar to the given SKU."""
    start_time = time.time()
    endpoint = "similar_products"
    try:
        items, model_version = await service.similar_for_sku(sku, params.k)
        result = SimilarResponse(
            sku=sku,
            k=params.k,
            model_version=model_version,
            items=items,
        )
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


@router.post(
    "/_refresh",
    status_code=status.HTTP_200_OK,
    summary="Refresh recommendation model",
    description="Force reload the latest recommendation model from artifacts.",
    responses={
        200: {
            "description": "Model refreshed successfully",
            "content": {
                "application/json": {
                    "example": {"model_version": "v1.0.1", "status": "reloaded"}
                }
            },
        },
        404: {"description": "Refresh endpoint not enabled"},
        500: {"description": "Internal server error"},
    },
)
async def refresh_model(
    request: Request,
    response: Response,
    service: Annotated[RecsService, Depends(get_recs_service)],
    _: AdminGuard,
) -> dict[str, str]:
    """Refresh the recommendation model."""
    start_time = time.time()
    endpoint = "refresh"
    try:
        if not settings.recs_allow_refresh_endpoint:
            REQUEST_COUNT.labels(endpoint=endpoint, status="404").inc()
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Refresh endpoint not enabled",
            )
        _, model_version = await service.refresh()
        result = {"model_version": model_version, "status": "reloaded"}
        REQUEST_COUNT.labels(endpoint=endpoint, status="200").inc()
        return result
    except HTTPException:
        raise
    except Exception as e:
        REQUEST_COUNT.labels(endpoint=endpoint, status="500").inc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        ) from e
    finally:
        REQUEST_DURATION.labels(endpoint=endpoint).observe(time.time() - start_time)
