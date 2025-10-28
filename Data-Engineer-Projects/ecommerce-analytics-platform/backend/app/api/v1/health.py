"""Health check endpoints for monitoring application status."""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.app.config import get_settings
from backend.app.core.cache import RedisCache
from backend.app.core.db import get_session
from ml.serve import AlsRecommender

router = APIRouter()


@router.get("/health", tags=["monitoring"])
async def health() -> dict[str, str]:
    """Simple health check endpoint that always returns OK."""
    return {"status": "ok"}


@router.get("/livez", tags=["monitoring"])
async def liveness() -> dict[str, str]:
    """Liveness check to verify the event loop is functional."""
    try:
        # Simple check: schedule a no-op task to verify event loop
        await asyncio.sleep(0)
        return {"status": "ok"}
    except Exception:
        return {"status": "error"}


@router.get("/readyz", tags=["monitoring"])
async def readiness(
    db: Session = Depends(get_session),
    settings: Any = Depends(get_settings),
) -> dict[str, Any]:
    """Readiness check verifying DB, Redis, and optional ALS recommender."""
    checks = {}
    ready = True

    # DB connectivity check
    try:
        db.execute(text("SELECT 1"))
        checks["database"] = True
    except Exception:
        checks["database"] = False
        ready = False

    # Redis connectivity check
    try:
        redis_cache = RedisCache(settings.redis_url)
        client = await redis_cache._get_client()
        await client.ping()
        checks["redis"] = True
    except Exception:
        checks["redis"] = False
        ready = False

    # ALS recommender check (if artifact dir is set)
    if settings.recs_artifact_dir:
        try:
            AlsRecommender.load_latest(settings.recs_artifact_dir)
            checks["als_recommender"] = True
        except Exception:
            checks["als_recommender"] = False
            ready = False
    else:
        checks["als_recommender"] = None  # Not configured

    return {"ready": ready, "checks": checks}
