"""Root API router providing health and metrics endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from backend.app.security.dependencies import require_roles

from .v1.auth import router as auth_router
from .v1.cache import router as cache_router
from .v1.health import router as health_router
from .v1.recs import router as recs_router
from .v1.sales import router as sales_router

router = APIRouter()


@router.get("/metrics", tags=["monitoring"])
async def metrics() -> Response:
    """Expose Prometheus metrics collected by the application."""
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)


@router.get(
    "/admin/ping",
    tags=["admin"],
    dependencies=[Depends(require_roles("admin"))],
)
async def admin_ping() -> dict[str, bool]:
    """Simple admin-only endpoint to verify privileged access."""
    return {"ok": True}


router.include_router(health_router, prefix="")
router.include_router(auth_router, prefix="")
router.include_router(sales_router, prefix="/api/v1")
router.include_router(recs_router, prefix="/api/v1")
router.include_router(cache_router, prefix="/api/v1")
