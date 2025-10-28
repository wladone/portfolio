"""Cache management endpoints."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from backend.app.core.cache import bump_namespace, delete_many, keys_for_index
from backend.app.schemas.cache import PurgePayload
from backend.app.security.dependencies import require_roles

router = APIRouter(prefix="/cache", tags=["cache"])

AdminGuard = Annotated[None, Depends(require_roles("admin"))]


@router.post(
    "/_bump/{target}",
    status_code=status.HTTP_200_OK,
    summary="Bump cache namespace",
    description="Increment the namespace version for the specified target to invalidate cached data.",
    responses={
        200: {
            "description": "Namespace bumped successfully",
            "content": {"application/json": {"example": {"namespace": 2}}},
        },
        400: {"description": "Invalid target"},
        403: {"description": "Insufficient permissions"},
        500: {"description": "Internal server error"},
    },
)
async def bump_cache_namespace(
    target: str,
    _: AdminGuard,
) -> dict[str, int]:
    """Bump the cache namespace for the given target."""
    if target not in {"sales", "recs"}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid target. Must be 'sales' or 'recs'.",
        )

    namespace_key = f"namespace:{target}"
    new_version = await bump_namespace(namespace_key)
    return {"namespace": new_version}


@router.post(
    "/_purge",
    status_code=status.HTTP_200_OK,
    summary="Purge cache selectively",
    description="Purge cached data for sales and/or recommendations based on the payload.",
    responses={
        200: {
            "description": "Cache purged successfully",
            "content": {"application/json": {"example": {"deleted": 42}}},
        },
        403: {"description": "Insufficient permissions"},
        500: {"description": "Internal server error"},
    },
)
async def purge_cache(
    payload: PurgePayload,
    _: AdminGuard,
) -> dict[str, int]:
    """Purge cache keys for the specified targets."""
    keys_to_delete = []

    if payload.sales:
        sales_keys = await keys_for_index("sales")
        keys_to_delete.extend(sales_keys)

    if payload.recs:
        recs_keys = await keys_for_index("recs")
        keys_to_delete.extend(recs_keys)

    deleted = await delete_many(keys_to_delete)
    return {"deleted": deleted}
