"""Pydantic models for recommendations API."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, conint

from backend.app.config import settings


class UserRecsParams(BaseModel):
    """Request parameters for user recommendations."""

    k: conint(gt=0, le=200) = settings.recs_topk_default
    exclude_seen: bool = settings.recs_exclude_seen_default
    fallback_strategy: Literal["items", "net"] = "items"


class SimilarParams(BaseModel):
    """Request parameters for similar items."""

    k: conint(gt=0, le=200) = settings.recs_topk_default


class RecItem(BaseModel):
    """Individual recommendation item."""

    sku: str
    name: str | None
    category: str | None
    score: float
    reason: Literal["als", "popular", "similarity"]


class UserRecsResponse(BaseModel):
    """Response model for user recommendations."""

    model_config = ConfigDict(arbitrary_types_allowed=True, from_attributes=True)

    user_id: int
    k: int
    model_version: str
    items: list[RecItem]


class SimilarResponse(BaseModel):
    """Response model for similar items."""

    model_config = ConfigDict(arbitrary_types_allowed=True, from_attributes=True)

    sku: str
    k: int
    model_version: str
    items: list[RecItem]
