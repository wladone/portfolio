"""Schemas for authentication endpoints."""

from __future__ import annotations

from pydantic import BaseModel, Field


class TokenResponse(BaseModel):
    """Response payload for issued access tokens."""

    access_token: str = Field(description="JWT access token.")
    token_type: str = Field(default="bearer", description="Token type, always bearer.")
    expires_in: int = Field(description="Token lifetime in seconds.")
