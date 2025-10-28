"""Cache management schemas."""

from __future__ import annotations

from pydantic import BaseModel


class PurgePayload(BaseModel):
    """Payload for selective cache purge operations."""

    sales: bool = False
    recs: bool = False
