"""Middleware for rate limiting using token bucket algorithm."""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict

from fastapi import HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response
from starlette.types import ASGIApp

from ..config import get_settings


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Rate limiting middleware using token bucket algorithm."""

    def __init__(self, app: ASGIApp) -> None:
        super().__init__(app)
        self._buckets: dict[str, dict[str, float]] = defaultdict(
            lambda: {"tokens": 0.0, "last_refill": time.time()}
        )
        self._lock = asyncio.Lock()
        self._settings = get_settings()

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        if not self._settings.rate_limit_enabled:
            return await call_next(request)

        ip = self._get_client_ip(request)
        async with self._lock:
            bucket = self._buckets[ip]
            self._refill_tokens(bucket)
            if bucket["tokens"] < 1:
                raise HTTPException(status_code=429, detail="Rate limit exceeded")
            bucket["tokens"] -= 1

        return await call_next(request)

    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP from X-Forwarded-For header or client.host."""
        x_forwarded_for = request.headers.get("X-Forwarded-For")
        if x_forwarded_for:
            # Take the first IP in case of multiple
            return x_forwarded_for.split(",")[0].strip()
        return request.client.host if request.client else "unknown"

    def _refill_tokens(self, bucket: dict[str, float]) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - bucket["last_refill"]
        refill_rate = (
            self._settings.rate_limit_requests
            / self._settings.rate_limit_window_seconds
        )
        tokens_to_add = elapsed * refill_rate
        bucket["tokens"] = min(
            self._settings.rate_limit_requests, bucket["tokens"] + tokens_to_add
        )
        bucket["last_refill"] = now
