"""Middleware for injecting a correlation ID into each request."""

from __future__ import annotations

import time
import uuid

import structlog
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp
from structlog.contextvars import bind_contextvars, clear_contextvars

CORRELATION_ID_HEADER = "X-Correlation-ID"
REQUEST_ID_HEADER = "X-Request-ID"
_logger = structlog.get_logger(__name__)


def _generate_correlation_id() -> str:
    """Return a new correlation identifier."""
    return uuid.uuid4().hex


def _generate_request_id() -> str:
    """Return a new request identifier."""
    return uuid.uuid4().hex


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Attach a correlation ID to every request for traceability."""

    def __init__(self, app: ASGIApp, header_name: str = CORRELATION_ID_HEADER) -> None:
        super().__init__(app)
        self._header_name = header_name

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        start_time = time.time()
        correlation_id = request.headers.get(
            self._header_name, _generate_correlation_id()
        )
        request_id = _generate_request_id()
        bind_contextvars(correlation_id=correlation_id, request_id=request_id)
        _logger.debug(
            "correlation_id_set",
            correlation_id=correlation_id,
            request_id=request_id,
            path=request.url.path,
        )

        try:
            response = await call_next(request)
        finally:
            clear_contextvars()

        duration_ms = (time.time() - start_time) * 1000
        response.headers[self._header_name] = correlation_id
        response.headers[REQUEST_ID_HEADER] = request_id
        _logger.info(
            "request_completed",
            correlation_id=correlation_id,
            request_id=request_id,
            client_host=request.client.host if request.client else None,
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            duration_ms=round(duration_ms, 2),
        )
        return response
