"""Application entrypoint for the FastAPI service."""

from __future__ import annotations

import asyncio
import logging

import structlog
import uvicorn
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import PrometheusFastApiInstrumentator

from .api.router import router as api_router
from .config import get_settings
from .core.cache import RedisCache, redis_client
from .core.db import SessionLocal
from .logging_config import configure_logging
from .middleware import CorrelationIdMiddleware, RateLimitMiddleware
from .obs.cache_invalidator import CacheInvalidator
from .services.recs_service import RecsService
from .streaming.orders_worker import OrdersWorker
from .streaming.settings import StreamingSettings

configure_logging(get_settings().app_log_level)
logger = structlog.get_logger(__name__)


async def startup_event():
    """Load recommendations model, start cache invalidator, and initialize streaming integration on startup."""
    settings = get_settings()
    db = SessionLocal()
    try:
        cache = RedisCache(settings.redis_url)
        service = RecsService(cache)  # Don't pass session to singleton
        _, model_version = await service.load_latest_if_needed()
        logger.info("recommendations_model_loaded", model_version=model_version)
    finally:
        db.close()

    # Start cache invalidator
    invalidator = CacheInvalidator(redis_client)
    await invalidator.start()
    logger.info("cache_invalidator_started")

    # Initialize streaming integration
    streaming_settings = StreamingSettings()
    orders_worker = OrdersWorker(streaming_settings)
    asyncio.create_task(orders_worker.start([streaming_settings.kafka_topic_orders]))
    logger.info(
        "streaming_integration_started", topic=streaming_settings.kafka_topic_orders
    )


def create_application() -> FastAPI:
    """Instantiate the FastAPI application with routers and middleware."""
    settings = get_settings()
    app = FastAPI(
        title="E-commerce Analytics API",
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )
    app.state.settings = settings

    # Add startup event
    app.add_event_handler("startup", startup_event)

    # Add Prometheus metrics middleware
    PrometheusFastApiInstrumentator(
        should_group_status_codes=False,
        should_ignore_untemplated=True,
        should_group_untemplated=False,
        should_round_latency_decimals=True,
        excluded_handlers=["/docs", "/redoc", "/openapi.json"],
        should_instrument_requests_inprogress=True,
        inprogress_name="requests_in_progress",
        inprogress_labels=True,
    ).instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)

    app.add_middleware(RateLimitMiddleware)
    app.add_middleware(CorrelationIdMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allow_origins,
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "X-Correlation-ID"],
        expose_headers=["X-Correlation-ID"],
    )

    app.include_router(api_router)

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        correlation_id = request.headers.get("X-Correlation-ID")
        is_auth_error = exc.status_code in (
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,
        )
        error_label = (
            "unauthorized"
            if exc.status_code == status.HTTP_401_UNAUTHORIZED
            else (
                "forbidden"
                if exc.status_code == status.HTTP_403_FORBIDDEN
                else exc.detail
            )
        )
        logger.error(
            "http_error",
            correlation_id=correlation_id,
            status=exc.status_code,
            path=request.url.path,
            detail=error_label if is_auth_error else exc.detail,
        )
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": error_label,
                "correlation_id": correlation_id,
            },
        )

    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        correlation_id = request.headers.get("X-Correlation-ID")
        logger.error(
            "unhandled_exception",
            correlation_id=correlation_id,
            status=500,
            path=request.url.path,
            detail=str(exc),
        )
        return JSONResponse(
            status_code=500,
            content={
                "error": "Internal server error",
                "correlation_id": correlation_id,
            },
        )

    logger.info(
        "application_startup", env=settings.app_env, log_level=settings.app_log_level
    )
    return app


app = create_application()


def run_dev_server() -> None:
    """Run the FastAPI development server."""
    uvicorn.run(
        "backend.app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level=logging.getLevelName(logging.INFO),
    )
