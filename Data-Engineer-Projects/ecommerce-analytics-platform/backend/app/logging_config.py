"""Configure structured JSON logging for the backend."""

from __future__ import annotations

import logging
import os
import sys

import structlog
from structlog.typing import Processor


def redact_sensitive(logger: str, method_name: str, event_dict: dict) -> dict:
    """Redact sensitive information from log entries."""
    import copy
    import re

    SENSITIVE_FIELDS = re.compile(
        r"(?i)(password|secret|token|key|auth)", re.IGNORECASE
    )

    output = copy.deepcopy(event_dict)

    for key, value in event_dict.items():
        # Skip event/log_level
        if key in ("event", "log_level"):
            continue

        # Redact sensitive keys
        if SENSITIVE_FIELDS.search(key):
            if isinstance(value, str) and len(value) > 8:
                output[key] = f"{value[:4]}...{value[-4:]}"
            else:
                output[key] = "***"

        # Handle nested dicts
        elif isinstance(value, dict):
            output[key] = redact_sensitive(logger, method_name, value)

    return output


def configure_logging(level: str = "INFO") -> None:
    """Initialize structlog and standard logging with JSON output."""

    processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        redact_sensitive,
        structlog.processors.JSONRenderer(),
    ]

    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(message)s",
        stream=sys.stdout,
    )


def bind_context(correlation_id: str, request_id: str) -> None:
    """Bind common context variables for logging."""
    structlog.contextvars.bind_contextvars(
        correlation_id=correlation_id,
        request_id=request_id,
        service="ecom-api",
        env=os.getenv("APP_ENV", "dev"),
    )
