"""Cache invalidation utilities for ETL jobs."""

from __future__ import annotations

import json
from datetime import date
from typing import Any

import structlog
from redis.asyncio import Redis
from sqlalchemy.orm import Session

from backend.app.config import get_settings
from backend.app.models.cache_event import CacheEvent

from ..db import session_scope
from ..settings import get_settings as get_etl_settings

logger = structlog.get_logger(__name__)


async def publish_invalidation(
    target: str,
    strategy: str,
    payload: dict[str, Any] | None = None,
    redis_client: Redis | None = None,
) -> None:
    """Publish cache invalidation message to Redis Pub/Sub and persist to meta.cache_events.

    Args:
        target: The cache target (e.g., "sales", "recs")
        strategy: The invalidation strategy ("namespace" or "selective")
        payload: Additional payload data for selective invalidation
        redis_client: Optional Redis client, defaults to global client
    """
    app_settings = get_settings()
    etl_settings = get_etl_settings()

    if not etl_settings.cache_invalidate_on_success:
        logger.info("Cache invalidation disabled", target=target)
        return

    if strategy not in ["namespace", "selective"]:
        raise ValueError(f"Unsupported strategy: {strategy}")

    message = {
        "target": target,
        "strategy": strategy,
        "payload": payload or {},
    }

    # Publish to Redis Pub/Sub
    if redis_client is None:
        from backend.app.core.cache import redis_client as default_client

        redis_client = default_client

    await redis_client.publish(
        app_settings.cache_pubsub_channel, json.dumps(message, default=str)
    )

    # Persist to database
    with session_scope() as session:
        _persist_cache_event(session, target, strategy, payload or {})

    logger.info(
        "Published cache invalidation",
        target=target,
        strategy=strategy,
        payload=payload,
    )


def _persist_cache_event(
    session: Session,
    event_type: str,
    strategy: str,
    payload: dict[str, Any],
) -> None:
    """Persist cache invalidation event to meta.cache_events table."""
    event_payload = {
        "strategy": strategy,
        **payload,
    }

    cache_event = CacheEvent(
        event_type=event_type,
        payload=event_payload,
    )
    session.add(cache_event)
    session.commit()


def collect_orders_invalidation_payload(
    processed_records: list[dict[str, Any]],
) -> dict[str, Any]:
    """Collect date range and channels from processed order records.

    Args:
        processed_records: List of processed order records

    Returns:
        Payload dict with 'from', 'to', and 'channels' keys
    """
    if not processed_records:
        return {}

    dates = []
    channels = set()

    for record in processed_records:
        # Extract transaction date
        txn_ts = record.get("transaction_ts")
        if txn_ts:
            if isinstance(txn_ts, str):
                # Assume ISO format, extract date part
                dates.append(txn_ts.split("T")[0])
            elif hasattr(txn_ts, "date"):
                dates.append(txn_ts.date().isoformat())
            elif isinstance(txn_ts, date):
                dates.append(txn_ts.isoformat())

        # Extract channel
        channel = record.get("channel_code")
        if channel:
            channels.add(channel)

    payload = {}
    if dates:
        payload["from"] = min(dates)
        payload["to"] = max(dates)
    if channels:
        payload["channels"] = list(channels)

    return payload
