"""Cache invalidation service with Pub/Sub listener."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import structlog
from redis.asyncio import Redis

from backend.app.config import get_settings
from backend.app.core.cache import bump_namespace, delete_many, keys_for_index
from backend.app.metrics import CACHE_INVALIDATIONS


class CacheInvalidator:
    """Background service for cache invalidation via Redis Pub/Sub."""

    def __init__(self, redis_client: Redis) -> None:
        self.redis = redis_client
        self.logger = structlog.get_logger(__name__)
        self.settings = get_settings()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """Start the Pub/Sub listener as a background task."""
        if self._task is not None:
            self.logger.warning("Cache invalidator already running")
            return

        self._task = asyncio.create_task(self._listen())
        self.logger.info("Cache invalidator started")

    async def stop(self) -> None:
        """Stop the Pub/Sub listener."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
            self.logger.info("Cache invalidator stopped")

    async def _listen(self) -> None:
        """Listen for cache invalidation messages on Pub/Sub channel."""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(self.settings.cache_pubsub_channel)

        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    await self._handle_message(message["data"])
        except asyncio.CancelledError:
            await pubsub.unsubscribe(self.settings.cache_pubsub_channel)
            raise
        except Exception as e:
            self.logger.error("Error in cache invalidator", error=str(e))
            raise

    async def _handle_message(self, data: bytes) -> None:
        """Parse and handle cache invalidation message."""
        try:
            payload = json.loads(data.decode("utf-8"))
            target = payload.get("target")
            strategy = payload.get("strategy")
            message_payload = payload.get("payload", {})

            self.logger.info(
                "Received cache invalidation message",
                target=target,
                strategy=strategy,
                payload=message_payload,
            )

            if strategy == "namespace":
                await self._invalidate_namespace(target)
            elif strategy == "selective" and self.settings.cache_selective_enabled:
                await self._invalidate_selective(target, message_payload)
            else:
                self.logger.warning(
                    "Unsupported invalidation strategy or selective disabled",
                    strategy=strategy,
                    selective_enabled=self.settings.cache_selective_enabled,
                )

            CACHE_INVALIDATIONS.inc()

        except json.JSONDecodeError as e:
            self.logger.error(
                "Invalid JSON in cache invalidation message", error=str(e)
            )
        except Exception as e:
            self.logger.error("Error handling cache invalidation message", error=str(e))

    async def _invalidate_namespace(self, target: str) -> None:
        """Invalidate cache by bumping namespace version."""
        if target == "sales":
            await bump_namespace(self.settings.cache_namespace_sales_key)
            self.logger.info("Bumped sales namespace")
        elif target == "recs":
            await bump_namespace(self.settings.cache_namespace_recs_key)
            self.logger.info("Bumped recs namespace")
        else:
            self.logger.warning("Unknown namespace target", target=target)

    async def _invalidate_selective(self, target: str, payload: dict[str, Any]) -> None:
        """Invalidate specific cache keys based on payload."""
        indexes = self._derive_indexes(target, payload)
        for index in indexes:
            try:
                keys = await keys_for_index(index)
                if keys:
                    deleted = await delete_many(keys)
                    self.logger.info(
                        "Invalidated selective cache",
                        index=index,
                        keys_deleted=deleted,
                    )
            except Exception as e:
                self.logger.error(
                    "Error invalidating selective cache", index=index, error=str(e)
                )

    def _derive_indexes(self, target: str, payload: dict[str, Any]) -> list[str]:
        """Derive affected indexes from target and payload."""
        indexes = []

        if target == "sales":
            # Channel index
            channel = payload.get("channel") or "all"
            indexes.append(f"sales:channel:{channel}")

            # Date indexes
            from_date = payload.get("from")
            to_date = payload.get("to")
            if from_date and to_date:
                # Assuming dates are in YYYY-MM-DD format
                # For simplicity, add day indexes (can be optimized)
                # In real implementation, might need date parsing
                indexes.append(f"sales:day:{from_date.replace('-', '')}")
                indexes.append(f"sales:day:{to_date.replace('-', '')}")

        elif target == "recs":
            # User or SKU indexes
            user_id = payload.get("user_id")
            sku = payload.get("sku")
            if user_id:
                indexes.append(f"recs:user:{user_id}")
            if sku:
                indexes.append(f"recs:sku:{sku}")

        return indexes
