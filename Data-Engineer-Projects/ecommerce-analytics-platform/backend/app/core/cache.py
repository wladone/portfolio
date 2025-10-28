"""Cache utilities with namespace and selective indexing support."""

from __future__ import annotations

from redis.asyncio import Redis

from backend.app.config import get_settings

settings = get_settings()
redis_client = Redis.from_url(settings.redis_url)


class RedisCache:
    """Redis cache implementation with async operations."""

    def __init__(self, redis_url: str):
        self._client = Redis.from_url(redis_url)

    async def _get_client(self):
        """Get the Redis client instance."""
        return self._client

    async def get(self, key: str) -> str | None:
        """Get value from cache."""
        return await self._client.get(key)

    async def set(self, key: str, value: str, ttl: int | None = None) -> bool:
        """Set value in cache with optional TTL."""
        return await self._client.set(key, value, ex=ttl)

    async def delete(self, key: str) -> int:
        """Delete key from cache."""
        return await self._client.delete(key)

    async def exists(self, key: str) -> bool:
        """Check if key exists in cache."""
        return await self._client.exists(key) > 0


async def get_namespace(key: str) -> int:
    """Get namespace version from Redis, defaulting to 1 if not found."""
    value = await redis_client.get(key)
    return int(value) if value else 1


async def bump_namespace(key: str) -> int:
    """Atomically increment namespace version and return new value."""
    return await redis_client.incr(key)


def build_cache_key(namespace: int, prefix: str, **params) -> str:
    """Build canonical cache key from namespace, prefix, and sorted params."""
    param_parts = [f"{k}={v}" for k, v in sorted(params.items())]
    param_str = ":".join(param_parts) if param_parts else ""
    return f"{namespace}:{prefix}:{param_str}"


async def delete_many(keys: list[str]) -> int:
    """Batch delete keys with CACHE_MAX_DELETE_BATCH limit, return total deleted."""
    batch_size = settings.cache_max_delete_batch
    deleted = 0
    for i in range(0, len(keys), batch_size):
        batch = keys[i : i + batch_size]
        deleted += await redis_client.delete(*batch)
    return deleted


async def index_key(index: str, cache_key: str):
    """Add cache key to Redis SET for selective indexing."""
    await redis_client.sadd(index, cache_key)


async def keys_for_index(index: str) -> list[str]:
    """Get all cache keys for a selective index."""
    members = await redis_client.smembers(index)
    return list(members)


async def delete_index(index: str):
    """Delete the entire selective index SET."""
    await redis_client.delete(index)
