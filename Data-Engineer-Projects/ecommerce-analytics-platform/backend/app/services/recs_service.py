"""Business logic layer for recommendations with ALS integration and caching."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import structlog

from backend.app.config import get_settings
from backend.app.core.cache import RedisCache, build_cache_key, get_namespace
from backend.app.core.db import SessionLocal
from backend.app.repositories.recs_repo import RecsRepository
from ml.serve import AlsRecommender


class RecsService:
    """Singleton thread-safe service for recommendations with ALS integration."""

    _instance: RecsService | None = None
    _lock = asyncio.Lock()
    _recommender: AlsRecommender | None = None
    _model_version: str | None = None

    def __new__(cls, *args, **kwargs) -> RecsService:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, cache: RedisCache) -> None:
        if not hasattr(self, "_initialized"):
            self._cache = cache
            self._logger = structlog.get_logger(__name__)
            self._initialized = True

    async def load_latest_if_needed(self) -> tuple[AlsRecommender, str]:
        """Load the latest recommender if not already loaded."""
        async with self._lock:
            if self._recommender is None:
                self._recommender, self._model_version = await self._load_recommender()
            return self._recommender, self._model_version

    async def refresh(self) -> tuple[AlsRecommender, str]:
        """Force reload the latest recommender."""
        async with self._lock:
            self._recommender, self._model_version = await self._load_recommender()
            return self._recommender, self._model_version

    async def _load_recommender(self) -> tuple[AlsRecommender, str]:
        """Load the recommender from the latest artifact."""
        settings = get_settings()
        recommender = await asyncio.to_thread(
            AlsRecommender.load_latest, settings.recs_artifact_dir
        )
        # Compute model version as the latest artifact directory name
        base_dir = Path(settings.recs_artifact_dir)
        candidates = [
            subdir
            for subdir in base_dir.iterdir()
            if subdir.is_dir() and (subdir / "model.json").exists()
        ]
        if candidates:
            candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            model_version = candidates[0].name
        else:
            model_version = "unknown"
        return recommender, model_version

    async def cache_key_user(
        self, user_id: int, k: int, exclude_seen: bool, strategy: str
    ) -> str:
        """Generate cache key for user recommendations."""
        settings = get_settings()
        ns = await get_namespace(settings.CACHE_NAMESPACE_RECS_KEY)
        return build_cache_key(
            ns,
            "recs:user",
            user_id=user_id,
            k=k,
            exclude_seen=exclude_seen,
            strategy=strategy,
        )

    async def cache_key_similar(self, sku: str, k: int) -> str:
        """Generate cache key for similar products."""
        settings = get_settings()
        ns = await get_namespace(settings.CACHE_NAMESPACE_RECS_KEY)
        return build_cache_key(ns, "recs:similar", sku=sku, k=k)

    async def recommend_for_user(
        self, user_id: int, k: int, exclude_seen: bool, strategy: str
    ) -> tuple[list[dict[str, Any]], str]:
        """Get recommendations for user with ALS or fallback, excluding seen products."""
        cache_key = await self.cache_key_user(user_id, k, exclude_seen, strategy)

        # Try cache first
        cached = None
        try:
            cached = await self._cache.get_json(cache_key)
        except RuntimeError as e:
            self._logger.warning("Redis unavailable, skipping cache", error=str(e))

        if cached:
            self._logger.debug("Cache hit for user recs", cache_key=cache_key)
            return cached["items"], cached["model_version"]

        # Cache miss, load recommender
        recommender, model_version = await self.load_latest_if_needed()

        # Get ALS recommendations (exclude_seen handled below)
        recs = await asyncio.to_thread(
            recommender.recommend_for_user, user_id, k, exclude_seen=False
        )

        if exclude_seen:
            # Get session for repository operations
            db = SessionLocal()
            try:
                repo = RecsRepository(db)
                seen = repo.get_user_seen_product_ids(user_id, limit=1000)
                filtered_recs = [(pid, score) for pid, score in recs if pid not in seen]
                if len(filtered_recs) < k:
                    # Fallback to popular products
                    popular_pids = recommender.fallback_popular(
                        k - len(filtered_recs), strategy
                    )
                    existing_pids = {pid for pid, _ in filtered_recs}
                    popular_pids = [
                        pid
                        for pid in popular_pids
                        if pid not in seen and pid not in existing_pids
                    ]
                    filtered_recs.extend([(pid, 0.0) for pid in popular_pids])
                recs = filtered_recs[:k]

                # Map to product details
                product_ids = [pid for pid, _ in recs]
                rows = repo.map_product_ids_to_rows(product_ids)
            finally:
                db.close()

        items = []
        for row, (pid, score) in zip(rows, recs, strict=False):
            reason = "als" if score > 0 else "popular"
            items.append(
                {
                    "sku": row["sku"],
                    "name": row["name"],
                    "category": row["category"],
                    "score": score,
                    "reason": reason,
                }
            )

        # Cache result
        settings = get_settings()
        try:
            await self._cache.set_json(
                cache_key,
                {"items": items, "model_version": model_version},
                settings.recs_cache_ttl_seconds,
            )
            if settings.cache_selective_enabled:
                from backend.app.core.cache import index_key

                await index_key(f"recs:user:{user_id}", cache_key)
        except RuntimeError as e:
            self._logger.warning(
                "Redis unavailable, skipping cache write", error=str(e)
            )

        return items, model_version

    async def similar_for_sku(
        self, sku: str, k: int
    ) -> tuple[list[dict[str, Any]], str]:
        """Get similar products for a SKU."""
        cache_key = await self.cache_key_similar(sku, k)

        # Try cache first
        cached = None
        try:
            cached = await self._cache.get_json(cache_key)
        except RuntimeError as e:
            self._logger.warning("Redis unavailable, skipping cache", error=str(e))

        if cached:
            self._logger.debug("Cache hit for similar", cache_key=cache_key)
            return cached["items"], cached["model_version"]

        # Get session for repository operations
        db = SessionLocal()
        try:
            repo = RecsRepository(db)

            # Get product ID
            product_id = repo.map_sku_to_product_id(sku)
            if product_id is None:
                return [], "unknown"

            # Load recommender
            recommender, model_version = await self.load_latest_if_needed()

            # Get similar products
            recs = await asyncio.to_thread(recommender.similar_products, product_id, k)

            # Map to product details
            product_ids = [pid for pid, _ in recs]
            rows = repo.map_product_ids_to_rows(product_ids)
        finally:
            db.close()

        items = []
        for row, (pid, score) in zip(rows, recs, strict=False):
            items.append(
                {
                    "sku": row["sku"],
                    "name": row["name"],
                    "category": row["category"],
                    "score": score,
                    "reason": "similarity",
                }
            )

        # Cache result
        settings = get_settings()
        try:
            await self._cache.set_json(
                cache_key,
                {"items": items, "model_version": model_version},
                settings.recs_cache_ttl_seconds,
            )
            if settings.cache_selective_enabled:
                from backend.app.core.cache import index_key

                await index_key(f"recs:sku:{sku}", cache_key)
        except RuntimeError as e:
            self._logger.warning(
                "Redis unavailable, skipping cache write", error=str(e)
            )

        return items, model_version
