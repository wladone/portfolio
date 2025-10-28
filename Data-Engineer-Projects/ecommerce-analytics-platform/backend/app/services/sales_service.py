"""Business logic layer for sales analytics."""

from __future__ import annotations

import asyncio
from datetime import date, timedelta

import structlog
from sqlalchemy import func, select

from backend.app.config import get_settings
from backend.app.core.cache import RedisCache, get_namespace
from backend.app.models.dim_date import DimDate
from backend.app.repositories.sales_repo import SalesRepository
from backend.app.schemas.sales import (
    SalesSummaryParams,
    SalesSummaryResponse,
    TopProductsParams,
    TopProductsResponse,
)

settings = get_settings()
if settings.cache_selective_enabled:
    from backend.app.core.cache import index_key
else:
    index_key = None


class SalesService:
    """Service layer for sales analytics with caching."""

    def __init__(self, session, cache: RedisCache) -> None:
        self._session = session
        self._cache = cache
        self._repo = SalesRepository(session)
        self._logger = structlog.get_logger(__name__)

    def normalize_date_range(
        self, from_: date | None, to: date | None
    ) -> tuple[date, date]:
        """Normalize date range with defaults and caps to valid dim_date range."""
        # Get valid date range from dim_date
        stmt = select(func.min(DimDate.date), func.max(DimDate.date))
        result = self._session.execute(stmt).first()
        min_date, max_date = result

        if from_ is None and to is None:
            # Default to last 30 days
            to = date.today()
            from_ = to - timedelta(days=30)
        elif from_ is None:
            from_ = to - timedelta(days=30)
        elif to is None:
            to = from_ + timedelta(days=30)

        # Cap to valid range
        from_ = max(from_, min_date)
        to = min(to, max_date)

        return from_, to

    async def cache_key_for(self, endpoint: str, **params) -> str:
        """Generate stable cache key for endpoint and params with namespace."""
        ns = await get_namespace(settings.CACHE_NAMESPACE_SALES_KEY)
        key_parts = ["sales", endpoint]
        for k, v in sorted(params.items()):
            if isinstance(v, date):
                v = v.isoformat()
            key_parts.append(str(v))
        return f"{ns}:{':'.join(key_parts)}"

    async def get_sales_summary(
        self, params: SalesSummaryParams
    ) -> SalesSummaryResponse:
        """Get sales summary with caching."""
        from_, to = self.normalize_date_range(params.from_, params.to)

        cache_key = await self.cache_key_for(
            "summary",
            from_=from_,
            to=to,
            channel=params.channel,
            granularity=params.granularity,
        )

        # Try cache first
        cached = None
        try:
            cached = await self._cache.get_json(cache_key)
        except RuntimeError as e:
            self._logger.warning("Redis unavailable, skipping cache", error=str(e))

        if cached:
            self._logger.debug("Cache hit for sales summary", cache_key=cache_key)
            return SalesSummaryResponse(**cached)

        # Cache miss, fetch from repo
        self._logger.debug("Cache miss for sales summary", cache_key=cache_key)
        rows = await asyncio.to_thread(
            self._repo.fetch_sales_summary,
            from_,
            to,
            params.channel,
            params.granularity,
        )

        response = SalesSummaryResponse(
            rows=rows,
            from_=from_,
            to=to,
            channel=params.channel,
            granularity=params.granularity,
        )

        # Cache result
        settings = get_settings()
        try:
            await self._cache.set_json(
                cache_key, response.model_dump(), settings.sales_cache_ttl_seconds
            )
        except RuntimeError as e:
            self._logger.warning(
                "Redis unavailable, skipping cache write", error=str(e)
            )

        # Index keys if selective caching enabled
        if settings.cache_selective_enabled:
            indexes = []
            # Channel index
            channel_code = params.channel or "all"
            indexes.append(f"sales:channel:{channel_code}")
            # Date indexes based on granularity
            if params.granularity == "day":
                current = from_
                while current <= to:
                    indexes.append(f"sales:day:{current.strftime('%Y%m%d')}")
                    current += timedelta(days=1)
            elif params.granularity == "month":
                current = from_.replace(day=1)
                while current <= to:
                    indexes.append(f"sales:month:{current.strftime('%Y%m')}")
                    # Next month
                    if current.month == 12:
                        current = current.replace(year=current.year + 1, month=1)
                    else:
                        current = current.replace(month=current.month + 1)
            # Index the key
            for index in indexes:
                try:
                    await index_key(index, cache_key)
                except RuntimeError as e:
                    self._logger.warning(
                        "Redis unavailable, skipping index write", error=str(e)
                    )

        return response

    async def get_top_products(self, params: TopProductsParams) -> TopProductsResponse:
        """Get top products with caching."""
        from_, to = self.normalize_date_range(params.from_, params.to)

        cache_key = await self.cache_key_for(
            "top_products",
            from_=from_,
            to=to,
            channel=params.channel,
            metric=params.metric,
            limit=params.limit,
            offset=params.offset,
        )

        # Try cache first
        cached = None
        try:
            cached = await self._cache.get_json(cache_key)
        except RuntimeError as e:
            self._logger.warning("Redis unavailable, skipping cache", error=str(e))

        if cached:
            self._logger.debug("Cache hit for top products", cache_key=cache_key)
            return TopProductsResponse(**cached)

        # Cache miss, fetch from repo
        self._logger.debug("Cache miss for top products", cache_key=cache_key)
        rows, total = await asyncio.to_thread(
            self._repo.fetch_top_products,
            from_,
            to,
            params.channel,
            params.metric,
            params.limit,
            params.offset,
        )

        response = TopProductsResponse(
            rows=rows,
            metric=params.metric,
            limit=params.limit,
            offset=params.offset,
            total=total,
        )

        # Cache result
        settings = get_settings()
        try:
            await self._cache.set_json(
                cache_key, response.model_dump(), settings.sales_cache_ttl_seconds
            )
        except RuntimeError as e:
            self._logger.warning(
                "Redis unavailable, skipping cache write", error=str(e)
            )

        # Index keys if selective caching enabled
        if settings.cache_selective_enabled:
            indexes = []
            # Channel index
            channel_code = params.channel or "all"
            indexes.append(f"sales:channel:{channel_code}")
            # Date indexes based on date range (top products doesn't have granularity, so use day range)
            current = from_
            while current <= to:
                indexes.append(f"sales:day:{current.strftime('%Y%m%d')}")
                current += timedelta(days=1)
            # Index the key
            for index in indexes:
                try:
                    await index_key(index, cache_key)
                except RuntimeError as e:
                    self._logger.warning(
                        "Redis unavailable, skipping index write", error=str(e)
                    )

        return response
