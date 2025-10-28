"""ETL configuration using Pydantic settings."""

from __future__ import annotations

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ETLSettings(BaseSettings):
    """Settings for ETL jobs."""

    model_config = SettingsConfigDict(
        env_file=(".env", ".env.example"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = Field(alias="DATABASE_URL")
    app_log_level: str = Field(default="INFO", alias="APP_LOG_LEVEL")
    prometheus_namespace: str = Field(
        default="ecom_analytics", alias="PROMETHEUS_NAMESPACE"
    )
    default_chunk_size: int = Field(default=5000, alias="ETL_CHUNK_SIZE")
    ensure_dim_date: bool = Field(default=False, alias="ETL_ENSURE_DIM_DATE")
    dry_run: bool = Field(default=False, alias="ETL_DRY_RUN")
    pushgateway_url: str | None = Field(default=None, alias="PUSHGATEWAY_URL")
    cdc_strategy_customers: Literal["watermark", "hash"] = Field(
        default="watermark", alias="CDC_STRATEGY_CUSTOMERS"
    )
    cdc_strategy_products: Literal["watermark", "hash"] = Field(
        default="watermark", alias="CDC_STRATEGY_PRODUCTS"
    )
    cdc_ts_field_customers: str = Field(
        default="updated_at", alias="CDC_TS_FIELD_CUSTOMERS"
    )
    cdc_ts_field_products: str = Field(
        default="updated_at", alias="CDC_TS_FIELD_PRODUCTS"
    )
    cdc_ts_field_orders: str = Field(
        default="transaction_ts", alias="CDC_TS_FIELD_ORDERS"
    )
    cdc_default_watermark_offset_days: int = Field(
        default=0, alias="CDC_DEFAULT_WATERMARK_OFFSET_DAYS"
    )
    cdc_batch_size: int = Field(default=5000, alias="CDC_BATCH_SIZE")
    cache_invalidate_on_success: bool = Field(
        default=True, alias="CACHE_INVALIDATE_ON_SUCCESS"
    )
    cache_invalidation_strategy: Literal["namespace", "selective"] = Field(
        default="namespace", alias="CACHE_INVALIDATION_STRATEGY"
    )


settings = ETLSettings()  # type: ignore[call-arg]


def get_settings() -> ETLSettings:
    """Return ETL settings."""
    return settings
