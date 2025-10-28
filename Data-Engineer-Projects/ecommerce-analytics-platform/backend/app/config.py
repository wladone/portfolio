"""Application configuration powered by pydantic settings."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """Central application configuration loaded from environment variables or `.env`."""

    model_config = SettingsConfigDict(
        env_file=(".env", ".env.example"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: str = Field(default="dev", alias="APP_ENV")
    app_log_level: str = Field(default="INFO", alias="APP_LOG_LEVEL")
    database_url: str = Field(alias="DATABASE_URL")
    redis_url: str = Field(alias="REDIS_URL")
    minio_endpoint: str = Field(alias="MINIO_ENDPOINT")
    minio_access_key: str = Field(alias="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(alias="MINIO_SECRET_KEY")
    jwt_secret: str = Field(alias="JWT_SECRET")
    jwt_algorithm: str = Field(default="HS256", alias="JWT_ALG")
    prometheus_namespace: str = Field(
        default="ecom_analytics", alias="PROMETHEUS_NAMESPACE"
    )
    cors_allow_origins: list[str] = Field(
        default_factory=lambda: [
            "http://localhost:3000",
            "http://localhost:8000",
        ],
        alias="CORS_ALLOW_ORIGINS",
    )
    sales_cache_ttl_seconds: int = Field(default=60)
    rate_limit_enabled: bool = Field(default=True)
    rate_limit_requests: int = Field(default=120)
    rate_limit_window_seconds: int = Field(default=60)
    recs_artifact_dir: str = Field(default="ml/artifacts", alias="RECS_ARTIFACT_DIR")
    recs_cache_ttl_seconds: int = Field(default=60, alias="RECS_CACHE_TTL_SECONDS")
    recs_topk_default: int = Field(default=10, alias="RECS_TOPK_DEFAULT")
    recs_exclude_seen_default: bool = Field(
        default=True, alias="RECS_EXCLUDE_SEEN_DEFAULT"
    )
    recs_allow_refresh_endpoint: bool = Field(
        default=True, alias="RECS_ALLOW_REFRESH_ENDPOINT"
    )

    # Authentication / RBAC
    auth_require_auth: bool = Field(default=False, alias="AUTH_REQUIRE_AUTH")
    auth_dev_users_enabled: bool = Field(default=True, alias="AUTH_DEV_USERS_ENABLED")
    auth_access_token_expires_seconds: int = Field(
        default=3600, alias="AUTH_ACCESS_TOKEN_EXPIRES_SECONDS"
    )

    # Cache invalidation
    cache_invalidation_strategy: Literal["namespace", "selective"] = Field(
        default="namespace", alias="CACHE_INVALIDATION_STRATEGY"
    )
    cache_namespace_sales_key: str = Field(
        default="ns:sales", alias="CACHE_NAMESPACE_SALES_KEY"
    )
    cache_namespace_recs_key: str = Field(
        default="ns:recs", alias="CACHE_NAMESPACE_RECS_KEY"
    )
    cache_pubsub_channel: str = Field(
        default="cache:invalidate", alias="CACHE_PUBSUB_CHANNEL"
    )
    cache_selective_enabled: bool = Field(
        default=False, alias="CACHE_SELECTIVE_ENABLED"
    )
    cache_max_delete_batch: int = Field(default=1000, alias="CACHE_MAX_DELETE_BATCH")

    @field_validator("cors_allow_origins", mode="before")
    @classmethod
    def parse_cors_origins(cls, value: object) -> list[str]:
        """Parse comma-separated origins into a list."""
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            return [origin.strip() for origin in value.split(",") if origin.strip()]
        return ["http://localhost:3000", "http://localhost:8000"]


settings = AppSettings()  # type: ignore[call-arg]


def get_settings() -> AppSettings:
    """Return the cached application settings."""
    return settings
