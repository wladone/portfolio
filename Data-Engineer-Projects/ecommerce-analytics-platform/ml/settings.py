"""Configuration for ML pipelines."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MLSettings(BaseSettings):
    """Settings for ALS training and serving."""

    model_config = SettingsConfigDict(
        env_file=(".env", ".env.example"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = Field(alias="DATABASE_URL")
    redis_url: str = Field(alias="REDIS_URL")
    artifact_path: Path = Field(default=Path("ml/artifacts"))
    als_factors: int = 64
    als_reg: float = 0.02
    als_iter: int = 15
    als_alpha: float = 40.0
    als_seed: int = 1337
    als_min_purchases_per_user: int = 3
    als_min_purchases_per_item: int = 5
    als_lookback_days: int | None = None
    als_artifact_dir: str = "ml/artifacts"
    topk_default: int = 10


settings = MLSettings()  # type: ignore[call-arg]


def get_settings() -> MLSettings:
    """Return ML settings."""
    return settings
