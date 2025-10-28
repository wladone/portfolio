"""Settings for end-to-end tests."""

from __future__ import annotations

import os
from datetime import date
from typing import Any

from pydantic import AnyUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class E2ESettings(BaseSettings):
    """Runtime configuration for the E2E runner."""

    model_config = SettingsConfigDict(
        env_prefix="E2E_",
        env_file=(".env", ".env.example"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    api_base_url: AnyUrl = Field(default="http://localhost:8000")
    database_url: str | None = None
    seed_start_date: date = Field(default=date(2023, 1, 1))
    seed_end_date: date = Field(default_factory=date.today)
    ensure_dim_date: bool = True
    recs_topk: int = 10
    timeout_seconds: int = 30
    wait_api_seconds: int = 60
    lookback_days: int = 365

    def model_post_init(self, __context: Any) -> None:  # type: ignore[override]
        if not self.database_url:
            self.database_url = (
                os.getenv("E2E_DATABASE_URL")
                or os.getenv("DATABASE_URL")
                or "postgresql+psycopg://app:app_password@localhost:5432/ecom"
            )


def get_settings() -> E2ESettings:
    """Return cached E2E settings."""
    return E2ESettings()
