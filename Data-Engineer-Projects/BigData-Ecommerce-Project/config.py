"""Runtime configuration for the BigData Ecommerce streaming pipeline."""
from __future__ import annotations

import os
import re
from typing import Iterable


def _env(name: str, default: str) -> str:
    """Return environment variable value or the provided default."""
    value = os.getenv(name)
    if value is None:
        return default
    stripped = value.strip()
    return default if stripped == "" else stripped


def _int_env(name: str, default: int) -> int:
    """Return integer environment variable value with fallback."""
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    try:
        return int(value)
    except ValueError as exc:  # pragma: no cover - validation guardrail
        raise ValueError(f"{name} must be an int, got {value!r}") from exc


PROJECT_ID = _env("PROJECT_ID", "demo-project")
DATAFLOW_BUCKET = _env("DATAFLOW_BUCKET", "demo-dataflow-bucket")
COMPOSER_ENV = _env("COMPOSER_ENV", "demo-composer-env")
TEMPLATE_PATH = _env(
    "TEMPLATE_PATH",
    f"gs://{DATAFLOW_BUCKET}/templates/streaming_pipeline_flex_template.json",
)

DATASET_NAME = _env("DATASET_NAME", "ecommerce")
VIEWS_TABLE = _env("VIEWS_TABLE", "product_views_summary")
SALES_TABLE = _env("SALES_TABLE", "sales_summary")
STOCK_TABLE = _env("STOCK_TABLE", "inventory_summary")

VIEWS_WINDOW_SECONDS = _int_env("VIEWS_WINDOW_SECONDS", 60)
AGGREGATION_WINDOW_SECONDS = _int_env("AGGREGATION_WINDOW_SECONDS", 300)

BIGTABLE_INSTANCE = _env("BIGTABLE_INSTANCE", "local-bigtable")
BIGTABLE_TABLE_DEFAULT = _env("BIGTABLE_TABLE_DEFAULT", "product_stats")
BIGTABLE_TABLE = _env("BIGTABLE_TABLE", BIGTABLE_TABLE_DEFAULT)
BIGTABLE_COLUMN_FAMILY = _env("BIGTABLE_COLUMN_FAMILY", "stats")
BIGTABLE_VIEW_COLUMN = _env("BIGTABLE_VIEW_COLUMN", "view_count")

TOPIC_CLICKS = _env("TOPIC_CLICKS", "clicks")
TOPIC_TRANSACTIONS = _env("TOPIC_TRANSACTIONS", "transactions")
TOPIC_STOCK = _env("TOPIC_STOCK", "stock")
TOPIC_DLQ = _env("TOPIC_DLQ", "dead-letter")
DLQ_SUBSCRIPTION = _env("DLQ_SUBSCRIPTION", "dead-letter-sub")

DEFAULT_REGION = _env("REGION", "europe-central2")
METRICS_NAMESPACE = _env("METRICS_NAMESPACE", "ecommerce_pipeline")

_DISALLOWED_CHARS = set("\n\r\t;&|`$")
_BQ_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,1023}$")
_TOPIC_IDENTIFIER = re.compile(r"^[A-Za-z][-A-Za-z0-9_]{2,255}$")
_BIGTABLE_TABLE_ID = re.compile(r"^[A-Za-z0-9_.-]{1,50}$")
_COLUMN_FAMILY = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
_PROJECT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]{5,29}$")
_BUCKET_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{2,62}$")
_COMPOSER_ENV_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{2,63}$")


def _ensure_safe(name: str, value: str, *, pattern: re.Pattern[str]) -> None:
    if value is None:
        raise ValueError(f"{name} must be set")
    if any(char in _DISALLOWED_CHARS for char in value):
        raise ValueError(f"{name} contains illegal characters: {value!r}")
    if not pattern.fullmatch(value):
        raise ValueError(f"{name} has invalid format: {value!r}")


def validate_configuration() -> None:
    """Validate static configuration values for safe defaults."""
    _ensure_safe("PROJECT_ID", PROJECT_ID, pattern=_PROJECT_ID_PATTERN)
    _ensure_safe("DATAFLOW_BUCKET", DATAFLOW_BUCKET, pattern=_BUCKET_PATTERN)
    _ensure_safe("COMPOSER_ENV", COMPOSER_ENV, pattern=_COMPOSER_ENV_PATTERN)
    _ensure_safe("DATASET_NAME", DATASET_NAME, pattern=_BQ_IDENTIFIER)
    for label, table in {
        "VIEWS_TABLE": VIEWS_TABLE,
        "SALES_TABLE": SALES_TABLE,
        "STOCK_TABLE": STOCK_TABLE,
    }.items():
        _ensure_safe(label, table, pattern=_BQ_IDENTIFIER)
    _ensure_safe("BIGTABLE_INSTANCE", BIGTABLE_INSTANCE, pattern=_PROJECT_ID_PATTERN)
    _ensure_safe("BIGTABLE_TABLE", BIGTABLE_TABLE, pattern=_BIGTABLE_TABLE_ID)
    _ensure_safe("BIGTABLE_COLUMN_FAMILY", BIGTABLE_COLUMN_FAMILY, pattern=_COLUMN_FAMILY)
    for label, topic in {
        "TOPIC_CLICKS": TOPIC_CLICKS,
        "TOPIC_TRANSACTIONS": TOPIC_TRANSACTIONS,
        "TOPIC_STOCK": TOPIC_STOCK,
        "TOPIC_DLQ": TOPIC_DLQ,
    }.items():
        _ensure_safe(label, topic, pattern=_TOPIC_IDENTIFIER)


def validate_bigquery_targets(dataset: str, tables: Iterable[str]) -> None:
    """Validate dataset and table identifiers supplied at runtime."""
    _ensure_safe("dataset", dataset, pattern=_BQ_IDENTIFIER)
    for table in tables:
        _ensure_safe("table", table, pattern=_BQ_IDENTIFIER)


def validate_pubsub_topics(topics: Iterable[str]) -> None:
    """Validate topic IDs (without the project prefix)."""
    for topic in topics:
        _ensure_safe("topic", topic, pattern=_TOPIC_IDENTIFIER)


def validate_bigtable(table: str, column_family: str | None = None) -> None:
    """Validate Bigtable identifiers."""
    _ensure_safe("bigtable_table", table, pattern=_BIGTABLE_TABLE_ID)
    if column_family is not None:
        _ensure_safe("bigtable_column_family", column_family, pattern=_COLUMN_FAMILY)


__all__ = [
    "PROJECT_ID",
    "DATAFLOW_BUCKET",
    "COMPOSER_ENV",
    "TEMPLATE_PATH",
    "DATASET_NAME",
    "VIEWS_TABLE",
    "SALES_TABLE",
    "STOCK_TABLE",
    "VIEWS_WINDOW_SECONDS",
    "AGGREGATION_WINDOW_SECONDS",
    "BIGTABLE_INSTANCE",
    "BIGTABLE_TABLE_DEFAULT",
    "BIGTABLE_TABLE",
    "BIGTABLE_COLUMN_FAMILY",
    "BIGTABLE_VIEW_COLUMN",
    "TOPIC_CLICKS",
    "TOPIC_TRANSACTIONS",
    "TOPIC_STOCK",
    "TOPIC_DLQ",
    "DLQ_SUBSCRIPTION",
    "DEFAULT_REGION",
    "METRICS_NAMESPACE",
    "validate_configuration",
    "validate_bigquery_targets",
    "validate_pubsub_topics",
    "validate_bigtable",
]
