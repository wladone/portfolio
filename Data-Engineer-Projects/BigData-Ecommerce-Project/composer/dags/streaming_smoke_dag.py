"""Airflow smoke DAG validating streaming pipeline sinks."""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

import config  # noqa: E402


def _get_setting(name: str, default: str | None = None, *, required: bool = False) -> str:
    env_value = os.getenv(name)
    if env_value:
        return env_value
    if required:
        return Variable.get(name)
    return Variable.get(name, default_var=default)


PROJECT_ID = _get_setting("PROJECT_ID", required=True)
REGION = _get_setting("REGION", config.DEFAULT_REGION)
DATASET_NAME = _get_setting("DATASET_NAME", config.DATASET_NAME)
VIEWS_TABLE = _get_setting("VIEWS_TABLE", config.VIEWS_TABLE)
BIGTABLE_INSTANCE = _get_setting("BIGTABLE_INSTANCE", required=True)
BIGTABLE_TABLE = _get_setting("BIGTABLE_TABLE", config.BIGTABLE_TABLE)
BIGTABLE_COLUMN_FAMILY = _get_setting("BIGTABLE_COLUMN_FAMILY", config.BIGTABLE_COLUMN_FAMILY)

RECENT_WINDOW_MINUTES = int(os.getenv("SMOKE_WINDOW_MINUTES", "60"))


def _check_bigtable() -> None:
    from google.cloud import bigtable

    client = bigtable.Client(project=PROJECT_ID, admin=True)
    instance = client.instance(BIGTABLE_INSTANCE)
    table = instance.table(BIGTABLE_TABLE)
    if not table.exists():
        raise ValueError(f"Bigtable table {BIGTABLE_TABLE} not found in instance {BIGTABLE_INSTANCE}")
    column_families = table.list_column_families()
    if BIGTABLE_COLUMN_FAMILY not in column_families:
        raise ValueError(
            f"Column family {BIGTABLE_COLUMN_FAMILY} missing from Bigtable table {BIGTABLE_TABLE}"
        )


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    dag_id="streaming_smoke_dag",
    description="Validates downstream sinks for the streaming pipeline (BigQuery + Bigtable).",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    render_template_as_native_obj=True,
    tags=["dataflow", "quality", "smoke"],
) as dag:
    check_recent_views = BigQueryCheckOperator(
        task_id="check_recent_views",
        sql=f"""
        SELECT
          COUNTIF(window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {RECENT_WINDOW_MINUTES} MINUTE)) > 0
        FROM `{PROJECT_ID}.{DATASET_NAME}.{VIEWS_TABLE}`
        """,
        use_legacy_sql=False,
        location=REGION,
    )

    validate_bigtable = PythonOperator(
        task_id="validate_bigtable_schema",
        python_callable=_check_bigtable,
    )

    check_recent_views >> validate_bigtable
