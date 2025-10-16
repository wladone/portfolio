"""Airflow DAG that launches the streaming pipeline via a Dataflow Flex Template."""
from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

import config  # noqa: E402


def _get_setting(name: str, default: Any | None = None, *, required: bool = False) -> str:
    env_value = os.getenv(name)
    if env_value:
        return env_value
    if required:
        return Variable.get(name)
    return Variable.get(name, default_var=default)


def _topic(project_id: str, topic_name: str) -> str:
    return f"projects/{project_id}/topics/{topic_name}"


PROJECT_ID = _get_setting("PROJECT_ID", required=True)
REGION = _get_setting("REGION", config.DEFAULT_REGION)
DATAFLOW_BUCKET = _get_setting("DATAFLOW_BUCKET", required=True)
TEMPLATE_PATH = _get_setting(
    "DATAFLOW_FLEX_TEMPLATE",
    f"gs://{DATAFLOW_BUCKET}/templates/streaming_pipeline_flex_template.json",
)
BIGTABLE_INSTANCE = _get_setting("BIGTABLE_INSTANCE", required=True)
BIGTABLE_TABLE = _get_setting("BIGTABLE_TABLE", config.BIGTABLE_TABLE)
DATASET_NAME = _get_setting("DATASET_NAME", config.DATASET_NAME)
VIEWS_TABLE = _get_setting("VIEWS_TABLE", config.VIEWS_TABLE)
SALES_TABLE = _get_setting("SALES_TABLE", config.SALES_TABLE)
STOCK_TABLE = _get_setting("STOCK_TABLE", config.STOCK_TABLE)
TOPIC_CLICKS = _get_setting("TOPIC_CLICKS", config.TOPIC_CLICKS)
TOPIC_TRANSACTIONS = _get_setting("TOPIC_TRANSACTIONS", config.TOPIC_TRANSACTIONS)
TOPIC_STOCK = _get_setting("TOPIC_STOCK", config.TOPIC_STOCK)
TOPIC_DLQ = _get_setting("TOPIC_DLQ", config.TOPIC_DLQ)

_parameters: Dict[str, Any] = {
    "project": PROJECT_ID,
    "region": REGION,
    "input_topic_clicks": _topic(PROJECT_ID, TOPIC_CLICKS),
    "input_topic_transactions": _topic(PROJECT_ID, TOPIC_TRANSACTIONS),
    "input_topic_stock": _topic(PROJECT_ID, TOPIC_STOCK),
    "dead_letter_topic": _topic(PROJECT_ID, TOPIC_DLQ),
    "output_bigquery_dataset": DATASET_NAME,
    "output_table_views": VIEWS_TABLE,
    "output_table_sales": SALES_TABLE,
    "output_table_stock": STOCK_TABLE,
    "bigtable_project": PROJECT_ID,
    "bigtable_instance": BIGTABLE_INSTANCE,
    "bigtable_table": BIGTABLE_TABLE,
}

with DAG(
    dag_id="streaming_flex_dag",
    description="Runs the Beam streaming pipeline using a Dataflow Flex Template.",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["dataflow", "beam", "flex-template", "ecommerce"],
    default_args={"owner": "data-eng", "depends_on_past": False},
) as dag:
    launch_flex_template = DataflowTemplatedJobStartOperator(
        task_id="launch_streaming_flex_template",
        job_name="streaming-flex-{{ ds_nodash }}",
        template=TEMPLATE_PATH,
        location=REGION,
        project_id=PROJECT_ID,
        parameters=_parameters,
    )

    launch_flex_template
