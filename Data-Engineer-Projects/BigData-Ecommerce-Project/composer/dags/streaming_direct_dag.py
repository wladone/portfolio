"""Airflow DAG that launches the streaming pipeline directly on Dataflow."""
from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataflow import BeamRunPythonPipelineOperator

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

import config  # noqa: E402


def _get_setting(name: str, default: Any | None = None, *, required: bool = False) -> str:
    env_value = os.getenv(name)
    if env_value:
        return env_value
    if required:
        return Variable.get(name)  # raises if missing
    return Variable.get(name, default_var=default)


def _topic(project_id: str, topic_name: str) -> str:
    return f"projects/{project_id}/topics/{topic_name}"


PROJECT_ID = _get_setting("PROJECT_ID", required=True)
REGION = _get_setting("REGION", config.DEFAULT_REGION)
DATAFLOW_BUCKET = _get_setting("DATAFLOW_BUCKET", required=True)
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

PIPELINE_FILE = "dags/beam/streaming_pipeline.py"

_pipeline_options: Dict[str, Any] = {
    "project": PROJECT_ID,
    "region": REGION,
    "tempLocation": f"gs://{DATAFLOW_BUCKET}/tmp",
    "stagingLocation": f"gs://{DATAFLOW_BUCKET}/staging",
    "runner": "DataflowRunner",
    "streaming": True,
    "save_main_session": True,
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
    dag_id="streaming_direct_dag",
    description="Runs the Beam streaming pipeline on Dataflow using BeamRunPythonPipelineOperator.",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["dataflow", "beam", "ecommerce"],
    default_args={"owner": "data-eng", "depends_on_past": False},
) as dag:
    run_dataflow = BeamRunPythonPipelineOperator(
        task_id="run_streaming_pipeline",
        py_file=PIPELINE_FILE,
        runner="DataflowRunner",
        py_requirements=[
            "apache-beam[gcp]==2.48.0",
            "google-cloud-bigtable==2.19.0",
        ],
        py_interpreter="python3",
        pipeline_options=_pipeline_options,
    )

    run_dataflow
