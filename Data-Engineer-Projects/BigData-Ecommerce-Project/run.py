from __future__ import annotations

import os
import re
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from typing import Sequence

import config

config.validate_configuration()

_IDENTIFIER_GUARD = set("\n\r\t;&|`$")
_PROJECT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]{5,29}$")
_BIGTABLE_INSTANCE_PATTERN = re.compile(r"^[a-z][-a-z0-9]{5,32}$")


def _validate_identifier(name: str, value: str, pattern: re.Pattern[str]) -> None:
    if value is None:
        raise ValueError(f"Missing value for {name}")
    if any(char in _IDENTIFIER_GUARD for char in value):
        raise ValueError(f"{name} contains unsupported characters: {value!r}")
    if not pattern.fullmatch(value):
        raise ValueError(f"{name} has invalid format: {value!r}")


def _validate_runtime_configuration(project_id: str, bigtable_instance: str, bigtable_table: str) -> None:
    _validate_identifier("PROJECT_ID", project_id, _PROJECT_ID_PATTERN)
    _validate_identifier("BIGTABLE_INSTANCE", bigtable_instance, _BIGTABLE_INSTANCE_PATTERN)
    config.validate_bigquery_targets(
        config.DATASET_NAME,
        [config.VIEWS_TABLE, config.SALES_TABLE, config.STOCK_TABLE],
    )
    config.validate_pubsub_topics(
        [config.TOPIC_CLICKS, config.TOPIC_TRANSACTIONS, config.TOPIC_STOCK, config.TOPIC_DLQ]
    )
    config.validate_bigtable(bigtable_table, config.BIGTABLE_COLUMN_FAMILY)


def env(name: str, default: str | None = None, required: bool = False) -> str:
    """Return environment variable value with optional default/required semantics."""
    value = os.environ.get(name)
    if value is None or str(value).strip() == "":
        if required:
            print(f"Missing required env: {name}")
            sys.exit(2)
        return default if default is not None else ""
    return str(value)


def shell(args: Sequence[str]) -> None:
    """Execute a command using subprocess with an argument list."""
    command = list(args)
    printable = " ".join(shlex.quote(str(arg)) for arg in command)
    print(printable)
    subprocess.run(command, check=True)


def _topic_path(project_id: str, topic: str) -> str:
    return f"projects/{project_id}/topics/{topic}"


def run_local() -> None:
    project_id = env("PROJECT_ID", config.PROJECT_ID)
    region = env("REGION", config.DEFAULT_REGION)
    bigtable_instance = env("BIGTABLE_INSTANCE", config.BIGTABLE_INSTANCE)
    bigtable_table = env("BIGTABLE_TABLE", config.BIGTABLE_TABLE)
    _validate_runtime_configuration(project_id, bigtable_instance, bigtable_table)

    args = [
        sys.executable,
        "beam/streaming_pipeline.py",
        "--runner=DirectRunner",
        "--streaming=True",
        f"--project={project_id}",
        f"--region={region}",
        f"--input_topic_clicks={_topic_path(project_id, config.TOPIC_CLICKS)}",
        f"--input_topic_transactions={_topic_path(project_id, config.TOPIC_TRANSACTIONS)}",
        f"--input_topic_stock={_topic_path(project_id, config.TOPIC_STOCK)}",
        f"--dead_letter_topic={_topic_path(project_id, config.TOPIC_DLQ)}",
        f"--output_bigquery_dataset={config.DATASET_NAME}",
        f"--output_table_views={config.VIEWS_TABLE}",
        f"--output_table_sales={config.SALES_TABLE}",
        f"--output_table_stock={config.STOCK_TABLE}",
        f"--bigtable_project={project_id}",
        f"--bigtable_instance={bigtable_instance}",
        f"--bigtable_table={bigtable_table}",
    ]

    shell(args)


def run_flex() -> None:
    project_id = env("PROJECT_ID", config.PROJECT_ID)
    region = env("REGION", config.DEFAULT_REGION)
    bucket = env("DATAFLOW_BUCKET", config.DATAFLOW_BUCKET)
    template = env(
        "TEMPLATE_PATH",
        f"gs://{bucket}/templates/streaming_pipeline_flex_template.json",
    )
    bigtable_instance = env("BIGTABLE_INSTANCE", config.BIGTABLE_INSTANCE)
    bigtable_table = env("BIGTABLE_TABLE", config.BIGTABLE_TABLE)
    _validate_runtime_configuration(project_id, bigtable_instance, bigtable_table)
    job = f"streaming-flex-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

    params = [
        ("project", project_id),
        ("region", region),
        ("tempLocation", f"gs://{bucket}/tmp"),
        ("stagingLocation", f"gs://{bucket}/staging"),
        ("streaming", "true"),
        ("save_main_session", "true"),
        ("input_topic_clicks", _topic_path(project_id, config.TOPIC_CLICKS)),
        ("input_topic_transactions", _topic_path(project_id, config.TOPIC_TRANSACTIONS)),
        ("input_topic_stock", _topic_path(project_id, config.TOPIC_STOCK)),
        ("dead_letter_topic", _topic_path(project_id, config.TOPIC_DLQ)),
        ("output_bigquery_dataset", config.DATASET_NAME),
        ("output_table_views", config.VIEWS_TABLE),
        ("output_table_sales", config.SALES_TABLE),
        ("output_table_stock", config.STOCK_TABLE),
        ("bigtable_project", project_id),
        ("bigtable_instance", bigtable_instance),
        ("bigtable_table", bigtable_table),
    ]

    param_arg = ",".join(f"{key}={value}" for key, value in params)
    args = [
        "gcloud",
        "dataflow",
        "flex-template",
        "run",
        job,
        f"--template-file-gcs-location={template}",
        "--region",
        region,
        "--parameters",
        param_arg,
    ]

    shell(args)


def run_direct() -> None:
    composer_env = env("COMPOSER_ENV", config.COMPOSER_ENV)
    region = env("REGION", config.DEFAULT_REGION)

    args = [
        "gcloud",
        "composer",
        "environments",
        "run",
        composer_env,
        "--location",
        region,
        "dags",
        "trigger",
        "streaming_direct_dag",
    ]

    shell(args)


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "local"
    if mode == "local":
        run_local()
    elif mode == "flex":
        run_flex()
    elif mode == "direct":
        run_direct()
    else:
        print("Usage: python run.py [local|flex|direct]")
        sys.exit(1)


if __name__ == "__main__":
    main()
