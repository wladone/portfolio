from datetime import datetime
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
}

with DAG(
    dag_id="retail_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["retail","etl","spark","bigquery"],
) as dag:

    extract_products = BashOperator(
        task_id="extract_products_api",
        bash_command="python ${AIRFLOW_HOME}/dags/../src/extract_products_api.py",
    )

    extract_customers = BashOperator(
        task_id="extract_customers_mysql",
        bash_command="python ${AIRFLOW_HOME}/dags/../src/extract_customers_mysql.py",
    )

    transform_and_load = BashOperator(
        task_id="transform_to_bigquery",
        bash_command=(
            "spark-submit --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.3 "
            "${AIRFLOW_HOME}/dags/../spark/transform_to_bq.py"
        ),
        env=os.environ.copy(),
    )

    [extract_products, extract_customers] >> transform_and_load

