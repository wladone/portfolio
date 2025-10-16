from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule
import os

# Conditional imports for Snowflake
if os.getenv('BACKEND', 'duckdb') == 'snowflake':
    from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
    from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator

# Default arguments
default_args = {
    'owner': 'fashion_retail',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# DAG definition
dag = DAG(
    'fashion_retail_etl_pipeline_v2',
    default_args=default_args,
    description='Fashion Retail ETL Pipeline',
    schedule_interval='0 2 * * *',  # Daily at 02:00 UTC
    max_active_runs=1,
    catchup=False,
    tags=['fashion', 'retail', 'etl'],
)

# Environment variables
BACKEND = os.getenv('BACKEND', 'duckdb')  # 'duckdb' or 'snowflake'

# Task definitions
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Wait for products file
wait_products = S3KeySensor(
    task_id='ingest.wait_products',
    bucket_name='fashion-retail-data-lake',
    bucket_key=f"raw/products/{{{{ ds_nodash }}}}/products_{{{{ ds_nodash }}}}.csv{{{{ '.gz' if os.getenv('GZIP_FILES', 'false').lower() == 'true' else '' }}}}",
    aws_conn_id='s3_retail_conn',
    timeout=3600,
    poke_interval=60,
    dag=dag,
)

# Wait for transactions file
wait_transactions = S3KeySensor(
    task_id='ingest.wait_transactions',
    bucket_name='fashion-retail-data-lake',
    bucket_key=f"raw/transactions/{{{{ ds_nodash }}}}/transactions_{{{{ ds_nodash }}}}.json{{{{ '.gz' if os.getenv('GZIP_FILES', 'false').lower() == 'true' else '' }}}}",
    aws_conn_id='s3_retail_conn',
    timeout=3600,
    poke_interval=60,
    dag=dag,
)

# Ensure objects (create schema/tables)
if BACKEND == 'duckdb':
    def ensure_objects():
        from scripts.load_to_duckdb import create_schema_duckdb
        create_schema_duckdb()

    ensure_objects_task = PythonOperator(
        task_id='ensure_objects',
        python_callable=ensure_objects,
        dag=dag,
    )
elif BACKEND == 'snowflake':
    ensure_objects_task = SnowflakeOperator(
        task_id='ensure_objects',
        snowflake_conn_id='snowflake_retail_conn',
        sql='dags/sql/create_schema_snowflake.sql',
        dag=dag,
    )

# Load products staging
if BACKEND == 'duckdb':
    def load_products_stg():
        from scripts.load_to_duckdb import load_products_to_duckdb
        load_products_to_duckdb('{{ ds_nodash }}')

    load_products_stg_task = PythonOperator(
        task_id='ingest.load_products_stg',
        python_callable=load_products_stg,
        dag=dag,
    )
elif BACKEND == 'snowflake':
    load_products_stg_task = S3ToSnowflakeOperator(
        task_id='ingest.load_products_stg',
        s3_keys=[
            f"raw/products/{{{{ ds_nodash }}}}/products_{{{{ ds_nodash }}}}.csv{{{{ '.gz' if os.getenv('GZIP_FILES', 'false').lower() == 'true' else '' }}}}"],
        table='stg_products',
        stage='fashion_retail_stage',
        file_format='csv_format',
        snowflake_conn_id='snowflake_retail_conn',
        dag=dag,
    )

# Load transactions staging
if BACKEND == 'duckdb':
    def load_transactions_stg():
        from scripts.load_to_duckdb import load_transactions_to_duckdb
        load_transactions_to_duckdb('{{ ds_nodash }}')

    load_transactions_stg_task = PythonOperator(
        task_id='ingest.load_transactions_stg',
        python_callable=load_transactions_stg,
        dag=dag,
    )
elif BACKEND == 'snowflake':
    load_transactions_stg_task = S3ToSnowflakeOperator(
        task_id='ingest.load_transactions_stg',
        s3_keys=[
            f"raw/transactions/{{{{ ds_nodash }}}}/transactions_{{{{ ds_nodash }}}}.json{{{{ '.gz' if os.getenv('GZIP_FILES', 'false').lower() == 'true' else '' }}}}"],
        table='stg_transactions',
        stage='fashion_retail_stage',
        file_format='json_format',
        snowflake_conn_id='snowflake_retail_conn',
        dag=dag,
    )

# DQ Branch


def dq_branch():
    # Simple check, can be expanded
    return 'transform.transform_products'


dq_branch_task = BranchPythonOperator(
    task_id='dq_branch',
    python_callable=dq_branch,
    dag=dag,
)

# Transform products
if BACKEND == 'duckdb':
    transform_products_task = SQLExecuteQueryOperator(
        task_id='transform.transform_products',
        conn_id='duckdb_retail_conn',
        sql='dags/sql/transform_products.sql',
        dag=dag,
    )
elif BACKEND == 'snowflake':
    transform_products_task = SnowflakeOperator(
        task_id='transform.transform_products',
        snowflake_conn_id='snowflake_retail_conn',
        sql='dags/sql/transform_products.sql',
        dag=dag,
    )

# Transform sales
if BACKEND == 'duckdb':
    transform_sales_task = SQLExecuteQueryOperator(
        task_id='transform.transform_sales',
        conn_id='duckdb_retail_conn',
        sql='dags/sql/transform_transactions.sql',
        dag=dag,
    )
elif BACKEND == 'snowflake':
    transform_sales_task = SnowflakeOperator(
        task_id='transform.transform_sales',
        snowflake_conn_id='snowflake_retail_conn',
        sql='dags/sql/transform_transactions.sql',
        dag=dag,
    )

# DQ Validate


def dq_validate():
    from scripts.validate_data_quality import validate_data_quality
    validate_data_quality(BACKEND)


dq_validate_task = PythonOperator(
    task_id='dq_validate',
    python_callable=dq_validate,
    dag=dag,
)

# Generate recommendations
if BACKEND == 'duckdb':
    generate_recommendations_task = SQLExecuteQueryOperator(
        task_id='generate_recommendations',
        conn_id='duckdb_retail_conn',
        sql='''
        INSERT INTO product_recommendations (region, product_id, total_quantity, rank)
        SELECT
            region,
            product_id,
            total_quantity,
            DENSE_RANK() OVER (PARTITION BY region ORDER BY total_quantity DESC) as rank
        FROM (
            SELECT
                region,
                product_id,
                SUM(quantity) as total_quantity
            FROM sales_fact
            WHERE sale_date >= date('now', '-30 days')
            GROUP BY region, product_id
        ) t
        WHERE rank <= 10;
        ''',
        dag=dag,
    )
elif BACKEND == 'snowflake':
    generate_recommendations_task = SnowflakeOperator(
        task_id='generate_recommendations',
        snowflake_conn_id='snowflake_retail_conn',
        sql='''
        INSERT INTO product_recommendations (region, product_id, total_quantity, rank)
        SELECT
            region,
            product_id,
            total_quantity,
            DENSE_RANK() OVER (PARTITION BY region ORDER BY total_quantity DESC) as rank
        FROM (
            SELECT
                region,
                product_id,
                SUM(quantity) as total_quantity
            FROM sales_fact
            WHERE sale_date >= DATEADD(day, -30, CURRENT_DATE)
            GROUP BY region, product_id
        ) t
        WHERE rank <= 10;
        ''',
        dag=dag,
    )

# Notifications
notify_success = EmailOperator(
    task_id='notify_success',
    to='alerts@fashionretail.com',
    subject='Fashion Retail ETL Pipeline Success',
    html_content='Pipeline completed successfully for {{ ds }}',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

notify_failure = EmailOperator(
    task_id='notify_failure',
    to='alerts@fashionretail.com',
    subject='Fashion Retail ETL Pipeline Failure',
    html_content='Pipeline failed for {{ ds }}',
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

# Task dependencies
start >> [wait_products, wait_transactions]
[wait_products, wait_transactions] >> ensure_objects_task
ensure_objects_task >> [load_products_stg_task, load_transactions_stg_task]
[load_products_stg_task, load_transactions_stg_task] >> dq_branch_task
dq_branch_task >> [transform_products_task, transform_sales_task]
[transform_products_task, transform_sales_task] >> dq_validate_task
dq_validate_task >> generate_recommendations_task
generate_recommendations_task >> [notify_success, notify_failure] >> end
