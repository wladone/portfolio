import pytest
from airflow.models import DagBag


def test_dag_loads():
    """Test that the DAG loads without errors"""
    dagbag = DagBag(dag_folder='dags', include_examples=False)
    dag = dagbag.get_dag('fashion_retail_etl_pipeline_v2')

    assert dag is not None
    assert len(dag.tasks) > 0
    assert dag.dag_id == 'fashion_retail_etl_pipeline_v2'


def test_dag_tasks():
    """Test that all required tasks are present"""
    dagbag = DagBag(dag_folder='dags', include_examples=False)
    dag = dagbag.get_dag('fashion_retail_etl_pipeline_v2')

    expected_tasks = [
        'start',
        'ingest.wait_products',
        'ingest.wait_transactions',
        'ensure_objects',
        'ingest.load_products_stg',
        'ingest.load_transactions_stg',
        'dq_branch',
        'transform.transform_products',
        'transform.transform_sales',
        'dq_validate',
        'generate_recommendations',
        'notify_success',
        'notify_failure',
        'end'
    ]

    task_ids = [task.task_id for task in dag.tasks]
    for expected_task in expected_tasks:
        assert expected_task in task_ids, f"Task {expected_task} not found in DAG"


def test_dag_dependencies():
    """Test that task dependencies are correct"""
    dagbag = DagBag(dag_folder='dags', include_examples=False)
    dag = dagbag.get_dag('fashion_retail_etl_pipeline_v2')

    # Check some key dependencies
    start_task = dag.get_task('start')
    wait_products = dag.get_task('ingest.wait_products')
    wait_transactions = dag.get_task('ingest.wait_transactions')

    # Start should have downstream to wait tasks
    assert wait_products in start_task.downstream_list
    assert wait_transactions in start_task.downstream_list

    # End should be the final task
    end_task = dag.get_task('end')
    assert len(end_task.downstream_list) == 0
