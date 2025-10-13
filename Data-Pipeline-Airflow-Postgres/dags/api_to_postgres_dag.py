from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import psycopg2
import os

def extract_data():
    url = "https://jsonplaceholder.typicode.com/posts"
    data = requests.get(url, timeout=30).json()
    df = pd.DataFrame(data)
    os.makedirs("/tmp/etl", exist_ok=True)
    df.to_csv("/tmp/etl/posts.csv", index=False)

def load_data():
    conn = psycopg2.connect(
        host="postgres",
        database="airflow_db",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()
    df = pd.read_csv("/tmp/etl/posts.csv")
    for _, row in df.iterrows():
        cur.execute(
            "INSERT INTO posts (id, title, body) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (int(row['id']), str(row['title']), str(row['body']))
        )
    conn.commit()
    cur.close()
    conn.close()

with DAG(
    "api_to_postgres",
    start_date=datetime(2025, 8, 13),
    schedule_interval="@daily",
    catchup=False
) as dag:
    t1 = PythonOperator(task_id="extract", python_callable=extract_data)
    t2 = PythonOperator(task_id="load", python_callable=load_data)
    t1 >> t2
