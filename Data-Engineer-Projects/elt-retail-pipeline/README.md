# ETL Retail Pipeline — End‑to‑End (API + CSV + MySQL → PySpark → BigQuery → Airflow)

Acest repo oferă un starter kit robust pentru un pipeline end‑to‑end de retail:
- Extract: API produse, CSV vânzări, MySQL clienți.
- Transform (PySpark): join + QC.
- Load (BigQuery): tabele dimensiune și fact.
- Orchestrare (Airflow): job zilnic.

## Setup
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
mkdir -p staging
```

**Spark BigQuery Connector** (local):

```bash
spark-submit \
  --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.3 \
  spark/transform_to_bq.py
```

## Flow rapid

1. Extract:

```bash
python src/extract_products_api.py
python src/extract_customers_mysql.py
# plasați/refresh vanzari.csv în staging/ (sau copiați din data-samples)
```

2. Transform & Load (Spark → BigQuery):

```bash
export GOOGLE_APPLICATION_CREDENTIALS=$(grep GOOGLE_APPLICATION_CREDENTIALS .env | cut -d= -f2)
export PYSPARK_PYTHON=$(which python)
spark-submit --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.3 \
    spark/transform_to_bq.py
```

3. (Opțional) Orchestrare cu Airflow:

```bash
airflow db init
airflow users create --username admin --password admin --role Admin --email you@example.com
airflow webserver -D && airflow scheduler -D
# puneți dags/retail_etl_dag.py în $AIRFLOW_HOME/dags/
```

## Note

* `STAGING_DIR` (default `staging`) controlează căile locale.
* Pentru BigQuery: `GCP_PROJECT_ID`, `BQ_DATASET`, `GCS_TEMP_BUCKET` + `GOOGLE_APPLICATION_CREDENTIALS` (service account JSON).
* Pe Databricks: importați `spark/transform_to_bq.py` într-un notebook/Job, atașați connectorul BigQuery și setați `temporaryGcsBucket`.

```
```

