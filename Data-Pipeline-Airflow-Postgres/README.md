# ⚙️ Data Pipeline with Apache Airflow & PostgreSQL
**Author:** Vlad Gabriel Popescu  
**Role:** Junior Data Engineer / Data Analyst  

![Status](https://img.shields.io/badge/status-active-brightgreen)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.3-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-ready-blue)

## 📌 Description
Daily ETL pipeline that extracts data from a public API, transforms it with Pandas, and loads it into PostgreSQL via Airflow.

---

## 📂 Project Structure
```
Data-Pipeline-Airflow-Postgres/
│── README.md
│── dags/
│   ├── api_to_postgres_dag.py
│── sql/
│   ├── create_tables.sql
│── docker-compose.yml
```

---

## 📜 DAG Explanation
- `extract_data()` downloads JSON from API and writes CSV to `/tmp/etl/posts.csv`
- `load_data()` inserts records into `posts` table using psycopg2
- DAG runs `@daily`, `catchup=False`

---

## 🚀 Run in Docker
```bash
docker compose up -d
# Airflow UI: http://localhost:8080 (user/pass: admin/admin)
# Enable DAG `api_to_postgres` and trigger a manual run.
docker compose down
```

---

## 📸 Screenshots
_Add Airflow DAG images to `screenshots/`._

---

## 📜 License
MIT License (see `LICENSE.md`)
