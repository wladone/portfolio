# 🛠 Portofoliu Proiecte Data & BI – Vlad Gabriel Popescu

Acest document conține instrucțiuni pentru rularea separată a fiecăruia dintre cele 5 proiecte incluse în portofoliu.
Fiecare proiect are propriul `README.md` cu explicații linie-cu-linie și pași Docker.

## 📂 Structura Proiectelor
1. `Sales-Analytics-Dashboard` – Dashboard Power BI cu sursă SQL.
2. `Data-Pipeline-Airflow-Postgres` – ETL zilnic API → PostgreSQL cu Apache Airflow.
3. `PySpark-Batch-Processing` – Procesare batch cu PySpark (local/Spark container).
4. `FastAPI-Postgres-DataAPI` – API REST (FastAPI) pentru date financiare.
5. `Real-Time-Kafka-Spark` – Streaming în timp real cu Kafka & Spark.

---

## 1️⃣ Sales Analytics Dashboard
```bash
cd Sales-Analytics-Dashboard
docker compose up -d
# Conectează Power BI Desktop la PostgreSQL: host localhost, db salesdb, user postgres, pass postgres
# La final:
docker compose down
```

## 2️⃣ Data Pipeline Airflow + Postgres
```bash
cd Data-Pipeline-Airflow-Postgres
docker compose up -d
# UI Airflow: http://localhost:8080  (creează user dacă e necesar)
# Activează DAG-ul `api_to_postgres` și rulează manual prima execuție.
docker compose down
```

## 3️⃣ PySpark Batch Processing
```bash
cd PySpark-Batch-Processing
# Variante:
# A) Spark container:
docker compose up -d
docker exec -it pyspark-batch-processing-spark-1 spark-submit /opt/spark-apps/notebooks/batch_job.py
# B) Local (dacă ai Spark instalat):
# spark-submit notebooks/batch_job_local.py
docker compose down
```

## 4️⃣ FastAPI + Postgres API
```bash
cd FastAPI-Postgres-DataAPI
docker compose up --build -d
# API Docs: http://localhost:8000/docs
docker compose down
```

## 5️⃣ Real-Time Kafka + Spark
```bash
cd Real-Time-Kafka-Spark
docker compose up -d
# Creează topicul:
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic sensor_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
# Rulează producerul (în containerul producer) și consumatorul Spark:
docker exec -it producer python /app/producer/kafka_producer.py
docker exec -it spark spark-submit /opt/spark-apps/consumer/spark_streaming_job.py
docker compose down
```

---

## 🛑 Note
- Rulează proiectele **independent**. Oprește containerele unui proiect înainte de a porni următorul.
- Dacă întâmpini probleme de permisiuni pe volume, pe Linux/Mac poți folosi: `chmod -R 777 .` în folderul proiectului.
