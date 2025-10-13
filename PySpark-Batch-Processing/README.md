# 🚀 PySpark Batch Processing – Databricks/Local
**Author:** Vlad Gabriel Popescu  
**Role:** Junior Data Engineer  

![Status](https://img.shields.io/badge/status-active-brightgreen)
![PySpark](https://img.shields.io/badge/PySpark-ready-orange)

## 📌 Description
Process CSV datasets with PySpark and output optimized Parquet files.

---

## 📂 Project Structure
```
PySpark-Batch-Processing/
│── README.md
│── notebooks/
│   ├── batch_job.py
│── data/
│   ├── large_dataset.csv
│── output/               # Parquet output
│── docker-compose.yml
```

---

## 📜 Code Explanation
- Reads `/data/large_dataset.csv` with header and inferred schema
- Renames `old_column` → `new_column`, drops NA rows
- Writes results to `/output/parquet_data`

---

## 🚀 Run in Docker
```bash
docker compose up -d
docker exec -it pyspark-batch spark-submit /opt/spark-apps/batch_job.py
# Parquet files appear under ./output/parquet_data
docker compose down
```

---

## 📸 Screenshots
_Add images to `screenshots/`._

---

## 📜 License
MIT License (see `LICENSE.md`)
