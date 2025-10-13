# 🔗 FastAPI + PostgreSQL – Data API
**Author:** Vlad Gabriel Popescu  
**Role:** Backend Developer / Data Engineer  

![Status](https://img.shields.io/badge/status-active-brightgreen)
![FastAPI](https://img.shields.io/badge/FastAPI-ready-teal)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-ready-blue)

## 📌 Description
REST API built with FastAPI and connected to PostgreSQL. Provides CRUD operations for financial-like items.

---

## 📂 Project Structure
```
FastAPI-Postgres-DataAPI/
│── README.md
│── app/
│   ├── main.py
│── sql/
│   ├── init_db.sql
│── requirements.txt
│── Dockerfile
│── docker-compose.yml
```

---

## 📜 Code Explanation
- `/` – health endpoint
- `POST /items/` – inserts an item (name, value)
- `GET /items/` – lists items sorted by id desc

---

## 🚀 Run in Docker
```bash
docker compose up --build -d
# Test:
# 1) Open Swagger: http://localhost:8000/docs
# 2) POST /items/ with {"name":"Sample","value":123.45}
# 3) GET /items/ should list the new item
docker compose down
```

---

## 📸 Screenshots
_Add images to `screenshots/`._

---

## 📜 License
MIT License (see `LICENSE.md`)
