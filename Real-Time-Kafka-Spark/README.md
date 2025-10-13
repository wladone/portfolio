# ⚡ Real-Time Streaming with Kafka & Spark Structured Streaming
**Author:** Vlad Gabriel Popescu  
**Role:** Junior Data Engineer / Big Data Developer  

![Status](https://img.shields.io/badge/status-active-brightgreen)
![Kafka](https://img.shields.io/badge/Kafka-ready-black)
![Spark](https://img.shields.io/badge/Spark-ready-orange)

## 📌 Description
Real-time ingestion with Apache Kafka and processing with Spark Structured Streaming. This setup prints messages to console (easy to demo).

---

## 📂 Project Structure
```
Real-Time-Kafka-Spark/
│── README.md
│── producer/
│   ├── kafka_producer.py
│── consumer/
│   ├── spark_streaming_job.py
│── configs/
│   ├── kafka_config.json
│── docker-compose.yml
```

---

## 📜 Code Explanation
- **Producer** sends 10 JSON messages into `sensor_data` topic
- **Spark consumer** reads from Kafka and prints parsed messages to console

---

## 🚀 Run in Docker
```bash
docker compose up -d
# Create topic is handled dynamically by producer send; if needed you can also create via kafka-topics.sh
docker exec -it spark spark-submit /opt/spark-apps/consumer/spark_streaming_job.py
# In another terminal you can see producer logs in `docker compose logs -f producer`
docker compose down
```

---

## 📸 Screenshots
_Add images to `screenshots/`._

---

## 📜 License
MIT License (see `LICENSE.md`)
