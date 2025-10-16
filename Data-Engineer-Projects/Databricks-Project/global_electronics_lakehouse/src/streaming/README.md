# Local Streaming Stack

This folder contains the Docker Compose stack and producer script used to emulate the real-time sales stream locally.

## Prerequisites

- Docker Desktop (with Linux containers / WSL2 on Windows).
- Python 3.10+ with `kafka-python` installed (`pip install kafka-python`).
- Synthetic dataset generated under `data/sales_stream` (see project root instructions).

## Usage

1. **Start Kafka**
   ```bash
   docker compose up -d
   ```

2. **Verify brokers**
   ```bash
   docker compose logs -f kafka
   ```
   Wait for the broker to report `Kafka Server started`.

3. **Replay demo events**
   ```bash
   python sales_producer.py --rate 25 --replay
   ```
   - `--bootstrap` overrides the broker endpoint (default `localhost:9092`).
   - `--topic` overrides the topic name (default `global_electronics_sales`).
   - `--rate` controls events/sec. Use `0` for max throughput.
   - Add `--replay` to continuously loop over the dataset.

4. **Stop stack**
   ```bash
   docker compose down
   ```

## Topic defaults

- Topic: `global_electronics_sales`
- Partitions: 3
- Auto-create enabled for development convenience

When deploying to Databricks or production Kafka clusters, pre-create the topic with appropriate retention, replication factor, and ACLs.
