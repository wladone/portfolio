"""Sales event producer that replays JSON lines into Kafka."""
import argparse
import json
import logging
import random
import sys
import time
from pathlib import Path

from kafka import KafkaProducer


def load_events(data_path: Path) -> list[dict]:
    try:
        if not data_path.exists():
            raise FileNotFoundError(f"Data path does not exist: {data_path}")
        events: list[dict] = []
        for json_file in sorted(data_path.glob("sales_*.json")):
            with json_file.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if line.strip():
                        events.append(json.loads(line))
        if not events:
            raise ValueError(f"No events found in {data_path}")
        random.shuffle(events)
        return events
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in sales files: {e}")
    except Exception as e:
        raise RuntimeError(f"Failed to load events from {data_path}: {e}")


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    try:
        return KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            key_serializer=lambda key: key.encode(
                "utf-8") if key is not None else None,
            linger_ms=20,
            retries=3,
        )
    except Exception as e:
        raise RuntimeError(f"Failed to create Kafka producer: {e}")


def stream_events(
    producer: KafkaProducer,
    topic: str,
    events: list[dict],
    rate: int,
    replay: bool,
    logger,
) -> None:
    interval = 1.0 / max(rate, 1)
    sent = 0
    try:
        while True:
            for event in events:
                key = event.get("order_id") or event.get("event_id")
                producer.send(topic, key=key, value=event)
                sent += 1
                if rate > 0:
                    time.sleep(interval)
            producer.flush()
            if not replay:
                break
            random.shuffle(events)
    except KeyboardInterrupt:
        logger.info("Interrupted by user, flushing pending messages...")
    except Exception as e:
        logger.error(f"Error during streaming: {e}")
        raise
    finally:
        try:
            producer.flush()
            producer.close()
        except Exception as e:
            logger.error(f"Error closing producer: {e}")
        logger.info(f"Sent {sent} events to {topic}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay synthetic sales events into Kafka.")
    parser.add_argument("--bootstrap", dest="bootstrap",
                        default="localhost:9092", help="Kafka bootstrap servers (host:port)")
    parser.add_argument("--topic", dest="topic",
                        default="global_electronics_sales", help="Kafka topic to publish events")
    parser.add_argument("--data-path", dest="data_path", default=str(Path(__file__).resolve(
    ).parents[2] / "data" / "sales_stream"), help="Path containing sales_*.json files")
    parser.add_argument("--rate", dest="rate", type=int,
                        default=25, help="Target events per second (0 for max)")
    parser.add_argument("--replay", dest="replay", action="store_true",
                        help="Loop over the dataset indefinitely")
    parser.add_argument("--log-file", default=str(Path(__file__).resolve(
    ).parents[2] / "_logs" / "sales_producer.log"), help="Log file path")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename=args.log_file)
    logger = logging.getLogger(__name__)

    try:
        # Input validation
        if args.rate < 0:
            raise ValueError("Rate must be non-negative")
        if not args.bootstrap:
            raise ValueError("Bootstrap servers must be provided")
        if not args.topic:
            raise ValueError("Topic must be provided")

        data_path = Path(args.data_path)
        events = load_events(data_path)
        producer = build_producer(args.bootstrap)
        logger.info(f"Loaded {len(events)} events from {data_path}")
        logger.info(
            f"Publishing to topic {args.topic} at ~{args.rate} events/sec")
        stream_events(producer, args.topic, events,
                      args.rate, args.replay, logger)
        return 0
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
