import json
import logging
from typing import Any

from kafka import KafkaConsumer

from backend.app.streaming.settings import StreamingSettings

logger = logging.getLogger(__name__)


class KafkaReader:
    def __init__(self, settings: StreamingSettings):
        self.settings = settings
        self.consumer = KafkaConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )

    def subscribe(self, topics: list[str]):
        self.consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {topics}")

    def poll(self) -> list[dict[str, Any]]:
        messages = []
        records = self.consumer.poll(timeout_ms=self.settings.poll_timeout_ms)
        for topic_partition, records_list in records.items():
            for record in records_list:
                messages.append(record.value)
        return messages

    def close(self):
        self.consumer.close()
        logger.info("Kafka consumer closed")
