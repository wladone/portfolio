"""Kafka event reader implementation."""

import hashlib
import json
import logging
from datetime import UTC, datetime

import orjson
from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition
from aiokafka.structs import OffsetAndMetadata

from .event_reader import Event
from .metrics import (
    KAFKA_ASSIGNMENTS,
    KAFKA_COMMITS,
    KAFKA_CONSUMER_LAG,
    KAFKA_POLL_RECORDS,
)
from .settings import StreamingSettings


class KafkaReader:
    """Kafka implementation of EventReader protocol."""

    def __init__(
        self,
        topic: str,
        group_id: str,
        brokers: str,
        settings: StreamingSettings,
        key_fields: list[str] | None = None,
    ):
        """Initialize Kafka reader.

        Args:
            topic: Topic name to consume from
            group_id: Consumer group ID
            brokers: Comma-separated list of brokers
            settings: Streaming settings
            key_fields: List of payload fields to hash for event_id
        """
        self.topic = topic
        self.group_id = group_id
        self.brokers = brokers
        self.settings = settings
        self.key_fields = key_fields or ["order_id", "order_line_nbr", "transaction_ts"]

        # Will be set in open()
        self._consumer: AIOKafkaConsumer | None = None
        self._partitions: set[TopicPartition] = set()
        self._in_flight_commits = 0

    @property
    def source(self) -> str:
        """Get event source identifier."""
        return f"kafka:{self.topic}"

    @property
    def partition(self) -> str:
        """Get partition identifier.

        Note: Returns "*" as we handle multiple partitions, but internal checkpointing
        tracks per-partition offsets.
        """
        return "*"

    async def open(self) -> None:
        """Open connection to Kafka."""
        self._consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.brokers,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset=self.settings.KAFKA_AUTO_OFFSET_RESET,
            max_poll_records=self.settings.KAFKA_MAX_POLL_RECORDS,
            security_protocol=self.settings.KAFKA_SECURITY_PROTOCOL,
        )

        # Configure SASL if enabled
        if self.settings.KAFKA_SASL_MECHANISM:
            self._consumer.sasl_mechanism = self.settings.KAFKA_SASL_MECHANISM
            self._consumer.sasl_plain_username = self.settings.KAFKA_SASL_USERNAME
            self._consumer.sasl_plain_password = self.settings.KAFKA_SASL_PASSWORD

        # Configure SSL if enabled
        if self.settings.KAFKA_SSL_CAFILE:
            self._consumer.ssl_cafile = self.settings.KAFKA_SSL_CAFILE

        # Register callbacks
        self._consumer.subscribe([self.topic], listener=self)

        await self._consumer.start()

    def _parse_timestamp(self, record: ConsumerRecord, payload: dict) -> datetime:
        """Extract timestamp from record or payload."""
        if "transaction_ts" in payload:
            # Try parsing from payload first
            try:
                import ciso8601

                return ciso8601.parse_datetime(payload["transaction_ts"])
            except (ValueError, ImportError):
                try:
                    return datetime.fromisoformat(payload["transaction_ts"])
                except ValueError:
                    pass

        # Fallback to Kafka record timestamp
        return datetime.fromtimestamp(record.timestamp / 1000, tz=UTC)

    def _generate_event_id(self, record: ConsumerRecord, payload: dict) -> str:
        """Generate stable event ID from record."""
        # Try key fields from payload
        key_values = []
        has_all_fields = True
        for field in self.key_fields:
            if field not in payload:
                has_all_fields = False
                break
            key_values.append(str(payload[field]))

        if has_all_fields:
            return hashlib.sha1("-".join(key_values).encode()).hexdigest()

        # Fallback to value hash
        return hashlib.sha1(record.value).hexdigest()

    def _normalize_debezium(self, payload: dict) -> dict | None:
        """Handle Debezium CDC format if present."""
        if "op" not in payload or "after" not in payload:
            return payload

        op = payload["op"]
        if op == "d":  # Delete - skip
            return None

        if op in ("c", "u"):  # Create/Update
            row = payload["after"]
            if "ts_ms" in payload:
                # Convert Debezium timestamp
                row["transaction_ts"] = datetime.fromtimestamp(
                    payload["ts_ms"] / 1000, tz=UTC
                ).isoformat()
            return row

        return payload  # Other ops pass through

    async def poll(self, max_events: int) -> list[Event]:
        """Poll for next batch of events."""
        if not self._consumer:
            raise RuntimeError("Reader not opened")

        records = await self._consumer.getmany(
            timeout_ms=self.settings.STREAM_POLL_INTERVAL_MS, max_records=max_events
        )

        events = []
        for partition, partition_records in records.items():
            for record in partition_records:
                try:
                    # Parse JSON payload
                    try:
                        payload = orjson.loads(record.value)
                    except Exception:  # Fallback to standard json
                        payload = json.loads(record.value)

                    # Handle Debezium envelope
                    payload = self._normalize_debezium(payload)
                    if payload is None:  # Skip deleted
                        continue

                    # Build normalized event
                    events.append(
                        Event(
                            source=self.source,
                            partition=str(record.partition),
                            offset=record.offset,
                            event_id=self._generate_event_id(record, payload),
                            timestamp=self._parse_timestamp(record, payload),
                            payload=payload,
                            raw=record.value,
                            key=record.key.decode("utf-8") if record.key else None,
                        )
                    )
                except Exception as e:
                    logging.error(f"Failed to process record: {e}")
                    continue

        # Track metrics
        KAFKA_POLL_RECORDS.observe(len(events))

        # Update lag metrics if possible
        if self._consumer and events:
            for tp in self._partitions:
                try:
                    end_offset = await self._consumer.end_offsets([tp])
                    if tp in end_offset:
                        lag = end_offset[tp] - events[-1].offset
                        KAFKA_CONSUMER_LAG.labels(
                            topic=self.topic,
                            partition=tp.partition,
                            group=self.group_id,
                        ).set(lag)
                except Exception as e:
                    logging.warning(f"Failed to update lag metric: {e}")

        return sorted(events, key=lambda e: e.offset)

    async def ack(self, last_offset: int) -> None:
        """Commit offset after successful processing."""
        if not self._consumer or not self._partitions:
            return

        # Limit in-flight commits
        if self._in_flight_commits >= self.settings.KAFKA_MAX_IN_FLIGHT_COMMITS:
            return

        try:
            self._in_flight_commits += 1

            # Commit for all assigned partitions
            offsets = {
                tp: OffsetAndMetadata(last_offset + 1, "") for tp in self._partitions
            }
            await self._consumer.commit(offsets)

            KAFKA_COMMITS.labels(topic=self.topic, group=self.group_id).inc()

        finally:
            self._in_flight_commits -= 1

    async def close(self) -> None:
        """Close the consumer."""
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None
            self._partitions.clear()

    def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:
        """Handle partition assignment."""
        self._partitions = partitions
        KAFKA_ASSIGNMENTS.labels(topic=self.topic, group=self.group_id).set(
            len(partitions)
        )
        logging.info(f"Assigned partitions: {partitions}")

    def on_partitions_revoked(self, partitions: set[TopicPartition]) -> None:
        """Handle partition revocation."""
        self._partitions.clear()
        KAFKA_ASSIGNMENTS.labels(topic=self.topic, group=self.group_id).set(0)
        logging.info(f"Revoked partitions: {partitions}")
