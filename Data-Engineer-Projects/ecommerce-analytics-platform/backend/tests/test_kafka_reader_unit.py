"""Unit tests for Kafka event reader."""

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiokafka import ConsumerRecord

from streaming.kafka_reader import KafkaReader
from streaming.settings import StreamingSettings


@pytest.fixture
def settings():
    """Create test settings."""
    return StreamingSettings()


@pytest.fixture
def mock_consumer():
    """Create mock Kafka consumer."""
    consumer = AsyncMock()
    consumer.getmany = AsyncMock()
    consumer.commit = AsyncMock()
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    return consumer


@pytest.fixture
def reader(settings, mock_consumer):
    """Create test reader with mocked consumer."""
    with patch("streaming.kafka_reader.AIOKafkaConsumer", return_value=mock_consumer):
        reader = KafkaReader("orders", "test-group", "localhost:9092", settings)
        return reader


def create_record(partition: int, offset: int, payload: dict) -> ConsumerRecord:
    """Create a test consumer record."""
    value = json.dumps(payload).encode()
    return ConsumerRecord(
        topic="orders",
        partition=partition,
        offset=offset,
        timestamp=int(datetime.now(UTC).timestamp() * 1000),
        timestamp_type=0,
        key=None,
        value=value,
        checksum=None,
        serialized_key_size=-1,
        serialized_value_size=len(value),
        headers=[],
    )


async def test_reader_initialization(reader):
    """Test reader initialization."""
    assert reader.topic == "orders"
    assert reader.group_id == "test-group"
    assert reader.brokers == "localhost:9092"
    assert reader.source == "kafka:orders"
    assert reader.partition == "*"


async def test_open_starts_consumer(reader):
    """Test open starts consumer."""
    await reader.open()
    reader._consumer.start.assert_called_once()


async def test_poll_valid_messages(reader):
    """Test polling valid messages."""
    ts = datetime.now(UTC).isoformat()
    records = [
        create_record(
            0,
            0,
            {
                "order_id": "1",
                "order_line_nbr": 1,
                "transaction_ts": ts,
                "customer_nk": "C1",
                "email": "test@example.com",
                "sku": "P1",
                "quantity": 1,
                "unit_price": 10.0,
            },
        ),
        create_record(
            0,
            1,
            {
                "order_id": "2",
                "order_line_nbr": 1,
                "transaction_ts": ts,
                "customer_nk": "C2",
                "email": "test2@example.com",
                "sku": "P2",
                "quantity": 2,
                "unit_price": 20.0,
            },
        ),
    ]

    reader._consumer.getmany.return_value = {0: records}
    await reader.open()

    events = await reader.poll(10)

    assert len(events) == 2
    assert all(e.source == "kafka:orders" for e in events)
    assert all(isinstance(e.timestamp, datetime) for e in events)
    assert [e.offset for e in events] == [0, 1]
    assert all(e.event_id for e in events)


async def test_poll_invalid_json(reader):
    """Test handling invalid JSON."""
    records = [
        create_record(0, 0, {"valid": "json"}),
        ConsumerRecord(
            topic="orders",
            partition=0,
            offset=1,
            timestamp=1000,
            timestamp_type=0,
            key=None,
            value=b"invalid json",
            checksum=None,
            serialized_key_size=-1,
            serialized_value_size=11,
            headers=[],
        ),
    ]

    reader._consumer.getmany.return_value = {0: records}
    await reader.open()

    events = await reader.poll(10)

    assert len(events) == 1  # Only valid JSON processed
    assert events[0].offset == 0


async def test_ack_commits_offset(reader):
    """Test acknowledging commits offset."""
    reader._partitions = {MagicMock(partition=0)}
    reader._consumer.commit = AsyncMock()
    await reader.open()

    await reader.ack(42)

    reader._consumer.commit.assert_called_once()


async def test_close_stops_consumer(reader):
    """Test close stops consumer."""
    await reader.open()
    await reader.close()
    reader._consumer.stop.assert_called_once()
