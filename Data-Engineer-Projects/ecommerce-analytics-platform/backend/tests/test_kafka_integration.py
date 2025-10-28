"""Integration tests for Kafka streaming components."""

import asyncio
import json
import socket
from datetime import UTC, datetime

import pytest
from sqlalchemy import text

from backend.app.core.db import AsyncSessionLocal
from streaming.kafka_reader import KafkaReader
from streaming.orders_worker import handle_orders_batch
from streaming.settings import StreamingSettings

pytestmark = [pytest.mark.asyncio, pytest.mark.slow]


def is_port_open(host: str, port: int) -> bool:
    """Check if a TCP port is open."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        result = sock.connect_ex((host, port))
        return result == 0
    finally:
        sock.close()


@pytest.fixture(scope="module")
def kafka_available():
    """Skip tests if Kafka is not available."""
    if not is_port_open("localhost", 9092):
        pytest.skip("Kafka not available on localhost:9092")


@pytest.fixture
async def kafka_reader(kafka_available):
    """Create test Kafka reader."""
    settings = StreamingSettings()
    reader = KafkaReader("orders", "test-group", "localhost:9092", settings)
    try:
        await reader.open()
        yield reader
    finally:
        await reader.close()


async def test_kafka_end_to_end(kafka_reader):
    """Test end-to-end order processing through Kafka."""
    # Create test messages
    messages = []
    for i in range(5):
        messages.append(
            {
                "order_id": f"test-{i}",
                "order_line_nbr": 1,
                "transaction_ts": datetime.now(UTC).isoformat(),
                "customer_nk": f"C{i}",
                "email": f"test{i}@example.com",
                "sku": f"P{i}",
                "quantity": i + 1,
                "unit_price": 10.0 * (i + 1),
            }
        )

    # Publish messages
    from aiokafka import AIOKafkaProducer

    producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")
    await producer.start()
    try:
        # Publish messages
        for msg in messages:
            await producer.send_and_wait("orders", json.dumps(msg).encode())
    finally:
        await producer.stop()

    # Wait for messages to be available
    await asyncio.sleep(1)

    # Poll and process messages
    events = await kafka_reader.poll(10)
    assert len(events) > 0

    # Process batch
    async with AsyncSessionLocal() as session:
        # Get initial count
        initial_count = await session.scalar(text("SELECT COUNT(*) FROM dw.fact_sales"))

        # Process events
        inserts, updates = await handle_orders_batch(events, session)

        # Verify counts increased
        final_count = await session.scalar(text("SELECT COUNT(*) FROM dw.fact_sales"))
        assert final_count > initial_count
        assert inserts > 0

        # Process same batch again - should be idempotent
        inserts2, updates2 = await handle_orders_batch(events, session)
        assert inserts2 == 0  # No new inserts

        # Verify count unchanged
        idempotent_count = await session.scalar(
            text("SELECT COUNT(*) FROM dw.fact_sales")
        )
        assert idempotent_count == final_count
