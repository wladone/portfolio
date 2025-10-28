"""Tests for KafkaReader contract and functionality."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from backend.app.streaming.reader import KafkaReader
from backend.app.streaming.settings import StreamingSettings


@pytest.fixture
def streaming_settings():
    """Provide test streaming settings."""
    return StreamingSettings(
        kafka_bootstrap_servers="localhost:9092",
        kafka_group_id="test_group",
        poll_timeout_ms=100,
    )


@pytest.fixture
def sample_messages():
    """Provide sample Kafka messages for testing."""
    return [
        {
            "order_id": "ORD-1001",
            "order_line_nbr": 1,
            "customer_nk": "cust-1001",
            "email": "user1@example.com",
            "sku": "SKU-1001",
            "product_name": "Laptop Pro",
            "brand": "TechBrand",
            "category": "Electronics",
            "quantity": 1,
            "unit_price": 1299.99,
            "discount_amount": 50.00,
            "currency_code": "USD",
            "channel_code": "web",
            "transaction_ts": "2025-01-05T12:00:00Z",
        },
        {
            "order_id": "ORD-1002",
            "order_line_nbr": 1,
            "customer_nk": "cust-1002",
            "email": "user2@example.com",
            "sku": "SKU-1003",
            "product_name": "Book Python Guide",
            "brand": "BookPub",
            "category": "Books",
            "quantity": 1,
            "unit_price": 39.99,
            "discount_amount": 5.00,
            "currency_code": "USD",
            "channel_code": "mobile",
            "transaction_ts": "2025-01-05T13:30:00Z",
        },
    ]


class TestKafkaReader:
    """Test suite for KafkaReader."""

    def test_init_creates_consumer_with_correct_config(self, streaming_settings):
        """Test that KafkaReader initializes consumer with correct configuration."""
        with patch("backend.app.streaming.reader.KafkaConsumer") as mock_consumer:
            reader = KafkaReader(streaming_settings)

            mock_consumer.assert_called_once_with(
                bootstrap_servers="localhost:9092",
                group_id="test_group",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=mock_consumer.call_args[1]["value_deserializer"],
            )

            # Verify deserializer function
            deserializer = mock_consumer.call_args[1]["value_deserializer"]
            test_message = json.dumps({"test": "data"}).encode("utf-8")
            assert deserializer(test_message) == {"test": "data"}

    def test_subscribe_calls_consumer_subscribe(self, streaming_settings):
        """Test that subscribe method calls consumer.subscribe with topics."""
        with patch("backend.app.streaming.reader.KafkaConsumer") as mock_consumer_class:
            mock_consumer = MagicMock()
            mock_consumer_class.return_value = mock_consumer

            reader = KafkaReader(streaming_settings)
            topics = ["orders", "customers"]

            reader.subscribe(topics)

            mock_consumer.subscribe.assert_called_once_with(topics)

    def test_poll_returns_deserialized_messages(
        self, streaming_settings, sample_messages
    ):
        """Test that poll returns properly deserialized messages from consumer."""
        with patch("backend.app.streaming.reader.KafkaConsumer") as mock_consumer_class:
            mock_consumer = MagicMock()
            mock_consumer_class.return_value = mock_consumer

            # Mock poll response with topic partitions
            mock_records = MagicMock()
            mock_records.items.return_value = [
                (
                    MagicMock(),
                    [
                        MagicMock(value=json.dumps(msg).encode("utf-8"))
                        for msg in sample_messages
                    ],
                )
            ]
            mock_consumer.poll.return_value = mock_records

            reader = KafkaReader(streaming_settings)
            result = reader.poll()

            assert result == sample_messages
            mock_consumer.poll.assert_called_once_with(timeout_ms=100)

    def test_poll_returns_empty_list_when_no_messages(self, streaming_settings):
        """Test that poll returns empty list when no messages available."""
        with patch("backend.app.streaming.reader.KafkaConsumer") as mock_consumer_class:
            mock_consumer = MagicMock()
            mock_consumer_class.return_value = mock_consumer

            mock_consumer.poll.return_value = {}

            reader = KafkaReader(streaming_settings)
            result = reader.poll()

            assert result == []

    def test_close_calls_consumer_close(self, streaming_settings):
        """Test that close method calls consumer.close."""
        with patch("backend.app.streaming.reader.KafkaConsumer") as mock_consumer_class:
            mock_consumer = MagicMock()
            mock_consumer_class.return_value = mock_consumer

            reader = KafkaReader(streaming_settings)
            reader.close()

            mock_consumer.close.assert_called_once()
