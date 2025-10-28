"""Tests for OrdersWorker streaming functionality."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend.app.streaming.orders_worker import OrdersWorker
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
def sample_orders():
    """Provide sample order messages for testing."""
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
            "order_id": "ORD-1001",
            "order_line_nbr": 2,
            "customer_nk": "cust-1001",
            "email": "user1@example.com",
            "sku": "SKU-1002",
            "product_name": "Mouse Wireless",
            "brand": "TechBrand",
            "category": "Electronics",
            "quantity": 2,
            "unit_price": 29.99,
            "discount_amount": 0,
            "currency_code": "USD",
            "channel_code": "web",
            "transaction_ts": "2025-01-05T12:00:00Z",
        },
    ]


class TestOrdersWorker:
    """Test suite for OrdersWorker."""

    def test_init_creates_sales_repo(self, streaming_settings):
        """Test that OrdersWorker initializes with SalesRepository."""
        with patch(
            "backend.app.streaming.orders_worker.SalesRepository"
        ) as mock_repo_class:
            mock_repo = MagicMock()
            mock_repo_class.return_value = mock_repo

            worker = OrdersWorker(streaming_settings)

            mock_repo_class.assert_called_once()
            assert worker.sales_repo == mock_repo

    @pytest.mark.asyncio
    async def test_process_batch_success(self, streaming_settings, sample_orders):
        """Test successful processing of order batch."""
        with (
            patch(
                "backend.app.streaming.orders_worker.SalesRepository"
            ) as mock_repo_class,
            patch(
                "backend.app.streaming.orders_worker.get_db_session"
            ) as mock_get_session,
        ):
            mock_repo = MagicMock()
            mock_repo_class.return_value = mock_repo
            mock_session = MagicMock()
            mock_get_session.return_value.__aenter__ = AsyncMock(
                return_value=mock_session
            )
            mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

            worker = OrdersWorker(streaming_settings)
            await worker.process_batch(sample_orders)

            # Verify session was acquired
            mock_get_session.assert_called_once()

            # Verify bulk_insert_orders was called with correct parameters
            mock_repo.bulk_insert_orders.assert_called_once_with(
                mock_session, sample_orders
            )

    @pytest.mark.asyncio
    async def test_process_batch_handles_exception(
        self, streaming_settings, sample_orders
    ):
        """Test that exceptions in process_batch are logged but not re-raised."""
        with (
            patch(
                "backend.app.streaming.orders_worker.SalesRepository"
            ) as mock_repo_class,
            patch(
                "backend.app.streaming.orders_worker.get_db_session"
            ) as mock_get_session,
            patch("backend.app.streaming.orders_worker.logger") as mock_logger,
        ):
            mock_repo = MagicMock()
            mock_repo.bulk_insert_orders.side_effect = Exception("Database error")
            mock_repo_class.return_value = mock_repo
            mock_session = MagicMock()
            mock_get_session.return_value.__aenter__ = AsyncMock(
                return_value=mock_session
            )
            mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

            worker = OrdersWorker(streaming_settings)
            await worker.process_batch(sample_orders)

            # Verify error was logged
            mock_logger.error.assert_called_once_with(
                "Failed to process orders batch: Database error"
            )

            # Verify bulk_insert_orders was still called
            mock_repo.bulk_insert_orders.assert_called_once_with(
                mock_session, sample_orders
            )

    @pytest.mark.asyncio
    async def test_process_batch_empty_batch(self, streaming_settings):
        """Test processing of empty batch."""
        with (
            patch(
                "backend.app.streaming.orders_worker.SalesRepository"
            ) as mock_repo_class,
            patch(
                "backend.app.streaming.orders_worker.get_db_session"
            ) as mock_get_session,
        ):
            mock_repo = MagicMock()
            mock_repo_class.return_value = mock_repo
            mock_session = MagicMock()
            mock_get_session.return_value.__aenter__ = AsyncMock(
                return_value=mock_session
            )
            mock_get_session.return_value.__aexit__ = AsyncMock(return_value=None)

            worker = OrdersWorker(streaming_settings)
            await worker.process_batch([])

            # Verify bulk_insert_orders was called with empty list
            mock_repo.bulk_insert_orders.assert_called_once_with(mock_session, [])

    def test_inherits_from_streaming_worker(self, streaming_settings):
        """Test that OrdersWorker inherits from StreamingWorker."""
        with patch("backend.app.streaming.orders_worker.SalesRepository"):
            worker = OrdersWorker(streaming_settings)

            # Check inheritance
            from backend.app.streaming.worker import StreamingWorker

            assert isinstance(worker, StreamingWorker)

            # Check that it has the required attributes
            assert hasattr(worker, "settings")
            assert hasattr(worker, "reader")
            assert hasattr(worker, "running")
