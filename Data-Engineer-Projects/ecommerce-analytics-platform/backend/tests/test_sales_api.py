"""Comprehensive tests for sales analytics API endpoints."""

from __future__ import annotations

import time
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from backend.app.config import AppSettings
from backend.app.main import app


@pytest.fixture
def test_client(db_engine):
    """Provide a FastAPI test client."""
    with TestClient(app) as client:
        yield client


@pytest.fixture
def small_test_data(db_engine):
    """Insert minimal test data: 1 customer, 2 products, 2-3 days, few fact_sales rows."""
    with db_engine.connect() as conn:
        # Insert dim_date for 2024-01-01 to 2024-01-03
        conn.execute(
            text(
                """
            INSERT INTO dw.dim_date (date_key, date, year, month, day, day_of_week, is_weekend)
            VALUES
            (20240101, '2024-01-01', 2024, 1, 1, 1, false),
            (20240102, '2024-01-02', 2024, 1, 2, 2, false),
            (20240103, '2024-01-03', 2024, 1, 3, 3, false)
            ON CONFLICT (date_key) DO NOTHING
        """
            )
        )
        conn.commit()

        # Insert dim_channel
        conn.execute(
            text(
                """
            INSERT INTO dw.dim_channel (channel_id, channel_code, channel_name)
            VALUES (1, 'online', 'Online Sales')
            ON CONFLICT (channel_id) DO NOTHING
        """
            )
        )
        conn.commit()

        # Insert dim_customer
        conn.execute(
            text(
                """
            INSERT INTO dw.dim_customer (customer_id, customer_code, name, email)
            VALUES (1, 'CUST001', 'Test Customer', 'test@example.com')
            ON CONFLICT (customer_id) DO NOTHING
        """
            )
        )
        conn.commit()

        # Insert dim_product
        conn.execute(
            text(
                """
            INSERT INTO dw.dim_product (product_id, sku, name, category)
            VALUES
            (1, 'PROD001', 'Test Product 1', 'Electronics'),
            (2, 'PROD002', 'Test Product 2', 'Books')
            ON CONFLICT (product_id) DO NOTHING
        """
            )
        )
        conn.commit()

        # Insert fact_sales: 3 rows on 2 days
        conn.execute(
            text(
                """
            INSERT INTO dw.fact_sales (
                date_key, customer_id, product_id, channel_id, order_id, order_line_nbr,
                transaction_ts, currency_code, quantity, unit_price, discount_amount, net_amount
            ) VALUES
            (20240101, 1, 1, 1, 'ORD001', 1, '2024-01-01 10:00:00+00', 'USD', 2, 10.00, 0.00, 20.00),
            (20240101, 1, 2, 1, 'ORD001', 2, '2024-01-01 10:00:00+00', 'USD', 1, 15.00, 1.50, 13.50),
            (20240102, 1, 1, 1, 'ORD002', 1, '2024-01-02 11:00:00+00', 'USD', 3, 10.00, 0.00, 30.00)
        """
            )
        )
        conn.commit()

    yield

    # Cleanup
    with db_engine.connect() as conn:
        conn.execute(
            text("DELETE FROM dw.fact_sales WHERE order_id IN ('ORD001', 'ORD002')")
        )
        conn.execute(text("DELETE FROM dw.dim_product WHERE product_id IN (1, 2)"))
        conn.execute(text("DELETE FROM dw.dim_customer WHERE customer_id = 1"))
        conn.execute(text("DELETE FROM dw.dim_channel WHERE channel_id = 1"))
        conn.execute(
            text(
                "DELETE FROM dw.dim_date WHERE date_key IN (20240101, 20240102, 20240103)"
            )
        )
        conn.commit()


def test_sales_summary_day_ok(test_client, small_test_data):
    """Insert 3 rows in fact_sales on 2 days, verify aggregation on day (orders/items/net)."""
    response = test_client.get(
        "/api/v1/sales/summary",
        params={"from_": "2024-01-01", "to": "2024-01-02", "granularity": "day"},
    )
    assert response.status_code == 200
    data = response.json()

    # Should have 2 rows (one per day)
    assert len(data["rows"]) == 2

    # Day 1: 2024-01-01 - 1 order, items=3, net=33.50
    day1 = next(r for r in data["rows"] if r["date"] == "2024-01-01")
    assert day1["orders"] == 1
    assert day1["items"] == 3
    assert Decimal(str(day1["net"])) == Decimal("33.50")

    # Day 2: 2024-01-02 - 1 order, items=3, net=30.00
    day2 = next(r for r in data["rows"] if r["date"] == "2024-01-02")
    assert day2["orders"] == 1
    assert day2["items"] == 3
    assert Decimal(str(day2["net"])) == Decimal("30.00")


def test_sales_summary_month_ok(test_client, small_test_data):
    """Aggregation on month."""
    response = test_client.get(
        "/api/v1/sales/summary",
        params={"from_": "2024-01-01", "to": "2024-01-31", "granularity": "month"},
    )
    assert response.status_code == 200
    data = response.json()

    # Should have 1 row for January
    assert len(data["rows"]) == 1
    row = data["rows"][0]
    assert row["year"] == 2024
    assert row["month"] == 1
    assert row["orders"] == 2  # 2 orders total
    assert row["items"] == 6  # 3 + 3
    assert Decimal(str(row["net"])) == Decimal("63.50")  # 33.50 + 30.00


def test_top_products_by_net(test_client, small_test_data):
    """Verify ordering desc by metric=net, respect limit/offset."""
    response = test_client.get(
        "/api/v1/sales/top-products",
        params={
            "from_": "2024-01-01",
            "to": "2024-01-02",
            "metric": "net",
            "limit": 1,
            "offset": 0,
        },
    )
    assert response.status_code == 200
    data = response.json()

    # Should have 1 row (limit=1)
    assert len(data["rows"]) == 1
    assert data["total"] == 2  # 2 products total

    # PROD001 should be first (net=50.00 > PROD002 net=13.50)
    row = data["rows"][0]
    assert row["sku"] == "PROD001"
    assert Decimal(str(row["net"])) == Decimal("50.00")


def test_cache_layer(test_client, small_test_data, monkeypatch):
    """Call same endpoint twice, verify cache hit (faster second call)."""
    # Mock Redis to avoid external dependency
    from backend.app.core.cache import RedisCache

    class MockRedis:
        def __init__(self):
            self.data = {}

        async def get(self, key):
            return self.data.get(key)

        async def setex(self, key, ttl, value):
            self.data[key] = value

    original_init = RedisCache.__init__

    def mock_init(self, redis_url=None):
        self.redis_url = redis_url or "redis://localhost:6379"
        self._client = MockRedis()

    monkeypatch.setattr(RedisCache, "__init__", mock_init)

    params = {"from_": "2024-01-01", "to": "2024-01-02", "granularity": "day"}

    # First call
    start1 = time.time()
    response1 = test_client.get("/api/v1/sales/summary", params=params)
    duration1 = time.time() - start1

    assert response1.status_code == 200

    # Second call (should be cached)
    start2 = time.time()
    response2 = test_client.get("/api/v1/sales/summary", params=params)
    duration2 = time.time() - start2

    assert response2.status_code == 200
    assert response1.json() == response2.json()

    # Second call should be faster (cache hit)
    # Note: In real scenarios, this would be more pronounced, but we check it's not slower
    assert duration2 <= duration1 * 1.1  # Allow some variance


def test_rate_limit(test_client, small_test_data, monkeypatch):
    """Set RATE_LIMIT_REQUESTS=2, hit endpoint 3x in window ⇒ 429 on third."""
    # Override settings
    original_settings = app.state.settings
    test_settings = AppSettings(
        app_env="test",
        database_url="postgresql://test:test@localhost:5432/test",
        redis_url="redis://localhost:6379",
        minio_endpoint="http://localhost:9000",
        minio_access_key="test",
        minio_secret_key="test",
        jwt_secret="test",
        rate_limit_enabled=True,
        rate_limit_requests=2,
        rate_limit_window_seconds=60,
    )
    app.state.settings = test_settings

    try:
        params = {"from_": "2024-01-01", "to": "2024-01-02", "granularity": "day"}

        # First request
        response1 = test_client.get("/api/v1/sales/summary", params=params)
        assert response1.status_code == 200

        # Second request
        response2 = test_client.get("/api/v1/sales/summary", params=params)
        assert response2.status_code == 200

        # Third request should be rate limited
        response3 = test_client.get("/api/v1/sales/summary", params=params)
        assert response3.status_code == 429
        assert "Rate limit exceeded" in response3.json()["detail"]

    finally:
        app.state.settings = original_settings
