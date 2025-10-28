"""Comprehensive tests for recommendations API endpoints."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

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
def minimal_recs_data(db_engine):
    """Insert minimal test data for recs: 2 customers, 3 products, some fact_sales."""
    with db_engine.connect() as conn:
        # Insert dim_date for 2024-01-01
        conn.execute(
            text(
                """
            INSERT INTO dw.dim_date (date_key, date, year, month, day, day_of_week, is_weekend)
            VALUES (20240101, '2024-01-01', 2024, 1, 1, 1, false)
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

        # Insert dim_customer: user 1 (known), user 999 (unknown)
        conn.execute(
            text(
                """
            INSERT INTO dw.dim_customer (customer_id, customer_code, name, email)
            VALUES
            (1, 'CUST001', 'Known User', 'known@example.com'),
            (999, 'CUST999', 'Unknown User', 'unknown@example.com')
            ON CONFLICT (customer_id) DO NOTHING
        """
            )
        )
        conn.commit()

        # Insert dim_product: 3 products
        conn.execute(
            text(
                """
            INSERT INTO dw.dim_product (product_id, sku, name, category)
            VALUES
            (1, 'PROD001', 'Product 1', 'Electronics'),
            (2, 'PROD002', 'Product 2', 'Books'),
            (3, 'PROD003', 'Product 3', 'Clothing')
            ON CONFLICT (product_id) DO NOTHING
        """
            )
        )
        conn.commit()

        # Insert fact_sales: user 1 has purchased PROD001 and PROD002
        conn.execute(
            text(
                """
            INSERT INTO dw.fact_sales (
                date_key, customer_id, product_id, channel_id, order_id, order_line_nbr,
                transaction_ts, currency_code, quantity, unit_price, discount_amount, net_amount
            ) VALUES
            (20240101, 1, 1, 1, 'ORD001', 1, '2024-01-01 10:00:00+00', 'USD', 1, 10.00, 0.00, 10.00),
            (20240101, 1, 2, 1, 'ORD001', 2, '2024-01-01 10:00:00+00', 'USD', 1, 15.00, 0.00, 15.00)
        """
            )
        )
        conn.commit()

    yield

    # Cleanup
    with db_engine.connect() as conn:
        conn.execute(text("DELETE FROM dw.fact_sales WHERE order_id = 'ORD001'"))
        conn.execute(text("DELETE FROM dw.dim_product WHERE product_id IN (1, 2, 3)"))
        conn.execute(text("DELETE FROM dw.dim_customer WHERE customer_id IN (1, 999)"))
        conn.execute(text("DELETE FROM dw.dim_channel WHERE channel_id = 1"))
        conn.execute(text("DELETE FROM dw.dim_date WHERE date_key = 20240101"))
        conn.commit()


@pytest.fixture
def mock_als_recommender():
    """Mock AlsRecommender with predictable behavior."""
    mock_recommender = MagicMock()
    mock_recommender.recommend_for_user = MagicMock(
        return_value=[
            (1, 0.9),  # PROD001 with score 0.9
            (2, 0.8),  # PROD002 with score 0.8
            (3, 0.7),  # PROD003 with score 0.7
        ]
    )
    mock_recommender.fallback_popular = MagicMock(
        return_value=[3, 1]
    )  # PROD003, PROD001
    mock_recommender.similar_products = MagicMock(
        return_value=[
            (2, 0.85),  # PROD002 similar to PROD001
            (3, 0.75),  # PROD003 similar to PROD001
        ]
    )
    return mock_recommender


@pytest.fixture
def mock_redis_cache():
    """Mock RedisCache to avoid external dependencies."""
    mock_cache = MagicMock()
    mock_cache.get_json = AsyncMock(return_value=None)  # No cache hits
    mock_cache.set_json = AsyncMock()
    return mock_cache


def test_user_recs_fallback_for_unknown_user(
    test_client, minimal_recs_data, mock_als_recommender, mock_redis_cache
):
    """Test fallback to popular products for unknown users, verify reason='popular'."""
    with (
        patch(
            "backend.app.services.recs_service.AlsRecommender.load_latest",
            return_value=mock_als_recommender,
        ),
        patch(
            "backend.app.services.recs_service.RedisCache",
            return_value=mock_redis_cache,
        ),
    ):
        # Mock recommend_for_user to return empty for unknown user
        mock_als_recommender.recommend_for_user.return_value = []

        response = test_client.get("/api/v1/recs/user/999", params={"k": 2})
        assert response.status_code == 200
        data = response.json()

        assert data["user_id"] == 999
        assert data["k"] == 2
        assert len(data["items"]) == 2

        # Should fallback to popular products
        assert data["items"][0]["sku"] == "PROD003"  # First popular
        assert data["items"][0]["reason"] == "popular"
        assert data["items"][0]["score"] == 0.0

        assert data["items"][1]["sku"] == "PROD001"  # Second popular
        assert data["items"][1]["reason"] == "popular"
        assert data["items"][1]["score"] == 0.0


def test_user_recs_exclude_seen(
    test_client, minimal_recs_data, mock_als_recommender, mock_redis_cache
):
    """Test exclusion of previously purchased products when exclude_seen=true."""
    with (
        patch(
            "backend.app.services.recs_service.AlsRecommender.load_latest",
            return_value=mock_als_recommender,
        ),
        patch(
            "backend.app.services.recs_service.RedisCache",
            return_value=mock_redis_cache,
        ),
    ):
        # User 1 has seen PROD001 and PROD002
        response = test_client.get(
            "/api/v1/recs/user/1", params={"k": 3, "exclude_seen": True}
        )
        assert response.status_code == 200
        data = response.json()

        assert data["user_id"] == 1
        assert data["k"] == 3
        assert len(data["items"]) == 3

        # Should exclude PROD001 and PROD002, include PROD003 from ALS, then fallback to popular
        skus = [item["sku"] for item in data["items"]]
        assert "PROD001" not in skus  # Excluded
        assert "PROD002" not in skus  # Excluded
        assert "PROD003" in skus  # From ALS recs

        # Check reasons
        prod003_item = next(item for item in data["items"] if item["sku"] == "PROD003")
        assert prod003_item["reason"] == "als"
        assert prod003_item["score"] == 0.7


def test_similar_products_ok(
    test_client, minimal_recs_data, mock_als_recommender, mock_redis_cache
):
    """Test similar products endpoint with valid SKU, verify reason='similarity'."""
    with (
        patch(
            "backend.app.services.recs_service.AlsRecommender.load_latest",
            return_value=mock_als_recommender,
        ),
        patch(
            "backend.app.services.recs_service.RedisCache",
            return_value=mock_redis_cache,
        ),
    ):
        response = test_client.get(
            "/api/v1/recs/similar-products/PROD001", params={"k": 2}
        )
        assert response.status_code == 200
        data = response.json()

        assert data["sku"] == "PROD001"
        assert data["k"] == 2
        assert len(data["items"]) == 2

        # Should return similar products
        assert data["items"][0]["sku"] == "PROD002"
        assert data["items"][0]["reason"] == "similarity"
        assert data["items"][0]["score"] == 0.85

        assert data["items"][1]["sku"] == "PROD003"
        assert data["items"][1]["reason"] == "similarity"
        assert data["items"][1]["score"] == 0.75


def test_refresh_endpoint(
    test_client, minimal_recs_data, mock_als_recommender, mock_redis_cache
):
    """Test POST /recs/_refresh when RECS_ALLOW_REFRESH_ENDPOINT=True, verify 200 response."""
    # Override settings to enable refresh endpoint
    original_settings = app.state.settings
    test_settings = AppSettings(
        app_env="test",
        database_url="postgresql://test:test@localhost:5432/test",
        redis_url="redis://localhost:6379",
        minio_endpoint="http://localhost:9000",
        minio_access_key="test",
        minio_secret_key="test",
        jwt_secret="test",
        recs_allow_refresh_endpoint=True,
    )
    app.state.settings = test_settings

    try:
        with (
            patch(
                "backend.app.services.recs_service.AlsRecommender.load_latest",
                return_value=mock_als_recommender,
            ),
            patch(
                "backend.app.services.recs_service.RedisCache",
                return_value=mock_redis_cache,
            ),
        ):
            response = test_client.post("/api/v1/recs/_refresh")
            assert response.status_code == 200
            data = response.json()

            assert "model_version" in data
            assert data["status"] == "reloaded"

    finally:
        app.state.settings = original_settings
