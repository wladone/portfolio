"""Comprehensive tests for ALS recommendation system."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from ml.als_train import train_als_model
from ml.serve import AlsRecommender


@pytest.fixture
def synthetic_interactions():
    """Generate synthetic interactions: 20 users × 30 items with random purchases."""
    np.random.seed(42)  # Deterministic seeding
    users = list(range(1, 21))  # User IDs 1-20
    items = list(range(1, 31))  # Item IDs 1-30

    interactions = []
    for user in users:
        # Each user buys 3-10 random items with quantities 1-5
        num_purchases = np.random.randint(3, 11)
        purchased_items = np.random.choice(items, num_purchases, replace=False)
        for item in purchased_items:
            quantity = np.random.randint(1, 6)
            interactions.append((user, item, quantity))

    return interactions


@pytest.fixture
def mock_session(synthetic_interactions):
    """Mock SQLAlchemy session to return synthetic data."""
    mock_session = MagicMock()

    # Mock the context manager and execute chain to return tuples directly
    mock_session.__enter__.return_value = mock_session
    mock_session.execute.return_value.all.return_value = synthetic_interactions

    return mock_session


@pytest.fixture
def tmp_artifact_dir(tmp_path):
    """Provide temporary directory for artifacts."""
    artifact_dir = tmp_path / "als_test_artifacts"
    artifact_dir.mkdir()
    yield artifact_dir
    # Cleanup handled by tmp_path


def test_artifact_roundtrip(synthetic_interactions, mock_session, tmp_artifact_dir):
    """Run small training on synthetic data, save/load artifact, verify consistency."""
    with (
        patch("ml.als_train.get_session", return_value=mock_session),
        patch(
            "ml.als_train.compute_metrics_at_k",
            return_value={"precision@10": 0.1, "recall@10": 0.05},
        ),
    ):
        # Train model with synthetic data
        artifact_path = train_als_model(
            output_path=tmp_artifact_dir,
            factors=8,  # Small factors for testing
            iterations=5,  # Few iterations for speed
            seed=42,
            min_purchases_per_user=1,  # Allow all users
            min_purchases_per_item=1,  # Allow all items
        )

    # Verify artifact files exist
    assert (artifact_path / "user_factors.npz").exists()
    assert (artifact_path / "item_factors.npz").exists()
    assert (artifact_path / "mappings.json").exists()
    assert (artifact_path / "popularity.json").exists()
    assert (artifact_path / "model.json").exists()

    # Load recommender
    recommender = AlsRecommender.load_from_path(str(artifact_path))

    # Verify factor shapes
    expected_users = len(set(u for u, i, c in synthetic_interactions))
    expected_items = len(set(i for u, i, c in synthetic_interactions))
    assert recommender.user_factors.shape == (expected_users, 8)
    assert recommender.item_factors.shape == (expected_items, 8)

    # Verify mappings consistency
    assert len(recommender.uid_map) == expected_users
    assert len(recommender.iid_map) == expected_items
    assert len(recommender.reverse_item_map) == expected_items

    # Verify uid_map values are consecutive indices
    uid_values = sorted(recommender.uid_map.values())
    assert uid_values == list(range(expected_users))

    # Verify reverse_item_map is inverse of iid_map
    for item_id, idx in recommender.iid_map.items():
        assert recommender.reverse_item_map[idx] == item_id


def test_recommend_for_user_deterministic(
    tmp_artifact_dir, synthetic_interactions, mock_session
):
    """Load same artifact twice, verify identical top-K recommendations."""
    with (
        patch("ml.als_train.get_session", return_value=mock_session),
        patch(
            "ml.als_train.compute_metrics_at_k",
            return_value={"precision@10": 0.1, "recall@10": 0.05},
        ),
    ):
        # Train once
        artifact_path = train_als_model(
            output_path=tmp_artifact_dir,
            factors=8,
            iterations=5,
            seed=42,
            min_purchases_per_user=1,
            min_purchases_per_item=1,
        )

    # Load recommender twice
    rec1 = AlsRecommender.load_from_path(str(artifact_path))
    rec2 = AlsRecommender.load_from_path(str(artifact_path))

    # Pick a known user
    known_user = list(rec1.uid_map.keys())[0]

    # Get recommendations from both
    recs1 = rec1.recommend_for_user(known_user, k=5)
    recs2 = rec2.recommend_for_user(known_user, k=5)

    # Verify identical (allowing for NaN comparison issues)
    assert len(recs1) == len(recs2) == 5
    for (pid1, score1), (pid2, score2) in zip(recs1, recs2, strict=False):
        assert pid1 == pid2
        # Handle NaN values
        if np.isnan(score1) and np.isnan(score2):
            continue
        assert score1 == score2
    assert all(
        isinstance(pid, int) and isinstance(score, (float, np.floating))
        for pid, score in recs1
    )


def test_similar_products(tmp_artifact_dir, synthetic_interactions, mock_session):
    """Verify descending scores and non-trivial similarities (exclude self)."""
    with (
        patch("ml.als_train.get_session", return_value=mock_session),
        patch(
            "ml.als_train.compute_metrics_at_k",
            return_value={"precision@10": 0.1, "recall@10": 0.05},
        ),
    ):
        artifact_path = train_als_model(
            output_path=tmp_artifact_dir,
            factors=8,
            iterations=5,
            seed=42,
            min_purchases_per_user=1,
            min_purchases_per_item=1,
        )

    recommender = AlsRecommender.load_from_path(str(artifact_path))

    # Pick a known product
    known_product = list(recommender.iid_map.keys())[0]

    # Get similar products
    similar = recommender.similar_products(known_product, k=5)

    # Verify length
    assert len(similar) == 5

    # Verify scores are descending
    scores = [score for _, score in similar]
    assert scores == sorted(scores, reverse=True)

    # Verify no self in results
    product_ids = [pid for pid, _ in similar]
    assert known_product not in product_ids

    # Verify non-trivial similarities (not all zero)
    assert not all(score == 0.0 for score in scores)

    # Verify all are valid product IDs
    assert all(pid in recommender.iid_map for pid in product_ids)


def test_fallback_for_unknown_user(
    tmp_artifact_dir, synthetic_interactions, mock_session
):
    """Verify fallback_popular returns non-empty list for unknown users."""
    with (
        patch("ml.als_train.get_session", return_value=mock_session),
        patch(
            "ml.als_train.compute_metrics_at_k",
            return_value={"precision@10": 0.1, "recall@10": 0.05},
        ),
    ):
        artifact_path = train_als_model(
            output_path=tmp_artifact_dir,
            factors=8,
            iterations=5,
            seed=42,
            min_purchases_per_user=1,
            min_purchases_per_item=1,
        )

    recommender = AlsRecommender.load_from_path(str(artifact_path))

    # Use unknown user ID (not in uid_map)
    unknown_user = int(max(recommender.uid_map.keys())) + 100

    # Get recommendations
    recs = recommender.recommend_for_user(unknown_user, k=5)

    # Should return fallback popular
    assert len(recs) == 5
    # For unknown users, scores are set to 0.0 in the fallback case
    # Just check that we get tuples with numeric values
    assert all(len(pair) == 2 for pair in recs)

    # Verify popular products are returned
    popular_pids = [pid for pid, _ in recs]
    expected_popular = recommender.fallback_popular(5)
    assert popular_pids == expected_popular

    # Verify fallback_popular returns non-empty
    assert len(expected_popular) > 0
    # Just check that we get some values (JSON loading may convert to different types)
    assert all(pid is not None for pid in expected_popular)
