"""ALS training pipeline with CLI interface."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import implicit.als
import numpy as np
import structlog
from implicit.evaluation import ranking_metrics_at_k, train_test_split
from scipy.sparse import csr_matrix
from sqlalchemy import func, select

from backend.app.core.db import get_session
from backend.app.models.fact_sales import FactSales
from ml.settings import get_settings

logger = structlog.get_logger(__name__)


def compute_metrics_at_k(
    model: implicit.als.AlternatingLeastSquares,
    train: csr_matrix,
    test: csr_matrix,
    k: int,
) -> dict[str, float]:
    """Compute ranking metrics at K."""
    return ranking_metrics_at_k(model, train, test, K=k, show_progress=False)


def train_als_model(
    output_path: Path | None = None,
    factors: int | None = None,
    regularization: float | None = None,
    iterations: int | None = None,
    alpha: float | None = None,
    seed: int | None = None,
    min_purchases_per_user: int | None = None,
    min_purchases_per_item: int | None = None,
    lookback_days: int | None = None,
) -> Path:
    """Train ALS model and export artifacts."""
    settings = get_settings()

    # Override settings with provided params
    factors = factors or settings.als_factors
    regularization = regularization or settings.als_reg
    iterations = iterations or settings.als_iter
    alpha = alpha or settings.als_alpha
    seed = seed or settings.als_seed
    min_purchases_per_user = (
        min_purchases_per_user or settings.als_min_purchases_per_user
    )
    min_purchases_per_item = (
        min_purchases_per_item or settings.als_min_purchases_per_item
    )
    lookback_days = lookback_days or settings.als_lookback_days

    logger.info(
        "Starting ALS training",
        factors=factors,
        regularization=regularization,
        iterations=iterations,
        alpha=alpha,
        seed=seed,
        min_purchases_per_user=min_purchases_per_user,
        min_purchases_per_item=min_purchases_per_item,
        lookback_days=lookback_days,
    )

    # Extract data from database
    session = next(get_session())
    try:
        query = select(
            FactSales.customer_id,
            FactSales.product_id,
            func.sum(FactSales.quantity).label("interactions"),
        )
        if lookback_days:
            cutoff = datetime.now() - timedelta(days=lookback_days)
            query = query.where(FactSales.transaction_ts >= cutoff)
        query = query.group_by(FactSales.customer_id, FactSales.product_id)
        data = session.execute(query).all()
    finally:
        session.close()

    logger.info("Extracted interactions", count=len(data))

    # Aggregate interactions
    interactions: dict[tuple[int, int], int] = {}
    user_counts: dict[int, int] = {}
    item_counts: dict[int, int] = {}
    for row in data:
        uid, iid, count = row
        interactions[(uid, iid)] = count
        user_counts[uid] = user_counts.get(uid, 0) + count
        item_counts[iid] = item_counts.get(iid, 0) + count

    # Filter users and items
    filtered_interactions = {
        (u, i): c
        for (u, i), c in interactions.items()
        if user_counts[u] >= min_purchases_per_user
        and item_counts[i] >= min_purchases_per_item
    }

    logger.info(
        "Filtered interactions",
        original_count=len(interactions),
        filtered_count=len(filtered_interactions),
    )

    # Create compact mappings
    users = sorted(set(u for u, i in filtered_interactions))
    items = sorted(set(i for u, i in filtered_interactions))
    uid_map = {u: idx for idx, u in enumerate(users)}
    iid_map = {i: idx for idx, i in enumerate(items)}
    reverse_item_map = {v: k for k, v in iid_map.items()}

    logger.info("Created mappings", num_users=len(users), num_items=len(items))

    # Build CSR matrix
    row = [uid_map[u] for u, i in filtered_interactions]
    col = [iid_map[i] for u, i in filtered_interactions]
    data_vals = list(filtered_interactions.values())
    matrix = csr_matrix((data_vals, (row, col)), shape=(len(users), len(items)))

    # Train-test split
    train, test = train_test_split(matrix, train_percentage=0.8, random_state=seed)

    # Train ALS model
    model = implicit.als.AlternatingLeastSquares(
        factors=factors,
        regularization=regularization,
        iterations=iterations,
        random_state=seed,
    )
    model.fit(matrix)  # Use full matrix instead of train split for now

    logger.info("ALS training completed")

    # Skip evaluation for now
    metrics = {"skipped": True}

    # Artifact export
    if output_path:
        artifact_dir = output_path
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        artifact_dir = Path(settings.als_artifact_dir) / f"als_model_{timestamp}"

    artifact_dir.mkdir(parents=True, exist_ok=True)

    # Save factors
    np.savez(artifact_dir / "user_factors.npz", model.user_factors)
    np.savez(artifact_dir / "item_factors.npz", model.item_factors)

    # Save mappings
    mappings = {
        "uid_map": {int(k): int(v) for k, v in uid_map.items()},
        "iid_map": {int(k): int(v) for k, v in iid_map.items()},
        "reverse_item_map": {int(k): int(v) for k, v in reverse_item_map.items()},
    }
    with open(artifact_dir / "mappings.json", "w") as f:
        json.dump(mappings, f)

    # Save popularity
    popularity = {
        int(reverse_item_map[i]): int(item_counts[reverse_item_map[i]])
        for i in range(len(items))
    }
    with open(artifact_dir / "popularity.json", "w") as f:
        json.dump(popularity, f)

    # Save model metadata with checksums
    model_meta: dict[str, Any] = {
        "factors": factors,
        "regularization": regularization,
        "iterations": iterations,
        "alpha": alpha,
        "seed": seed,
        "min_purchases_per_user": min_purchases_per_user,
        "min_purchases_per_item": min_purchases_per_item,
        "lookback_days": lookback_days,
        "metrics": metrics,
        "created_at": datetime.now().isoformat(),
    }

    # Compute checksums
    def compute_checksum(file_path: Path) -> str:
        with open(file_path, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()

    model_meta["user_factors_checksum"] = compute_checksum(
        artifact_dir / "user_factors.npz"
    )
    model_meta["item_factors_checksum"] = compute_checksum(
        artifact_dir / "item_factors.npz"
    )

    with open(artifact_dir / "model.json", "w") as f:
        json.dump(model_meta, f)

    logger.info("Artifacts exported", artifact_dir=str(artifact_dir))

    return artifact_dir


def main(argv: list[str] | None = None) -> None:
    """CLI entry point."""
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Train ALS recommendation model")
    parser.add_argument(
        "--factors",
        type=int,
        default=settings.als_factors,
        help="Number of latent factors",
    )
    parser.add_argument(
        "--regularization",
        type=float,
        default=settings.als_reg,
        help="Regularization parameter",
    )
    parser.add_argument(
        "--iterations", type=int, default=settings.als_iter, help="Number of iterations"
    )
    parser.add_argument(
        "--alpha",
        type=float,
        default=settings.als_alpha,
        help="Alpha for implicit feedback",
    )
    parser.add_argument(
        "--seed", type=int, default=settings.als_seed, help="Random seed"
    )
    parser.add_argument(
        "--min-purchases-per-user",
        type=int,
        default=settings.als_min_purchases_per_user,
        help="Min purchases per user",
    )
    parser.add_argument(
        "--min-purchases-per-item",
        type=int,
        default=settings.als_min_purchases_per_item,
        help="Min purchases per item",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=settings.als_lookback_days,
        help="Lookback days for data",
    )
    parser.add_argument(
        "--output-path", type=Path, default=None, help="Output directory path"
    )

    args = parser.parse_args(argv)

    train_als_model(
        output_path=args.output_path,
        factors=args.factors,
        regularization=args.regularization,
        iterations=args.iterations,
        alpha=args.alpha,
        seed=args.seed,
        min_purchases_per_user=args.min_purchases_per_user,
        min_purchases_per_item=args.min_purchases_per_item,
        lookback_days=args.lookback_days,
    )


if __name__ == "__main__":
    main()
