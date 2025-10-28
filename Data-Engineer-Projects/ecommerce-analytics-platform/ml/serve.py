"""ALS recommendation serving."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

import numpy as np

from ml.settings import get_settings

TOPK_DEFAULT = get_settings().topk_default


def _topk_by_dot(
    query: np.ndarray, matrix: np.ndarray, k: int
) -> list[tuple[int, float]]:
    """Compute top-K by dot product using efficient argpartition."""
    scores = np.dot(query, matrix.T)
    if k >= len(scores):
        top_indices = np.argsort(scores)[::-1]
    else:
        top_indices = np.argpartition(scores, -k)[-k:]
        top_indices = top_indices[np.argsort(scores[top_indices])[::-1]]
    return [(int(idx), float(scores[idx])) for idx in top_indices]


class AlsRecommender:
    """ALS-based recommender system."""

    def __init__(
        self,
        user_factors: np.ndarray,
        item_factors: np.ndarray,
        uid_map: dict[int, int],
        iid_map: dict[int, int],
        reverse_item_map: dict[int, int],
        popularity: dict[int, int],
    ):
        """Initialize recommender with pre-trained factors and mappings."""
        self.user_factors = user_factors / np.linalg.norm(
            user_factors, axis=1, keepdims=True
        )
        self.item_factors = item_factors / np.linalg.norm(
            item_factors, axis=1, keepdims=True
        )
        self.uid_map = uid_map
        self.iid_map = iid_map
        self.reverse_item_map = reverse_item_map
        self.popularity = popularity

    @classmethod
    def load_from_path(cls, path: str) -> AlsRecommender:
        """Load recommender from specific artifact path."""
        artifact_dir = Path(path)

        # Load factors
        user_factors = np.load(artifact_dir / "user_factors.npz")["arr_0"]
        item_factors = np.load(artifact_dir / "item_factors.npz")["arr_0"]

        # Load mappings
        with open(artifact_dir / "mappings.json") as f:
            mappings = json.load(f)
        uid_map = {int(k): int(v) for k, v in mappings["uid_map"].items()}
        iid_map = {int(k): int(v) for k, v in mappings["iid_map"].items()}
        reverse_item_map = {
            int(k): int(v) for k, v in mappings["reverse_item_map"].items()
        }

        # Load popularity
        with open(artifact_dir / "popularity.json") as f:
            popularity = json.load(f)

        return cls(
            user_factors, item_factors, uid_map, iid_map, reverse_item_map, popularity
        )

    @classmethod
    def load_latest(cls, artifact_dir: str) -> AlsRecommender:
        """Load most recent valid artifact directory."""
        base_dir = Path(artifact_dir)
        candidates = []
        for subdir in base_dir.iterdir():
            if subdir.is_dir() and (subdir / "model.json").exists():
                candidates.append(subdir)

        if not candidates:
            raise ValueError(f"No valid artifacts found in {artifact_dir}")

        # Sort by creation time (most recent first)
        candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        latest = candidates[0]

        return cls.load_from_path(str(latest))

    def recommend_for_user(
        self, user_db_id: int, k: int = TOPK_DEFAULT, exclude_seen: bool = True
    ) -> list[tuple[int, float]]:
        """Return product recommendations with scores for a user."""
        if user_db_id not in self.uid_map:
            return [(pid, 0.0) for pid in self.fallback_popular(k)]

        user_idx = self.uid_map[user_db_id]
        user_factor = self.user_factors[user_idx]

        # Compute scores for all items
        recommendations = _topk_by_dot(user_factor, self.item_factors, k)

        # Convert internal indices to DB IDs
        result = [(self.reverse_item_map[idx], score) for idx, score in recommendations]

        # TODO: Implement exclude_seen when DB integration is available
        if exclude_seen:
            pass  # Placeholder

        return result

    def similar_products(
        self, product_db_id: int, k: int = TOPK_DEFAULT
    ) -> list[tuple[int, float]]:
        """Return similar products with scores."""
        if product_db_id not in self.iid_map:
            return []

        item_idx = self.iid_map[product_db_id]
        item_factor = self.item_factors[item_idx]

        # Compute similarities, exclude self
        similarities = _topk_by_dot(item_factor, self.item_factors, k + 1)
        similarities = [(idx, score) for idx, score in similarities if idx != item_idx][
            :k
        ]

        # Convert to DB IDs
        result = [(self.reverse_item_map[idx], score) for idx, score in similarities]

        return result

    def fallback_popular(
        self, k: int, strategy: Literal["items", "net"] = "items"
    ) -> list[int]:
        """Return popular products for cold-start scenarios."""
        if strategy == "items":
            # Sort by interaction count descending
            sorted_items = sorted(
                self.popularity.items(), key=lambda x: x[1], reverse=True
            )
            return [pid for pid, _ in sorted_items[:k]]
        else:
            # Placeholder for "net" strategy if needed
            raise NotImplementedError("Strategy 'net' not implemented")
