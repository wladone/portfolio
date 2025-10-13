"""High-level deduplication and assignment helpers built from core components."""

from __future__ import annotations
import pandas as pd

from cluster import build_pairs, connected_components
from aggregate import aggregate_cluster

DEFAULT_THRESHOLD = 0.80


def deduplicate(df: pd.DataFrame, threshold: float = DEFAULT_THRESHOLD) -> pd.DataFrame:
    pairs = build_pairs(df, threshold=threshold)
    comps = connected_components(pairs)
    clustered = set(x for comp in comps for x in comp)
    singletons = [i for i in df.index if i not in clustered]
    comps.extend([[i] for i in singletons])

    records = [aggregate_cluster(df.loc[comp]) for comp in comps]
    return pd.DataFrame(records)


def assign_clusters(df: pd.DataFrame, dedup_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in dedup_df.iterrows():
        for idx in r["original_indices"]:
            rows.append(
                {"row_index": idx, "product_uid": r.get("product_uid")})
    return pd.DataFrame(rows)

