"""Data quality helpers for the Veridion POC pipeline."""

from __future__ import annotations

from typing import List

import pandas as pd

from .config import AppConfig
from .normalize import normalize_name


def _norm_kwargs(cfg: AppConfig) -> dict[str, object]:
    return {
        "lower": bool(cfg.normalization.lower),
        "strip_punct": bool(cfg.normalization.strip_punct),
        "remove_legal_suffixes": bool(cfg.normalization.remove_legal_suffixes),
        "legal_suffixes": cfg.normalization.legal_suffixes or [],
    }


def _safe_str(value: object) -> str:
    if isinstance(value, str):
        return value
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value)


def detect_duplicates(df: pd.DataFrame, cfg: AppConfig) -> pd.DataFrame:
    """Group rows by normalized name (+ optional country) and surface collisions."""
    if df.empty or cfg.input.name_column not in df.columns:
        return pd.DataFrame(columns=["key", "row_indices", "count"])

    norm_args = _norm_kwargs(cfg)
    name_norm = df[cfg.input.name_column].apply(lambda v: normalize_name(_safe_str(v), **norm_args))

    if cfg.input.country_column and cfg.input.country_column in df.columns:
        country_norm = df[cfg.input.country_column].apply(lambda v: _safe_str(v).strip().lower())
        key = name_norm + "|" + country_norm
    else:
        key = name_norm

    tmp = pd.DataFrame({"key": key})
    tmp["row_index"] = df.index

    grouped = (
        tmp.groupby("key", dropna=False)["row_index"]
        .apply(list)
        .reset_index()
        .rename(columns={"row_index": "row_indices"})
    )
    grouped["count"] = grouped["row_indices"].apply(len)

    duplicates = grouped.loc[grouped["count"] > 1].sort_values(
        by=["count", "key"], ascending=[False, True], ignore_index=True
    )

    if duplicates.empty:
        return pd.DataFrame(columns=["key", "row_indices", "count"])

    return duplicates[["key", "row_indices", "count"]]


def missingness(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Compute missing counts/percentages for requested columns."""
    if not cols:
        return pd.DataFrame(columns=["column", "missing", "missing_pct"])

    n = len(df)
    results: list[dict[str, object]] = []
    seen: set[str] = set()

    for col in cols:
        if col is None or col in seen:
            continue
        seen.add(col)

        if col in df.columns:
            missing = int(df[col].isna().sum())
        else:
            missing = n

        pct = round((missing / n * 100.0) if n else 0.0, 2)
        results.append({"column": col, "missing": missing, "missing_pct": pct})

    return pd.DataFrame(results, columns=["column", "missing", "missing_pct"])
