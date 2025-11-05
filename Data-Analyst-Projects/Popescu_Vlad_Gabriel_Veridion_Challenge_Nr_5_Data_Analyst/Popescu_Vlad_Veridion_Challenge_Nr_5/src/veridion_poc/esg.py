"""Optional ESG hints via keyword search, no external data."""

from __future__ import annotations

from typing import List, Optional

import pandas as pd

from .config import AppConfig

_ESG_TERMS = [
    "solar",
    "wind",
    "renewable",
    "recycling",
    "circular",
    "hydro",
    "geothermal",
    "ev",
    "battery",
    "bio",
    "green",
]


def _safe_best_index(value: object) -> Optional[int]:
    if value is None:
        return None
    try:
        if isinstance(value, float) and pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _get_source_row(df: pd.DataFrame, row_index: object) -> Optional[pd.Series]:
    if row_index is None:
        return None
    if row_index in df.index:
        return df.loc[row_index]
    try:
        pos = int(row_index)
    except (TypeError, ValueError):
        return None
    if 0 <= pos < len(df):
        return df.iloc[pos]
    return None


def _find_terms(text: str) -> List[str]:
    if not isinstance(text, str):
        return []
    lowered = text.lower()
    matches: List[str] = []
    seen = set()
    for term in _ESG_TERMS:
        if term in lowered and term not in seen:
            matches.append(term)
            seen.add(term)
    return matches


def maybe_esg(df: pd.DataFrame, er_df: pd.DataFrame, cfg: AppConfig) -> pd.DataFrame:
    if not getattr(cfg.features, "esg", False):
        return er_df

    results = er_df.copy()
    esg_hint, esg_terms, esg_scores = [], [], []

    name_pattern = cfg.candidates.name_pattern
    domain_pattern = cfg.candidates.domain_pattern
    input_name_col = cfg.input.name_column

    for _, er_row in er_df.iterrows():
        best_idx = _safe_best_index(er_row.get("best_cand_index"))
        src_row = _get_source_row(df, er_row.get("row_index"))

        cand_name = ""
        cand_domain = ""

        if best_idx and src_row is not None:
            ncol = name_pattern.format(n=best_idx)
            dcol = domain_pattern.format(n=best_idx)
            if ncol in src_row.index:
                cand_name = str(src_row.get(ncol, "") or "")
            if dcol in src_row.index:
                cand_domain = str(src_row.get(dcol, "") or "")

        if not cand_name and src_row is not None and input_name_col in src_row.index:
            cand_name = str(src_row.get(input_name_col, "") or "")

        terms = _find_terms(cand_name) + _find_terms(cand_domain)
        dedup = []
        seen = set()
        for term in terms:
            if term not in seen:
                dedup.append(term)
                seen.add(term)

        esg_hint.append(bool(dedup))
        esg_terms.append("|".join(dedup))
        esg_scores.append(min(1.0, len(dedup) / 5.0))

    results["esg_hint"] = esg_hint
    results["esg_match_terms"] = esg_terms
    results["esg_score"] = esg_scores
    return results
