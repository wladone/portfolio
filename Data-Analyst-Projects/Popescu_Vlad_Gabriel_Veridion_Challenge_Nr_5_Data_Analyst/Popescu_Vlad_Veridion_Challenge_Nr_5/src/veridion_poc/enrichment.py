"""Optional enrichment hook (no external APIs)."""

from __future__ import annotations

import re
from typing import Optional, Tuple

import pandas as pd

from .config import AppConfig

_DOMAIN_RE = re.compile(r"^(?:https?://)?([^/]+)")


def _safe_best_index(value: object) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int,)):
        return int(value)
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


def _extract_host(domain: str) -> str:
    if not isinstance(domain, str):
        return ""
    domain = domain.strip()
    if not domain:
        return ""
    match = _DOMAIN_RE.match(domain.lower())
    return match.group(1) if match else domain.lower()


def _split_domain(host: str) -> Tuple[str, str]:
    if not host:
        return "", ""
    host = host.removeprefix("www.")
    parts = host.split(".")
    if len(parts) >= 2:
        tld = parts[-1]
        if len(parts) >= 3:
            root = parts[-3]
        else:
            root = parts[-2]
        return root, tld
    return host, ""


def maybe_enrich(df: pd.DataFrame, er_df: pd.DataFrame, cfg: AppConfig) -> pd.DataFrame:
    """Append enrichment columns based on best candidate domain."""
    if not getattr(cfg.features, "enrichment", False):
        return er_df

    domain_pattern = cfg.candidates.domain_pattern
    results = er_df.copy()

    roots, tlds, has_website = [], [], []

    for _, er_row in er_df.iterrows():
        best_idx = _safe_best_index(er_row.get("best_cand_index"))
        src_row = _get_source_row(df, er_row.get("row_index"))
        domain_value = ""

        if best_idx and src_row is not None:
            dom_col = domain_pattern.format(n=best_idx)
            if dom_col in src_row.index:
                domain_value = src_row.get(dom_col, "") or ""
        if not domain_value and src_row is not None:
            # attempt to fall back to best_cand_domain column if present in ER DF
            domain_value = er_row.get("best_cand_domain", "") or ""

        host = _extract_host(str(domain_value))
        root, tld = _split_domain(host)
        roots.append(root)
        tlds.append(tld)
        has_website.append(bool(host))

    results["domain_root"] = roots
    results["domain_tld"] = tlds
    results["has_website"] = has_website
    return results
