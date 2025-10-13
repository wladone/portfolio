"""Blocking heuristics for grouping similar product rows before scoring."""

from __future__ import annotations
from typing import List
import pandas as pd

from utils import normalize_text, model_tokens, canonical_gtin


def block_keys(row: pd.Series) -> List[str]:
    keys: List[str] = []

    gtin = (
        canonical_gtin(row.get("gtin", ""))
        or canonical_gtin(row.get("ean", ""))
        or canonical_gtin(row.get("upc", ""))
    )
    if gtin:
        keys.append(f"GTIN:{gtin}")

    brand = normalize_text(row.get("brand", ""))
    model = normalize_text(row.get("model", ""))
    if brand and model:
        keys.append(f"BM:{brand}:{model}")

    name = row.get("name", "") or row.get("title", "") or ""
    name_norm = normalize_text(name)
    toks = name_norm.split()
    if len(toks) >= 3:
        keys.append("NAME3:" + ":".join(toks[:3]))

    mtoks = model_tokens(name)
    if brand and mtoks:
        keys.append("B-MTOK:" + brand + ":" + ":".join(sorted(set(mtoks))[:2]))

    if name_norm:
        keys.append("NAMEx:" + ":".join(sorted(set(toks))[:5]))

    # dedup preserve order
    seen = set()
    out = []
    for k in keys:
        if k not in seen:
            out.append(k)
            seen.add(k)
    return out

