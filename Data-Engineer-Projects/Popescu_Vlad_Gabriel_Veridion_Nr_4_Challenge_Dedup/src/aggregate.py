"""Aggregate raw record clusters into canonicalized product rows."""

from __future__ import annotations
from collections import Counter, defaultdict
from typing import Any, Dict
import pandas as pd

from utils import normalize_text, canonical_gtin, stable_uid


def _best_text(df: pd.DataFrame, col: str) -> str | None:
    if col not in df.columns:
        return None
    vals = [str(x) for x in df[col].dropna().astype(str) if str(x).strip()]
    if not vals:
        return None
    norm_groups = defaultdict(list)
    for v in vals:
        norm_groups[normalize_text(v)].append(v)
    # Majority by normalized form; tie-break by longest representative
    majority_norm, reps = max(
        norm_groups.items(), key=lambda kv: (len(kv[1]), len(max(kv[1], key=len)))
    )
    return max(reps, key=len)


def aggregate_cluster(df: pd.DataFrame) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    out["original_count"] = len(df)
    out["original_indices"] = df.index.tolist()

    for col in ["name", "title", "brand", "model", "description"]:
        out[col] = _best_text(df, col)

    # identifiers
    for k in ["gtin", "ean", "upc", "sku"]:
        if k in df.columns:
            vals = [str(v)
                    for v in df[k].dropna().astype(str) if str(v).strip()]
            if k in {"gtin", "ean", "upc"}:
                canon = [canonical_gtin(v) for v in vals if canonical_gtin(v)]
                if canon:
                    modal = Counter(canon).most_common(1)[0][0]
                    for v in vals:
                        if canonical_gtin(v) == modal:
                            out[k] = v
                            break
                else:
                    out[k] = None
            else:
                out[k] = Counter(vals).most_common(1)[0][0] if vals else None

    # URLs/images
    for k in ["url", "product_url", "image", "image_url", "images"]:
        if k in df.columns:
            uniq = list(dict.fromkeys(
                [str(v) for v in df[k].dropna().astype(str) if str(v).strip()]))
            if uniq:
                out[k + "_list"] = uniq[:100]

    # prices
    for k in ["price", "amount", "price_value"]:
        if k in df.columns:
            vals = pd.to_numeric(df[k], errors="coerce").dropna()
            if len(vals):
                out["price_min"] = float(vals.min())
                out["price_median"] = float(vals.median())
                out["price_max"] = float(vals.max())
                break

    # stable product UID
    id_key = (
        canonical_gtin(out.get("gtin"))
        or canonical_gtin(out.get("ean"))
        or canonical_gtin(out.get("upc"))
    )
    b = out.get("brand", "")
    m = out.get("model", "")
    b_norm, m_norm = normalize_text(b), normalize_text(m)
    bm_key = (b_norm + "|" + m_norm) if (b_norm or m_norm) else ""
    name_key = normalize_text(out.get("name", "") or out.get("title", ""))
    key = id_key or bm_key or name_key
    out["product_uid"] = stable_uid(key) if key else None

    out["cluster_size"] = out["original_count"]
    return out

