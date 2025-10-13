"""Similarity scoring helpers that compare product records."""

from __future__ import annotations
import difflib
from typing import List
import pandas as pd

from utils import tokenize, normalize_text, model_tokens, canonical_gtin


def jaccard(a: List[str], b: List[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def pair_score(r1: pd.Series, r2: pd.Series) -> float:
    # Exact ID match → 1.0
    ids1 = [canonical_gtin(r1.get(k, "")) for k in ("gtin", "ean", "upc")]
    ids2 = [canonical_gtin(r2.get(k, "")) for k in ("gtin", "ean", "upc")]
    if set([i for i in ids1 if i]) & set([j for j in ids2 if j]):
        return 1.0

    name1 = normalize_text(r1.get("name", "") or r1.get("title", ""))
    name2 = normalize_text(r2.get("name", "") or r2.get("title", ""))
    brand1 = normalize_text(r1.get("brand", ""))
    brand2 = normalize_text(r2.get("brand", ""))
    model1 = normalize_text(r1.get("model", ""))
    model2 = normalize_text(r2.get("model", ""))

    tok1, tok2 = tokenize(name1), tokenize(name2)
    mt1 = model_tokens(name1) + tokenize(model1)
    mt2 = model_tokens(name2) + tokenize(model2)

    s_name_tok = jaccard(tok1, tok2)
    s_name_ratio = difflib.SequenceMatcher(
        None, name1, name2).ratio() if name1 and name2 else 0.0
    s_brand = 1.0 if brand1 and brand1 == brand2 else 0.0
    s_model = jaccard(mt1, mt2)

    return (
        0.40 * s_name_tok +
        0.20 * s_name_ratio +
        0.30 * s_model +
        0.10 * s_brand
    )

