"""Optional procurement segmentation via simple keyword heuristics."""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .config import AppConfig
from .normalize import normalize_name

_SEGMENTS: Dict[str, List[str]] = {
    "Machinery": ["machine", "machinery", "equipment", "industrial", "automation"],
    "Chemicals": ["chemical", "chem", "polymer", "coating", "adhesive", "solvent"],
    "IT/Software": ["software", "cloud", "data", "digital", "system", "it", "ai"],
    "Logistics": ["logistic", "transport", "shipping", "freight", "cargo"],
    "Energy": ["energy", "power", "solar", "wind", "hydro", "battery"],
    "Construction": ["construct", "building", "civil", "infrastructure", "cement"],
}


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


def _domain_core(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        return ""
    host = value.strip().lower()
    host = re.sub(r"^https?://", "", host)
    host = host.split("/")[0]
    host = host.removeprefix("www.")
    parts = host.split(".")
    if len(parts) >= 3:
        return parts[-3]
    if len(parts) >= 2:
        return parts[-2]
    return parts[0] if parts else ""


def _score_segment(texts: List[str]) -> Tuple[str, float]:
    best_segment = "Unknown"
    best_score = 0
    for segment, keywords in _SEGMENTS.items():
        score = 0
        for kw in keywords:
            for text in texts:
                if kw in text:
                    score += 1
        if score > best_score:
            best_score = score
            best_segment = segment
    confidence = min(1.0, best_score / 3.0) if best_score else 0.0
    return best_segment, confidence


def maybe_segments(df: pd.DataFrame, er_df: pd.DataFrame, cfg: AppConfig) -> pd.DataFrame:
    if not getattr(cfg.features, "procurement_segments", False):
        return er_df

    norm_kwargs = {
        "lower": bool(cfg.normalization.lower),
        "strip_punct": bool(cfg.normalization.strip_punct),
        "remove_legal_suffixes": bool(cfg.normalization.remove_legal_suffixes),
        "legal_suffixes": cfg.normalization.legal_suffixes or [],
    }

    name_pattern = cfg.candidates.name_pattern
    domain_pattern = cfg.candidates.domain_pattern
    input_name_col = cfg.input.name_column

    segments, confidences = [], []

    for _, er_row in er_df.iterrows():
        best_idx = _safe_best_index(er_row.get("best_cand_index"))
        src_row = _get_source_row(df, er_row.get("row_index"))

        cand_name = ""
        cand_domain = ""

        if best_idx and src_row is not None:
            name_col = name_pattern.format(n=best_idx)
            domain_col = domain_pattern.format(n=best_idx)
            if name_col in src_row.index:
                cand_name = str(src_row.get(name_col, "") or "")
            if domain_col in src_row.index:
                cand_domain = str(src_row.get(domain_col, "") or "")

        if not cand_name and src_row is not None and input_name_col in src_row.index:
            cand_name = str(src_row.get(input_name_col, "") or "")

        normalized_name = normalize_name(cand_name, **norm_kwargs)
        domain_core = _domain_core(cand_domain)

        segment, confidence = _score_segment([normalized_name, domain_core])
        segments.append(segment)
        confidences.append(confidence)

    result = er_df.copy()
    result["segment"] = segments
    result["segment_confidence"] = confidences
    return result
