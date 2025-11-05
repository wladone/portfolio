"""Entity resolution core logic: best candidate search and scoring."""

from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd
from rapidfuzz import fuzz

from .config import AppConfig
from .normalize import normalize_name


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value)


def _score_pair(
    inp_norm: str,
    cand_norm: str,
    *,
    country_same: bool,
    country_bonus: float,
) -> float:
    if not inp_norm or not cand_norm:
        return 0.0
    score = fuzz.token_set_ratio(inp_norm, cand_norm) / 100.0
    if country_same and country_bonus > 0.0:
        score = min(1.0, score + country_bonus)
    return score


def entity_resolution(df: pd.DataFrame, cfg: AppConfig) -> pd.DataFrame:
    """Score candidates for each row and emit best-match metadata."""
    results: list[Dict[str, Any]] = []

    norm_kwargs = {
        "lower": bool(cfg.normalization.lower),
        "strip_punct": bool(cfg.normalization.strip_punct),
        "remove_legal_suffixes": bool(cfg.normalization.remove_legal_suffixes),
        "legal_suffixes": cfg.normalization.legal_suffixes or [],
    }
    country_bonus = float(cfg.thresholds.country_bonus)
    accept_threshold = float(cfg.thresholds.accept)
    maybe_threshold = float(cfg.thresholds.maybe)

    max_candidates = int(cfg.candidates.max_candidates)

    # Detect whether explicit candidate columns exist (cand1_name .. candN_name)
    candidate_columns_exist = any(
        cfg.candidates.name_pattern.format(n=n) in df.columns for n in range(1, max_candidates + 1)
    )

    # Define fallback candidate name columns (common fields found in the raw CSV)
    # Prefer legal -> commercial -> company -> domain (more specific first)
    fallback_name_cols = [
        "company_legal_names",
        "company_commercial_names",
        "company_name",
        "website_domain",
    ]
    # Fallback country column candidates
    fallback_country_cols = ["main_country", "main_country_code",
                             "input_main_country", "input_country", "country"]

    for idx, row in df.iterrows():
        input_name_raw = _safe_str(row.get(cfg.input.name_column, ""))
        input_name_norm = normalize_name(input_name_raw, **norm_kwargs)

        input_country_norm: Optional[str] = None
        if cfg.input.country_column and cfg.input.country_column in df.columns:
            country_raw = _safe_str(row.get(cfg.input.country_column, ""))
            country_clean = country_raw.strip().lower()
            if country_clean:
                input_country_norm = country_clean

        best_index: Optional[int] = None
        best_score: float = 0.0

        # Two modes: 1) explicit cand{n}_name columns exist (preferred)
        #            2) fallback mode: try a small set of likely columns from the raw CSV
        if candidate_columns_exist:
            for n in range(1, max_candidates + 1):
                name_col = cfg.candidates.name_pattern.format(n=n)
                if name_col not in df.columns:
                    continue

                cand_name_raw = _safe_str(row.get(name_col, ""))
                if not cand_name_raw.strip():
                    continue

                cand_name_norm = normalize_name(cand_name_raw, **norm_kwargs)
                cand_country_norm: Optional[str] = None

                if input_country_norm is not None:
                    cand_country_col = cfg.candidates.country_pattern.format(
                        n=n)
                    if cand_country_col in df.columns:
                        cand_country_norm = _safe_str(
                            row.get(cand_country_col, "")).strip().lower() or None

                score = _score_pair(
                    input_name_norm,
                    cand_name_norm,
                    country_same=(
                        cand_country_norm == input_country_norm) if input_country_norm else False,
                    country_bonus=country_bonus,
                )

                if score > best_score:
                    best_score = score
                    best_index = n
        else:
            # Fallback mode: use common company fields as candidates
            cand_idx = 0
            for col in fallback_name_cols:
                if cand_idx >= max_candidates:
                    break
                if col not in df.columns:
                    continue
                cand_idx += 1
                cand_name_raw = _safe_str(row.get(col, ""))
                if not cand_name_raw.strip():
                    continue

                cand_name_norm = normalize_name(cand_name_raw, **norm_kwargs)

                # Try to pick a sensible country for this candidate from fallback_country_cols
                cand_country_norm: Optional[str] = None
                for ccol in fallback_country_cols:
                    if ccol in df.columns:
                        cand_country_raw = _safe_str(
                            row.get(ccol, "")).strip().lower()
                        if cand_country_raw:
                            cand_country_norm = cand_country_raw
                            break

                score = _score_pair(
                    input_name_norm,
                    cand_name_norm,
                    country_same=(
                        cand_country_norm == input_country_norm) if input_country_norm else False,
                    country_bonus=country_bonus,
                )

                if score > best_score:
                    best_score = score
                    # record fallback index (1-based position within fallback candidates)
                    best_index = cand_idx

        if best_score >= accept_threshold:
            status = "accept"
        elif best_score >= maybe_threshold:
            status = "maybe"
        else:
            status = "unmatched"

        result: Dict[str, Any] = {
            "row_index": idx,
            "status": status,
            "score": round(best_score, 4),
        }

        if best_index is not None:
            result["best_cand_index"] = best_index
            if candidate_columns_exist:
                result["best_cand_name"] = row.get(
                    cfg.candidates.name_pattern.format(n=best_index))
                result["best_cand_country"] = row.get(
                    cfg.candidates.country_pattern.format(n=best_index))
                result["best_cand_id"] = row.get(
                    cfg.candidates.id_pattern.format(n=best_index))
            else:
                # Map fallback index back to the chosen fallback column (best_index is 1-based)
                fb_cols_available = [
                    c for c in fallback_name_cols if c in df.columns]
                if 0 < best_index <= len(fb_cols_available):
                    fb_col = fb_cols_available[best_index - 1]
                    result["best_cand_name"] = row.get(fb_col)
                    # try to pick a sensible id/country for fallback
                    result["best_cand_country"] = next(
                        (row.get(c) for c in fallback_country_cols if c in df.columns), None)
                    result["best_cand_id"] = row.get(
                        "veridion_id", None) if "veridion_id" in df.columns else None

        results.append(result)

    return pd.DataFrame(results)
