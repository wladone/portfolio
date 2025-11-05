"""Preprocess raw CSV to create cand{n}_name and cand{n}_country columns.

Usage:
  python scripts/preprocess_candidates.py --config config/config.yaml --out data/processed/preprocessed_input.csv

This script reads the configured input CSV and writes a copy where common
company fields are promoted to candidate columns expected by the ER engine.
No new dependencies are required beyond pandas (already in requirements).
"""

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

from src.veridion_poc.config import load_config


def build_candidates(df: pd.DataFrame, max_candidates: int = 5) -> pd.DataFrame:
    # Candidate source columns in priority order
    src_cols = [
        "company_legal_names",
        "company_commercial_names",
        "company_name",
        "website_domain",
    ]

    out = df.copy()

    cand_i = 1
    for col in src_cols:
        if cand_i > max_candidates:
            break
        if col in df.columns:
            out[f"cand{cand_i}_name"] = df[col]
            # Try to pick a country for the candidate from common fields
            country_col = None
            for c in ("main_country", "main_country_code", "input_main_country", "input_country"):
                if c in df.columns:
                    country_col = c
                    break
            out[f"cand{cand_i}_country"] = df[country_col] if country_col is not None else None
            cand_i += 1

    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--out", default="data/processed/preprocessed_input.csv")
    args = ap.parse_args()

    cfg = load_config(args.config)
    inp = Path(cfg.input.path)
    df = pd.read_csv(inp)

    df2 = build_candidates(df, max_candidates=int(
        cfg.candidates.max_candidates))
    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    df2.to_csv(outp, index=False)
    print("Wrote preprocessed input:", outp.resolve())


if __name__ == "__main__":
    main()
