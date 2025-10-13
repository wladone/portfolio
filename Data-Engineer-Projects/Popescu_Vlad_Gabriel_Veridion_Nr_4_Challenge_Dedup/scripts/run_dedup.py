"""CLI entry point for the deduplication pipeline plus summary reporting."""
from __future__ import annotations
from utils import canonical_gtin
from pipeline import deduplicate, assign_clusters

import argparse
import os
import sys
import pandas as pd

# Ensure flat src/ is importable BEFORE importing pipeline
this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(os.path.dirname(this_dir), "src"))


try:
    from reporting import generate_visual_reports
except ImportError:  # pragma: no cover - optional dependency guard
    generate_visual_reports = None


def read_any(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in {".parquet", ".pq", ".snappy"}:
        return pd.read_parquet(path)
    elif ext in {".csv", ".tsv"}:
        return pd.read_csv(path)
    else:
        try:
            return pd.read_parquet(path)
        except Exception:
            return pd.read_csv(path)


def canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map common real-world column names to the pipeline's expected names."""
    if df is None or not len(df.columns):
        return df
    df = df.copy()
    # Case-insensitive lookup
    lower = {c.lower(): c for c in df.columns}

    def first_present(cands):
        for c in cands:
            lc = c.lower()
            if lc in lower:
                return lower[lc]
        return None

    renames = {}
    mapping = {
        "name": ["name", "product_name"],
        "title": ["title", "product_title"],
        "brand": ["brand", "brand_name"],
        "model": ["model", "model_number", "mpn"],
        "description": ["description", "product_summary", "summary"],
        "price": ["price", "amount", "price_value", "sale_price"],
        "product_url": ["product_url", "page_url"],
        "image_url": ["image_url", "image"],
    }
    for canon, cands in mapping.items():
        if canon not in df.columns:
            src = first_present(cands)
            if src and src != canon:
                renames[src] = canon

    if renames:
        df = df.rename(columns=renames)

    # Identify code columns
    gtin_src = first_present(["gtin", "gtin13", "gtin_13"]) or None
    ean_src = first_present(["ean", "ean13"]) or None
    upc_src = first_present(["upc", "upc12"]) or None

    # Derive from product_identifier if possible
    pid_col = first_present(["product_identifier"]) or None
    if pid_col:
        series = df[pid_col].astype(str)
        # Attempt to populate gtin if not present
        if "gtin" not in df.columns and not gtin_src:
            cand = series.map(lambda x: canonical_gtin(x))
            if cand.fillna("").astype(str).str.len().gt(0).any():
                df["gtin"] = cand.replace({"": None})
        # If ean missing, use same canonical digits (some datasets mix naming)
        if "ean" not in df.columns and not ean_src:
            cand = series.map(lambda x: canonical_gtin(x))
            if cand.fillna("").astype(str).str.len().gt(0).any():
                df["ean"] = cand.replace({"": None})
        # Otherwise, treat as SKU fallback if nothing canonical
        if "sku" not in df.columns:
            df["sku"] = series.where(series.str.strip().ne(""))

    # Ensure explicit gtin/ean/upc columns are present if their sources exist under alternate names
    if gtin_src and gtin_src != "gtin" and "gtin" not in df.columns:
        df["gtin"] = df[gtin_src]
    if ean_src and ean_src != "ean" and "ean" not in df.columns:
        df["ean"] = df[ean_src]
    if upc_src and upc_src != "upc" and "upc" not in df.columns:
        df["upc"] = df[upc_src]

    return df


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Veridion product deduplication (flat src/ modules)"
    )
    parser.add_argument("--input", required=True,
                        help="Input parquet/csv path")
    parser.add_argument("--out-dir", default="data/processed",
                        help="Output directory")
    parser.add_argument("--threshold", type=float,
                        default=0.80, help="Match score threshold [0..1]")
    parser.add_argument(
        "--skip-reports",
        action="store_true",
        help="Skip generating Excel and visualization files",
    )
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    df = read_any(args.input)
    df = canonicalize_columns(df)
    print(f"Loaded input: {df.shape} | columns={list(df.columns)[:12]}...")

    dedup_df = deduplicate(df, threshold=args.threshold)
    map_df = assign_clusters(df, dedup_df)

    # Save outputs
    out_csv = os.path.join(args.out_dir, "dedup.csv")
    dedup_df.to_csv(out_csv, index=False)
    try:
        dedup_df.to_parquet(os.path.join(
            args.out_dir, "dedup.parquet"), index=False)
    except Exception:
        pass

    map_df.to_csv(os.path.join(
        args.out_dir, "cluster_assignments.csv"), index=False)

    print(f"Wrote: {out_csv} and mapping CSV")

    if args.skip_reports:
        print("Reports skipped (--skip-reports).")
    else:
        if generate_visual_reports is None:
            print("Install optional reporting dependencies to enable Excel and charts.")
        else:
            generate_visual_reports(dedup_df, map_df, args.out_dir)
            print("Created Excel workbook and charts under the output directory.")


if __name__ == "__main__":
    main()
