"""Generate human-friendly summaries and charts for deduplication outputs."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Tuple

import matplotlib
matplotlib.use("Agg")  # Ensure plotting works in headless environments
import matplotlib.pyplot as plt
import pandas as pd


def _summary_stats(dedup_df: pd.DataFrame, map_df: pd.DataFrame) -> Tuple[dict, pd.DataFrame]:
    """Compute high-level metrics about the deduplicated dataset."""
    total_rows = int(map_df["row_index"].nunique()) if not map_df.empty else 0
    total_clusters = int(len(dedup_df))
    multi_clusters = int((dedup_df.get("cluster_size", 1) > 1).sum())
    mean_cluster = float(dedup_df.get("cluster_size", pd.Series([1])).mean()) if not dedup_df.empty else 0.0

    summary = {
        "total_input_rows": total_rows,
        "clusters_total": total_clusters,
        "clusters_multi_member": multi_clusters,
        "avg_cluster_size": round(mean_cluster, 3),
    }
    summary_df = pd.DataFrame(
        {"metric": list(summary.keys()), "value": list(summary.values())}
    )
    return summary, summary_df


def _ensure_reports_dir(out_dir: str | Path) -> Path:
    reports = Path(out_dir) / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    return reports


def write_excel_workbook(dedup_df: pd.DataFrame, map_df: pd.DataFrame, out_dir: str | Path) -> Path:
    """Create an Excel workbook with the main tables and key summary sheets."""
    dest = Path(out_dir) / "dedup.xlsx"
    summary, summary_df = _summary_stats(dedup_df, map_df)

    top_clusters = dedup_df.nlargest(25, "cluster_size") if "cluster_size" in dedup_df else pd.DataFrame()
    brand_counts = (
        dedup_df["brand"].dropna().astype(str).str.strip().replace({"": None}).dropna()
        if "brand" in dedup_df else pd.Series(dtype="object")
    )
    top_brands = (
        brand_counts.value_counts().head(25).rename_axis("brand").reset_index(name="products")
        if not brand_counts.empty else pd.DataFrame(columns=["brand", "products"])
    )

    with pd.ExcelWriter(dest, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="summary", index=False)
        dedup_df.to_excel(writer, sheet_name="deduplicated", index=False)
        map_df.to_excel(writer, sheet_name="cluster_assignments", index=False)
        if not top_clusters.empty:
            top_clusters.to_excel(writer, sheet_name="top_clusters", index=False)
        if not top_brands.empty:
            top_brands.to_excel(writer, sheet_name="top_brands", index=False)

    # Persist summary JSON alongside the workbook for quick reuse
    summary_json = dest.with_suffix(".json")
    summary_json.write_text(json.dumps(summary, indent=2))
    return dest


def _plot_cluster_distribution(dedup_df: pd.DataFrame, reports_dir: Path) -> None:
    if "cluster_size" not in dedup_df:
        return
    counts = dedup_df["cluster_size"].value_counts().sort_index()
    if counts.empty:
        return
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.bar(counts.index.astype(int), counts.values, color="#2C7FB8")
    ax.set_xlabel("Cluster size")
    ax.set_ylabel("Number of clusters")
    ax.set_title("Distribution of cluster sizes")
    ax.set_xticks(counts.index.astype(int))
    ax.set_ylim(0, counts.values.max() * 1.1)
    fig.tight_layout()
    fig.savefig(reports_dir / "cluster_size_distribution.png", dpi=150)
    plt.close(fig)


def _plot_top_brands(dedup_df: pd.DataFrame, reports_dir: Path) -> None:
    if "brand" not in dedup_df:
        return
    series = dedup_df["brand"].dropna().astype(str).str.strip()
    series = series[series != ""]
    if series.empty:
        return
    top = series.value_counts().head(15)
    fig, ax = plt.subplots(figsize=(8, 4.5))
    top.sort_values().plot(kind="barh", ax=ax, color="#41B6C4")
    ax.set_xlabel("Number of canonical products")
    ax.set_ylabel("Brand")
    ax.set_title("Top brands by deduplicated products")
    fig.tight_layout()
    fig.savefig(reports_dir / "top_brands.png", dpi=150)
    plt.close(fig)


def generate_visual_reports(dedup_df: pd.DataFrame, map_df: pd.DataFrame, out_dir: str | Path) -> None:
    """Produce Excel, JSON, and PNG reports that summarize dedup results."""
    reports_dir = _ensure_reports_dir(out_dir)
    write_excel_workbook(dedup_df, map_df, out_dir)
    _plot_cluster_distribution(dedup_df, reports_dir)
    _plot_top_brands(dedup_df, reports_dir)

