"""CLI entrypoint: run ER (+ optional QC) and export Power BI-ready CSVs.
Generates an English PDF report with --report.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

from src.veridion_poc.config import load_config
from src.veridion_poc.er import entity_resolution
from src.veridion_poc.qc import detect_duplicates, missingness
from src.veridion_poc.report_pdf import generate_pdf


def _ensure_dirs():
    Path("data/processed").mkdir(parents=True, exist_ok=True)
    Path("outputs/powerbi").mkdir(parents=True, exist_ok=True)
    Path("outputs/pdf").mkdir(parents=True, exist_ok=True)


def _print_summary(er_df: pd.DataFrame):
    total = len(er_df)
    by_status = er_df["status"].value_counts(
        dropna=False).to_dict() if "status" in er_df.columns else {}
    print(f"[ER] rows={total} status_dist={by_status}")


def _build_sections(cfg, er_df, dup_df=None, miss_df=None) -> list[tuple[str, str]]:
    total = len(er_df)
    accept = int((er_df["status"] == "accept").sum())
    maybe = int((er_df["status"] == "maybe").sum())
    unmatched = int((er_df["status"] == "unmatched").sum())
    match_rate = round(((accept + maybe) / total * 100.0) if total else 0.0, 2)
    accept_rate = round((accept / total * 100.0) if total else 0.0, 2)

    context = (
        "Veridion POC — Entity Resolution & Data Quality for Procurement. "
        "Goal: resolve suppliers against up to five candidates per row, validate data quality, "
        "and deliver clean, BI-ready datasets."
    )

    methodology = (
        "- Name normalization: remove diacritics, lowercase, strip punctuation, remove legal suffixes (configurable).\n"
        "- Scoring: RapidFuzz token_set_ratio (0..1) + country bonus when candidate country equals input country.\n"
        f"- Thresholds: accept ≥ {cfg.thresholds.accept:.2f}, maybe ≥ {cfg.thresholds.maybe:.2f}, country bonus {cfg.thresholds.country_bonus:.2f}.\n"
        "- Duplicates: group by normalized name (+ country).\n"
        "- Missingness: % missing on key columns (name, country)."
    )

    results = (
        f"- Total rows: {total}\n"
        f"- Accept: {accept}  |  Maybe: {maybe}  |  Unmatched: {unmatched}\n"
        f"- Match rate (Accept+Maybe): {match_rate}%  |  Accept rate: {accept_rate}%"
    )

    if dup_df is not None and miss_df is not None:
        dup_groups = int(len(dup_df))
        top_missing = miss_df.sort_values(
            "missing_pct", ascending=False).head(3)
        top_missing_str = ", ".join(
            f"{r['column']}={r['missing_pct']}%" for _, r in top_missing.iterrows()
        ) if not top_missing.empty else "—"
        qc = (
            f"- Duplicate groups: {dup_groups}\n"
            f"- Top 3 columns by missingness: {top_missing_str}"
        )
    else:
        qc = "QC not enabled for this run (use --qc to include duplicates & missingness)."

    next_steps = (
        "- Manually validate 'unmatched' rows (external sources where available).\n"
        "- Consolidate confirmed duplicate groups (master vendor).\n"
        "- Fine-tune thresholds (accept/maybe) on golden sets.\n"
        "- (Optional) Enable Enrichment/ESG/Segments in YAML if needed.\n"
        "- Import CSVs in Power BI and define KPIs (match rate, unmatched, duplicate rate)."
    )

    return [
        ("Context", context),
        ("Methodology", methodology),
        ("Entity Resolution Results", results),
        ("Data Quality (optional)", qc),
        ("Next Steps", next_steps),
    ]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--qc", action="store_true")
    ap.add_argument("--report", action="store_true")
    args = ap.parse_args()

    cfg = load_config(args.config)
    _ensure_dirs()

    # Load input CSV
    df = pd.read_csv(cfg.input.path)

    # --- ER ---
    er_df = entity_resolution(df, cfg)

    # --- QC (optional) ---
    dup_df = miss_df = None
    if args.qc:
        dup_df = detect_duplicates(df, cfg)
        miss_df = missingness(
            df, [cfg.input.name_column, cfg.input.country_column or "input_country"])
        Path("data/processed/duplicates.csv").write_text(
            dup_df.to_csv(index=False), encoding="utf-8")
        Path("data/processed/missingness.csv").write_text(
            miss_df.to_csv(index=False), encoding="utf-8")

    # Write ER output
    er_out = Path("data/processed/entity_resolution.csv")
    er_df.to_csv(er_out, index=False)

    # --- Power BI export (always) ---
    pbi_out = Path("outputs/powerbi/fact_entity_resolution.csv")
    er_df.to_csv(pbi_out, index=False)

    # Summary
    _print_summary(er_df)
    print("[WRITE]", er_out.resolve())
    if dup_df is not None:
        print("[WRITE]", Path("data/processed/duplicates.csv").resolve())
    if miss_df is not None:
        print("[WRITE]", Path("data/processed/missingness.csv").resolve())
    print("[WRITE]", pbi_out.resolve())

    # --- PDF report (optional, English) ---
    if args.report:
        pdf_path = Path("outputs/pdf/POC_Veridion_ER_QC.pdf")
        sections = _build_sections(cfg, er_df, dup_df, miss_df)
        generate_pdf(
            str(pdf_path), "POC Report – Veridion (ER + QC)", sections)
        print("[WRITE]", pdf_path.resolve())


if __name__ == "__main__":
    main()
