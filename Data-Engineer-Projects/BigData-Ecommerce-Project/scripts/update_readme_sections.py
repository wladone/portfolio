#!/usr/bin/env python3
"""
Idempotent README updater:
- Inserts combined "One-command run" + "Costs & Cleanup automation" after the
  "Publish sample events" code block (or appends to EOF).
- Replaces "Romania region & GDPR notes" with "Romania/EU region notes".
- Removes any legacy "## Costs & Cleanup" sections.
Safe to run multiple times.
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
import tempfile

ONE_COMMAND_AND_CLEANUP = """\
## One-command run
- **Local (DirectRunner):** `python run.py local`
- **Dataflow Flex Template:** `python run.py flex`
- **Composer (direct DAG):** `python run.py direct`
- **Publish samples:** `make publish-samples`

**Setup prerequisites**: Fill `.env` (RO/EU defaults use `REGION=europe-central2`), build the Flex template per README, and upload `beam/streaming_pipeline.py` to the Composer bucket under `data/beam/` for the direct DAG.

## Costs & Cleanup automation
- **Plan (safe):** `make cleanup-plan` — lists running Dataflow jobs, GCS tmp/staging/templates, BQ dataset tables, Pub/Sub topics, and Bigtable presence.
- **Apply (destructive, guarded):** `make cleanup-apply`  
  - Requires `DRY_RUN=0` **and** `CONFIRM=DELETE`.  
  - Optional flags: `DELETE_TOPICS=1`, `DELETE_BQ_DATASET=1`, `DELETE_BIGTABLE_TABLE=1`, `KEEP_TEMPLATES=0`.
- **Nightly guardrails (safe):** `make guardrails` — drains running Dataflow jobs with name prefix `streaming*`, clears GCS tmp/staging, sets BQ TTL (30d), sets Pub/Sub retention (24h).
"""

ROMANIA_EU_NOTES = """\
## Romania/EU region notes
- Keep **all resources in one region** (Composer, Dataflow, GCS, BigQuery, Pub/Sub, Bigtable).
  Default: `europe-central2` (Warsaw). Fallback: `europe-west3` (Frankfurt).
- Use a **regional** BigQuery dataset (same region) to avoid cross-region writes and reduce latency.
- Optional security: **Private IP** and **CMEK** with a KMS key in the same region.
- GDPR hygiene: store UTC timestamps, avoid PII in logs, set Pub/Sub retention and Bigtable GC TTLs consciously, and document data flows for audits.
"""

H2 = r"^##\s+"
_FLAGS = re.I | re.M | re.S


def _normalize_newlines(text: str) -> tuple[str, str]:
    if "\r\n" in text:
        return text.replace("\r\n", "\n"), "\r\n"
    if "\r" in text:
        return text.replace("\r", "\n"), "\r"
    return text, "\n"


def _restore_newlines(text: str, newline: str) -> str:
    return text.replace("\n", newline)


def _strip_legacy_costs(text: str) -> str:
    pat = re.compile(
        rf"^##\s+Costs\s*&\s*Cleanup(?![^\n]*automation).*?(?=^{H2}|\Z)",
        _FLAGS,
    )
    return re.sub(pat, "", text)


def _replace_region_notes(text: str) -> str:
    pat_old = re.compile(
        rf"^##\s+Romania\s+region\s*&\s*GDPR\s+notes.*?(?=^{H2}|\Z)",
        _FLAGS,
    )
    text = re.sub(pat_old, ROMANIA_EU_NOTES.strip() + "\n\n", text)

    pat_new = re.compile(
        rf"^##\s+Romania/EU\s+region\s+notes.*?(?=^{H2}|\Z)",
        _FLAGS,
    )
    matches = list(pat_new.finditer(text))
    if matches:
        first = matches[0]
        last = matches[-1]
        text = (
            text[: first.start()]
            + ROMANIA_EU_NOTES.strip()
            + "\n\n"
            + text[last.end() :]
        )
    else:
        text = text.rstrip() + "\n\n" + ROMANIA_EU_NOTES.strip() + "\n"
    return text


def _ensure_combined_block(text: str) -> str:
    if ONE_COMMAND_AND_CLEANUP.strip() in text:
        return text

    anchor_pattern = re.compile(
        r"((?:\*\*|##\s+)Publish\s+sample\s+events[\s\S]*?\n```[\s\S]*?\n```)",
        re.I,
    )
    match = anchor_pattern.search(text)
    if match:
        insert_at = match.end()
        return (
            text[:insert_at]
            + "\n\n"
            + ONE_COMMAND_AND_CLEANUP.strip()
            + "\n\n"
            + text[insert_at:]
        )
    return text.rstrip() + "\n\n" + ONE_COMMAND_AND_CLEANUP.strip() + "\n"


def patch_readme_text(text: str) -> str:
    original = text
    normalized, newline = _normalize_newlines(text)
    updated = _strip_legacy_costs(normalized)
    updated = _replace_region_notes(updated)
    updated = _ensure_combined_block(updated)
    updated = re.sub(r"\n{3,}", "\n\n", updated)
    updated = updated.rstrip("\n") + "\n"
    if updated == normalized:
        return original
    return _restore_newlines(updated, newline)


def atomic_write(path: str, data: str) -> None:
    directory = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(prefix=".readme.tmp.", dir=directory, text=True)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(data)
    bak = path + ".bak"
    if os.path.exists(path) and not os.path.exists(bak):
        shutil.copy2(path, bak)
    os.replace(tmp, path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", default="README.md", help="README path")
    parser.add_argument(
        "--dry-run", action="store_true", help="Print status, don't write"
    )
    parser.add_argument(
        "--stdout", action="store_true", help="Emit patched content to stdout"
    )
    parser.add_argument(
        "--check", action="store_true", help="Exit 3 if changes would be made"
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    try:
        with open(args.path, "r", encoding="utf-8") as handle:
            content = handle.read()
    except FileNotFoundError:
        print(f"ERROR: {args.path} not found", file=sys.stderr)
        return 2

    patched = patch_readme_text(content)
    changed = patched != content

    if args.check and changed:
        if args.verbose:
            print("README would be updated.")
        return 3

    if args.stdout:
        sys.stdout.write(patched)
        return 0

    if args.dry_run:
        print("README needs update." if changed else "No changes needed.")
        return 0

    if changed:
        atomic_write(args.path, patched)
        if args.verbose:
            print(f"Updated {args.path} (backup at {args.path}.bak).")
    else:
        if args.verbose:
            print("No changes needed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
