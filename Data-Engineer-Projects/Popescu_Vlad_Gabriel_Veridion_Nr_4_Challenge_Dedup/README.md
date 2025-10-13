# Veridion Product Deduplication

A lightweight pipeline for turning noisy product catalogs into a canonicalized, deduplicated view. The project combines heuristic blocking, similarity scoring, and clustering, and ships with scripts that make it easy to run end-to-end on CSV or Parquet input.

## Highlights
- **Flexible inputs** - accepts CSV or Parquet with loose column naming; common aliases are auto-mapped.
- **Deterministic clustering** - union-find clustering driven by token, brand, and identifier similarity.
- **Rich outputs** - canonical product table, row-to-cluster assignments, optional Excel workbook, charts, and JSON summary.
- **One-command execution** - `run_current.ps1` (Windows) or direct `python scripts/run_dedup.py` run the whole pipeline.
- **Batteries included** - virtual environment setup, pytest integration, and optional reporting dependencies.

## Prerequisites
- Python 3.10 or newer available on your PATH.
- PowerShell 7+ recommended on Windows (Windows PowerShell 5.1 works).
- Optional: `make` for Unix-style shortcuts.

## Quick Start (Windows)
```powershell
# From the repository root
./run_current.ps1 -InputPath "data/raw/veridion_product_deduplication_challenge.snappy.parquet" -OutDir "data/processed/latest_run"
```
The script will create `.venv` if needed, install dependencies, run the pipeline, and emit a short summary. Use `-Setup` to force environment recreation or `-Test` to run pytest beforehand.

## Cross-Platform Quick Start
```bash
python -m venv .venv
source .venv/bin/activate       # Windows: ./.venv/Scripts/Activate.ps1
pip install -r requirements.txt
pytest -q                       # optional but recommended
python scripts/run_dedup.py --input data/raw/your_file.csv --out-dir data/processed/latest_run
```
Add `--skip-reports` if you only need the CSV/Parquet outputs. Adjust `--threshold` (default `0.80`) to tune match sensitivity.

## Command Reference
| Option | Description |
| --- | --- |
| `-InputPath` / `--input` | CSV or Parquet file to deduplicate (required). |
| `-OutDir` / `--out-dir` | Directory for outputs. Created automatically; defaults to `data/processed`. |
| `-Threshold` / `--threshold` | Similarity score required to merge products. Typical range: 0.75-0.85. |
| `-Setup` | Recreate `.venv` and reinstall dependencies. |
| `-Test` | Run `pytest -q` before executing the pipeline. |
| `--skip-reports` | Skip Excel/PNG/JSON report generation. |

## Output Artifacts
All outputs land under `--out-dir` (default `data/processed`). Running with reports enabled produces:
- `dedup.csv` - canonicalized product view with `product_uid`, `cluster_size`, and aggregated attributes.
- `dedup.parquet` - Parquet equivalent (written when a Parquet engine is available).
- `cluster_assignments.csv` - mapping of original `row_index` to `product_uid` and cluster metadata.
- `dedup.xlsx` - Excel workbook with summary, deduplicated table, assignments, and top clusters/brands.
- `dedup.json` - JSON summary of headline metrics (generated alongside the workbook).
- `reports/` - PNG charts (`cluster_size_distribution.png`, `top_brands.png`) and supporting assets.

## Input Expectations
The pipeline works with sparse, messy catalogs. Column names are case-insensitive and common aliases are auto-renamed. Helpful fields (when present):
- Identifiers: `gtin`, `ean`, `upc`, `sku`, `product_identifier`.
- Text features: `name`, `title`, `description`.
- Brand/model: `brand`, `brand_name`, `model`, `model_number`, `mpn`.
- Pricing: `price`, `amount`, `price_value`, `sale_price`.
- URLs and media: `product_url`, `page_url`, `image_url`, `image`.
Missing fields simply reduce recall; the pipeline still runs on minimal schemas.

## How the Pipeline Works
1. **Blocking** - generate candidate pairs via GTIN/EAN/UPC matches, brand+model combos, and name-token shingles (`src/blocking.py`).
2. **Scoring** - compute a weighted score from token Jaccard, `difflib` ratios, identifier agreement, and brand/model signals (`src/similarity.py`).
3. **Clustering** - perform union-find over pairs that clear the similarity threshold (`src/cluster.py`).
4. **Aggregation** - choose canonical strings, aggregate price statistics, and mint stable `product_uid`s (`src/aggregate.py`).
5. **Reporting** - optional Excel workbook, JSON metrics, and PNG charts summarizing cluster distribution (`src/reporting.py`).

## Project Structure
```
project-root/
|-- data/                # raw, interim, and processed sample data
|-- scripts/run_dedup.py # CLI entry point used by the runner scripts
|-- src/                 # pipeline modules (blocking, similarity, cluster, aggregate, reporting)
|-- run_current.ps1      # Windows wrapper: setup + optional tests + pipeline + summary
|-- run.ps1              # Legacy runner script (retained for backwards compatibility)
|-- requirements.txt     # runtime dependencies
|-- tests/               # pytest suite covering key pipeline behaviors
```

## Development
- `pytest -q` runs the automated tests.
- `make test` / `make run` provide Unix shortcuts (see `Makefile`).
- Linting is not enforced; feel free to add `ruff`, `black`, or similar tools for local checks.

## Troubleshooting
- **Missing Parquet engine** - install `pyarrow` or `fastparquet` (included in `requirements.txt`). The pipeline falls back to CSV when Parquet writes fail.
- **Out-of-memory on huge datasets** - lower the threshold to reduce cluster sizes, or process the catalog in partitions.
- **No reports generated** - ensure optional dependencies (`matplotlib`, `openpyxl`) are available; they ship via `requirements.txt`.
- **Virtual environment issues** - rerun with `-Setup` to rebuild `.venv` from scratch.

---
Questions or ideas? Feel free to open an issue or start a discussion.
