# Veridion POC ? Entity Resolution & Data Quality

![Python](https://img.shields.io/badge/Python-3.12%20%7C%203.13-blue)
![License](https://img.shields.io/badge/License-MIT-green)
![Tests](https://img.shields.io/badge/Tests-pytest-informational)
![Lint](https://img.shields.io/badge/Lint-ruff%20%26%20black-informational)

This repository delivers a **Windows-friendly** Python pipeline (venv-based) that covers:
- **Entity Resolution (ER)** across up to 5 candidates per record (RapidFuzz + country bonus)
- **Data Quality checks** (duplicate clusters & missingness statistics)
- **Power BI-ready exports** (clean CSV outputs)
- **PDF reporting** via ReportLab
- Optional hooks: **Enrichment**, **ESG hints**, **Procurement Segments** (disabled by default)

> Tech stack highlights: `pandas`, `rapidfuzz`, `Unidecode`, `pyyaml`, `reportlab`, `pytest`, `ruff`, `black`.

Estimated engineering effort so far: **~7?8 hours** (including scaffolding, pipeline logic, dashboards, and documentation).

---

## 1) Project Layout

```
Popescu_Vlad_Veridion_Challenge_Nr_5/
?? config/config.yaml
?? data/raw/                # place the input CSV here (e.g. presales_data_sample.csv)
?? data/interim/
?? data/processed/
?? notebooks/POC_ER_QC.ipynb
?? notebooks/Client_Analyst_View.ipynb
?? outputs/pdf/
?? outputs/powerbi/
?? scripts/run_er.py
?? scripts/init_and_run.ps1
?? src/veridion_poc/        # config loader, normalization, ER, QC, report generation, feature hooks
?? tests/
?? .vscode/
?? requirements.txt
?? README.md
```

Expected CSV headers (configurable):
- Input: `input_name`, `input_country`
- Candidates (1..5): `cand{n}_name`, `cand{n}_country`, `cand{n}_address`, `cand{n}_domain`, `cand{n}_id`

---

## 2) Quick Setup (Windows + PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
code .
```

From VS Code (**Terminal ? Run Task**) execute:
- **Install deps**
- **Run ER**
- **Run QC + Report**

or run directly:

```powershell
python scripts\run_er.py --qc --report
# or
.\scripts\init_and_run.ps1
```

---

## 3) Configuration (`config/config.yaml`)

```yaml
input:
  path: data/raw/presales_data_sample.csv
  name_column: input_name
  country_column: input_country

candidates:
  max_candidates: 5
  name_pattern: "cand{n}_name"
  country_pattern: "cand{n}_country"
  address_pattern: "cand{n}_address"
  domain_pattern: "cand{n}_domain"
  id_pattern: "cand{n}_id"

thresholds:
  accept: 0.70
  maybe: 0.50
  country_bonus: 0.05

normalization:
  lower: true
  strip_punct: true
  remove_legal_suffixes: true
  legal_suffixes: [srl, sa, ltd, "a/s", aps, llc, inc, gmbh, ... ]

features:
  enrichment: false
  esg: false
  procurement_segments: false
```

Scoring logic: `status = accept` if score ? `accept`; `maybe` if `maybe ? score < accept`; otherwise `unmatched`. The country bonus is applied when `cand_country` matches `input_country` (case-insensitive).

---

## 4) Running the Pipeline & Outputs

```powershell
python scripts\run_er.py --qc --report
```

Artifacts written by the pipeline:
- `data/processed/entity_resolution.csv` ? ER results (extra columns appear if features are enabled)
- `data/processed/duplicates.csv` ? duplicate groups (when `--qc` is used)
- `data/processed/missingness.csv` ? column-level missingness stats (when `--qc` is used)
- `outputs/powerbi/fact_entity_resolution.csv` ? Power BI feed
- `outputs/pdf/POC_Veridion_ER_QC.pdf` ? compact PDF report (when `--report` is used)

---

## 5) Power BI Quickstart

1. **Get Data ? Text/CSV** ? `outputs/powerbi/fact_entity_resolution.csv`
2. Suggested visuals:
   - KPI/Card: Match-rate (% rows with `status` in {`accept`, `maybe`})
   - Clustered bar: status distribution
   - Table: `status = "unmatched"` (include `input_name`, `input_country`, scores)
   - Heatmap/bar: top `best_cand_country`
3. Sample DAX metrics:

```DAX
Match Rate :=
  DIVIDE(
    COUNTROWS(FILTER('fact_entity_resolution', 'fact_entity_resolution'[status] IN {"accept","maybe"})),
    COUNTROWS('fact_entity_resolution')
  )

Accept Rate :=
  DIVIDE(
    COUNTROWS(FILTER('fact_entity_resolution', 'fact_entity_resolution'[status] = "accept")),
    COUNTROWS('fact_entity_resolution')
  )
```

---

## 6) Notebooks

- `POC_ER_QC.ipynb` ? end-to-end walkthrough: load config, run ER/QC, export CSVs, generate PDF.
- `Client_Analyst_View.ipynb` ? analyst dashboard with score distributions, country analysis, duplicate/missingness insights, optional segment/ESG visualizations, funnel KPIs.

Both notebooks reference the processed outputs from `data/processed/` and `outputs/` to stay aligned with the pipeline.

---

## 7) Quality & Development

```powershell
pytest -q
ruff check .
black .
```

VS Code tasks pre-configured: Run ER, Run QC + Report, Run Tests, Lint (ruff), Format (black).

---

## 8) Troubleshooting

- **CSV missing**: ensure `config.input.path` points to an existing file.
- **Diacritics/Unicode**: strings are normalized with `Unidecode` before scoring.
- **Low scores**: tweak `thresholds.accept/maybe` or extend `legal_suffixes`.
- **Optional features**: set `features.*: true` in `config.yaml`, rerun the pipeline, and the new columns will appear automatically.
- **Module import errors inside notebooks**: run `python scripts\run_er.py` at least once (or add project root to `sys.path`) so that processed files exist.

---

## 9) License

MIT License ? see [`LICENSE`](LICENSE).

? 2025 Vlad Gabriel Popescu
