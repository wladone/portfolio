# Veridion POC - Entity Resolution & Data Quality

![Python](https://img.shields.io/badge/Python-3.12%20%7C%203.13-blue)
![License](https://img.shields.io/badge/License-MIT-green)
![Tests](https://img.shields.io/badge/Tests-pytest-informational)
![Lint](https://img.shields.io/badge/Lint-ruff%20%26%20black-informational)

Acest proiect livreaza un pipeline **Windows-friendly** (Python, venv) pentru:
- **Entity Resolution (ER)** pe candidati 1..5/linie (RapidFuzz + bonus tara)
- **QC** (duplicate & missingness)
- **Exporturi Power BI** (CSV-uri curate)
- **Raport PDF** (ReportLab)
- Hook-uri optionale: **Enrichment**, **ESG hints**, **Procurement Segments** (dezactivate implicit)

> Tehnologii: pandas, apidfuzz, Unidecode, pyyaml, eportlab, pytest, uff, lack.

---

## 1) Structura proiect

`
Popescu_Vlad_Veridion_Challenge_Nr_5/
?? config/config.yaml
?? data/raw/                # pune aici CSV-ul (ex: presales_data_sample.csv)
?? data/interim/
?? data/processed/
?? notebooks/POC_ER_QC.ipynb
?? outputs/pdf/
?? outputs/powerbi/
?? scripts/run_er.py
?? scripts/init_and_run.ps1
?? src/veridion_poc/        # logica: config, normalize, er, qc, report_pdf, hooks
?? tests/
?? .vscode/
?? requirements.txt
?? README.md
`

CSV asteptat (cap de tabel):
- Input: input_name, input_country
- Candidati (1..5): cand{n}_name, cand{n}_country, cand{n}_address, cand{n}_domain, cand{n}_id

Maparea este configurabila in config/config.yaml.

---

## 2) Setup rapid (Windows, PowerShell)

`powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
code .
`

In VS Code (Terminal > Run Task) ruleaza:
- Install deps
- Run ER
- Run QC + Report

sau direct din shell:

`powershell
python scripts\run_er.py --qc --report
# ori
.\scripts\init_and_run.ps1
`

---

## 3) Configurare (config/config.yaml)

`yaml
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
`

Praguri: status=accept daca scor >= ccept; maybe daca maybe <= scor < accept; altfel unmatched. Bonusul de tara se aplica atunci cand tara candidatului coincide cu tara inputului.

---

## 4) Rulare si output

`powershell
python scripts\run_er.py --qc --report
`

Fisiere rezultate:
- data/processed/entity_resolution.csv (ER, plus coloane noi daca features=true)
- data/processed/duplicates.csv (daca rulezi cu --qc)
- data/processed/missingness.csv (daca rulezi cu --qc)
- outputs/powerbi/fact_entity_resolution.csv
- outputs/pdf/POC_Veridion_ER_QC.pdf (daca rulezi cu --report)

---

## 5) Power BI - Quickstart

1. Get Data > Text/CSV > outputs/powerbi/fact_entity_resolution.csv
2. Vizualizari utile:
   - KPI/Card: Match-rate (procent status in {accept, maybe})
   - Bar chart: distributia status
   - Table: randuri status = "unmatched"
   - Bar chart: Top n est_cand_country
3. DAX de baza:

   `DAX
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
   `

---

## 6) Notebook


otebooks/POC_ER_QC.ipynb parcurge end-to-end: load config, incarca CSV, ruleaza ER, QC, exporta CSV si genereaza PDF.

---

## 7) Calitate si dezvoltare

`powershell
pytest -q
ruff check .
black .
`

Tasks VS Code: Run ER, Run QC + Report, Run Tests, Lint (ruff), Format (black).

---

## 8) Troubleshooting

- CSV missing: verifica input.path.
- Diacritice: normalizeaza cu Unidecode.
- Scoruri mici: ajusteaza pragurile sau legal_suffixes.
- Features optionale: seteaza eatures.*: true in config si ruleaza din nou.

---

## 9) Licenta

MIT License - vezi fisierul LICENSE.

Copyright 2025 Vlad Gabriel Popescu
