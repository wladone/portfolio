[![CI](https://github.com/<USER>/<REPO>/actions/workflows/ci.yml/badge.svg)](https://github.com/<USER>/<REPO>/actions/workflows/ci.yml) [![Release](https://img.shields.io/github/v/release/<USER>/<REPO>?display_name=tag)](https://github.com/<USER>/<REPO>/releases)


# Retail Sales Analytics (Power BI + Python) — GitHub Ready

Toolkit complet pentru analiză de vânzări **Retail/E-commerce**: dataset demo, ETL, măsuri DAX, layout-uri Power BI,
forecast (daily & monthly), elasticitate preț, RLS, bookmarks, template PBIT — totul gata de publicat pe GitHub.

## Conținut (versiuni 1 → 9)
- **v1–v3**: dataset + SQL/DAX + README + notebook; **UserRegion** (RLS)
- **v4–v6**: forecast (daily + monthly), guide Power BI, pagini predefinite (layout PNG + YAML)
- **v7**: dataset **lightweight** + automatizare relații (Tabular Editor)
- **v8**: pagină **How to Use** + **PBIT template kit**
- **v9**: **Pareto 80/20** DAX + **Bookmarks** guide

## Structura repo
```
project/
  data/
    processed/        # CSV: fact & dims, forecast files, pricing metrics
    lightweight/      # subset (~90 zile) pt. demo rapid
    security/         # UserRegion.csv / .xlsx (RLS)
  dax/                # measures.dax + pareto_measures.dax
  powerbi/
    layouts/          # 00–05 png + page_layouts.yaml
    howto/            # HowTo_Page.md
    template/         # parametru FolderPath + fn_GetCsv + queries.pq
    auto/             # Build-Model-Relationships.ps1 + CreateRelationships.csx
    RLS.txt           # expresia de securitate (exemplu)
    README_PBIX.md    # ghid complet Power BI (pagini, forecast, layout-uri)
  notebooks/          # Retail_Analytics.ipynb + .py
  scripts/            # elasticity_pricing.py, monthly_forecast.py
  sql/                # warehouse.sql
  docs/figures/       # grafice PNG
```

## Quickstart
```bash
# 1) Clonare
git clone <repo-url> && cd retail_sales_analytics_repo

# 2) Mediu
python -m venv .venv && . .venv/bin/activate    # (Windows: .\.venv\Scripts\activate)
pip install -r requirements.txt

# 3) Rulează analizele Python
make elasticity
make forecast
# outputs în: project/data/processed/
```

## Power BI (5 pagini + Forecast)
1. Importă CSV-urile din `project/data/processed/` (sau `project/data/lightweight/`).
2. În *Model*, leagă cheile (sau rulează scriptul din `project/powerbi/auto/`).  
3. Copiază măsurile din `project/dax/measures.dax` (+ `pareto_measures.dax`).  
4. Aplică tema `project/powerbi/theme.json` și **layout-urile** (Page Background).  
5. Urmează `project/powerbi/README_PBIX.md` (include pagina **Forecast** + RLS).

## Licență
MIT — vezi `LICENSE`.

— Build 2025-08-30


## Releases
Creează un **release** prin versionare cu tag (ex.: `v1.0.0`). Workflow-ul *release.yml* va construi și atașa automat un ZIP.

```bash
git tag -a v1.0.0 -m "First release"
git push origin v1.0.0
```
> În `README.md`, înlocuiește **<USER>/<REPO>** cu numele tău GitHub pentru ca badge-urile să funcționeze.
