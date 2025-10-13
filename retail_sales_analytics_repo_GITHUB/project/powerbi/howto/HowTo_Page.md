# How to Use (Power BI)
1) **Import CSV-urile** din `data/processed/` sau varianta `data/lightweight/`.
2) **Adaugă măsurile** din `dax/measures.dax` și setează tema `powerbi/theme.json`.
3) **Creează relațiile** (Date/Prod/Client/Canal/Regiune/Campanie → Fact) sau rulează `powerbi/auto/Build-Model-Relationships.ps1`.
4) **Aplică layout-urile** din `powerbi/layouts/` ca Page Background (Transparency=0%), apoi așază vizualurile conform ghidului.
5) **Forecast (opțional)**: importă `fact_monthly_sales.csv`, `monthly_forecast.csv`, `forecast_backtest.csv`, `forecast_metrics.csv` și creează pagina „Forecast”.
6) **RLS (opțional)**: importă `data/security/UserRegion.xlsx` și aplică expresia din `powerbi/RLS.txt`.
