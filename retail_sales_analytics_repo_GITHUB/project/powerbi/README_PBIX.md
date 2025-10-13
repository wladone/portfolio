
# Power BI – Ghid rapid (5 pagini)

1) **Home → Get Data → Text/CSV** și importă următoarele fișiere din `/mnt/data/retail_sales_project/data/processed`:
   - dim_date.csv, dim_product.csv, dim_customer.csv, dim_channel.csv, dim_region.csv, dim_campaign.csv, fact_sales.csv

2) **Model**
   - Leagă `fact_sales[OrderDate]` la `dim_date[Date]` (Many-to-one, Single, direction from date to fact).
   - Leagă `ProductID`, `CustomerID`, `ChannelID`, `RegionID`, `CampaignID` la tabelele dim corespunzătoare (single-direction).
   - Setează `dim_date[Date]` ca **Date table**.
   - Marchează `dim_product[ProductID]`, `dim_customer[CustomerID]` etc. drept chei.

3) **Măsuri DAX** – copiază conținutul din `../dax/measures.dax` în un tabel de măsuri (Modeling → New measure).

4) **Theme** – View → Themes → Browse for themes → alege `powerbi/theme.json`.

5) **Pagini**
   - **1_Executive**: Card KPI (Sales, GM%, Units, AOV, YoY%), linie Sales (by Date) cu YoY (secondary), bar Pareto (SKU), slicere Date/Channel/Region/Campaign.
   - **2_Product**: Matrix Category→Subcategory→SKU (Sales, GM%, Return%), scatter GM% vs Sales, waterfall Net→Discount→COGS→GM.
   - **3_Channels**: Stacked bar Sales by Channel + GM% (line), map/treemap pe Region.
   - **4_Pricing**: Scatter Price vs Units (by Category), combo Sales vs Discount%, tabel campanii cu Promo Uplift% și ROI.
   - **5_Customers**: RFM segmente (heatmap), cohort retenție (line), card CLV simplificat.

6) **RLS** (opțional): adaugă un tabel UserRegion (import xlsx/csv) și expresia din `powerbi/RLS.txt`.

7) **Salvează** ca `.pbix`.


## 6) Pagina „Forecast” (Daily & Monthly)
**Daily**: dacă vrei forecast zilnic, folosește graficul din notebook și/sau creează un vizual de tip line cu [Sales] și o serie „Forecast” dintr-un tabel extern (opțional).

**Monthly (recomandat pentru exec):**
1. Importă din `data/processed/`:
   - `fact_monthly_sales.csv` (coloane: MonthDate, ActualNetSales)
   - `monthly_forecast.csv` (MonthDate, yhat, yhat_lower, yhat_upper)
   - `forecast_backtest.csv` (MonthDate, Actual, Forecast) – pentru evaluarea modelului
   - `forecast_metrics.csv` – pentru KPI (MAE, Bias)

2. Creează relații (dacă folosești `dim_date`):
   - Leagă `dim_date[Date]` ↔ `fact_monthly_sales[MonthDate]` (Many-to-one, Single)
   - Opțional: leagă și `monthly_forecast[MonthDate]` și `forecast_backtest[MonthDate]` la `dim_date[Date]`

3. Vizuale recomandate:
   - **Line chart**: `MonthDate` pe axă; serii:
     - Actual = `fact_monthly_sales[ActualNetSales]`
     - Forecast = `monthly_forecast[yhat]`
     - (opțional) Bandă de încredere: `yhat_lower`, `yhat_upper` ca „error bars”
   - **Cards**: importă `forecast_metrics.csv` și afișează `MAE` și `Bias` (interpretare: Bias>0 = sub-forecasting, Bias<0 = over-forecasting)
   - **Table**: `forecast_backtest` pentru transparență (Actual, Forecast, Error)

4. Variantă DAX (DIY):
   - Creează o masă „ForecastCompare” (Power Query) prin merge între `fact_monthly_sales` și `forecast_backtest` pe `MonthDate`, redenumește coloanele în `Actual` și `Forecast`.
   - Adaugă din `dax/measures.dax` măsurile:
     - `Forecast Actual`, `Forecast Pred`, `Forecast Error`, `Forecast MAE`, `Forecast Bias`
   - Folosește **cards** pentru MAE/Bias și un **line** cu Actual vs Pred.

5. Interpretare rapidă:
   - **MAE** = eroarea medie absolută (în unități monetare).
   - **Bias** = media (Actual − Forecast). Pozitiv => modelul **subestimează**; negativ => **supraestimează**.


## 7) Paginile predefinite (layout-uri)
În folderul `powerbi/layouts/` ai imaginile **01–05 _Layout.png** și fișierul `page_layouts.yaml` cu coordonate sugerate.
**Cum le folosești (rapid):**
1. În fiecare pagină, setează **Canvas settings → Page background** la imaginea layout-ului corespunzător; **Transparency = 0%**.
2. Adaugă vizualurile în dreptunghiurile marcate, folosind măsurile/coloanele indicate în `page_layouts.yaml`.
3. După aranjare, poți seta transparența background-ului la 100% sau îl poți păstra ca ghid.

> Sfat: activează **Snap to grid** în Power BI pentru aliniere ușoară.


## 8) Demo „Lightweight” + automatizare relații (Tabular Editor)
- **Dataset mic** în `data/lightweight/`: folosește `*_small.csv` pentru o versiune rapidă (ultimele ~90 zile).
- După importul CSV-urilor, poți crea automat relațiile:

**Pas cu Pas (cu Power BI Desktop deschis pe raport):**
1. Instalează **Tabular Editor** (v2 sau v3).
2. Deschide raportul în Power BI Desktop (modelul trebuie să fie încărcat).
3. Rulează în PowerShell:  
   `.\powerbiuto\Build-Model-Relationships.ps1 -ScriptPath .\powerbiuto\CreateRelationships.csx`  
   (actualizează calea către Tabular Editor dacă este diferită)
4. Scriptul aplică relațiile cheie: Date→Fact, Product→Fact, Customer→Fact, Channel→Fact, Region→Fact, Campaign→Fact.


## 9) Pagina „How to Use”
- Fundal: `powerbi/layouts/00_HowTo_Layout.png`
- Text & pași: `powerbi/howto/HowTo_Page.md`

## 10) Kit pentru **PBIT Template**
- `powerbi/template/parameters.pq` – parametru `FolderPath`
- `powerbi/template/fn_GetCsv.pq` – funcție generică de încărcare CSV
- `powerbi/template/*.pq` – interogări pentru fiecare tabel
- `powerbi/template/README_TEMPLATE.md` – pașii pentru a salva ca **.pbit**


## 11) Pareto 80/20 + Bookmarks
- DAX: `dax/pareto_measures.dax` (rank, % și cumul, flag 80%).
- Visual: **Combo** (coloană = [Sales], linie = [SKU Cum Sales %]) sortată desc după [Sales].
- Slicer/Filtru: `Pareto 80% Flag` (sau `Pareto 80% Mask = 1`) pentru a izola contributorii principali.
- Bookmarks: vezi `powerbi/bookmarks/README_BOOKMARKS.md` pentru `Promo ON/OFF`, `Online`, `București`, `Pareto Top 80`, `Reset`.
