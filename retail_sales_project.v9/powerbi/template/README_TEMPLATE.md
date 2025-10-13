# PBIT Template – Kit
Acest kit te ajută să creezi rapid un **Power BI Template (.pbit)** cu conexiuni la folderul de date.

## Pași (2–3 minute)
1. Deschide **Power BI Desktop** → *Blank report*.
2. **Transform Data** → **Manage Parameters** → **New** → `FolderPath` (Text) și setează calea locală către `data\processed`.
3. În **Power Query**, creează o nouă **Blank Query** cu numele `GetCsv` și lipește conținutul din `fn_GetCsv.pq`.
4. Creează câte o **Blank Query** pentru fiecare tabel (`dim_date`, `dim_product`, etc.) și lipește codul corespunzător din fișierele `.pq`.
5. **Close & Apply** → în *Model view*, creează relațiile (sau rulează scriptul din `powerbi/auto/`).
6. **Importă** `dax/measures.dax` (creează măsurile manual), aplică `powerbi/theme.json` și layout-urile.
7. **File → Export → Power BI template (.pbit)**. La deschiderea template-ului, vei putea schimba `FolderPath`.

> Sfat: Poți folosi varianta `data/lightweight` ca set implicit pentru template (îți recomandăm pentru demo).
