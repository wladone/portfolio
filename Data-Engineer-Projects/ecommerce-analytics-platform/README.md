# E-commerce Analytics Platform

![CI](https://github.com/${ORG}/${REPO}/actions/workflows/ci.yml/badge.svg)
![Docker Release](https://github.com/${ORG}/${REPO}/actions/workflows/release-docker.yml/badge.svg)

## Rulare cu Docker Compose (dev)

Pentru a rula aplicația în mod dezvoltare folosind Docker Compose:

1. Copiază fișierul de configurare mediu:
   ```bash
   cp .env.example .env
   ```

2. Pornește serviciile:
   ```bash
   make compose-up
   ```

3. Aplică migrațiile bazei de date:
   ```bash
   alembic upgrade head
   ```

4. Generează date sintetice:
   ```bash
   make seed
   ```

5. Antrenează modelul:

## Documentație

Documentația completă este disponibilă ca un site MkDocs Material:

```bash
# Instalare dependențe docs
poetry install --extras docs

# Pornire server local
make docs-serve
# Deschide http://localhost:8001

# Build static
make docs-build
# Output în .dist/site/
```

## Securitate & Secrete

### Pre-commit Hooks

Proiectul folosește multiple hooks de pre-commit pentru scanare de securitate:

```bash
# Instalare
poetry install && pre-commit install

# Generare baseline pentru detect-secrets
detect-secrets scan > .secrets.baseline

# Rulare completă
pre-commit run --all-files
```

### Scanare de Securitate în CI

Job-ul `security_scan` rulează:
- detect-secrets pentru identificare secrete hardcodate
- Gitleaks pentru scanare de secrete și credențiale
- Bandit pentru scanare statică de vulnerabilități Python
- Generare SBOM în format CycloneDX XML

Artefactele sunt disponibile în GitHub Actions:
- `sbom-python.xml` - lista completă de dependențe
- `bandit-report.json` - rezultate scanare statică
- `gitleaks-report.json` - secrete detectate (dacă există)

### SBOM & Trivy

Generare locală SBOM:
```bash
make sbom
```

Scanare imagini Docker:
```bash
make scan-docker
```

## Rotire JWT (HS256)

Managementul cheilor JWT folosește un keyset cu `kid` și rotire:

```bash
# Lista cheilor curente
python scripts/rotate_jwt_keys.py list

# Adaugă cheie nouă și dezactivează vechea cheie
python scripts/rotate_jwt_keys.py rotate --kid 2025-10-26

# Reîncarcă cheile în API
curl -X POST -H "Authorization: Bearer <admin_token>" \
  http://localhost:8000/auth/_reload-keys

# Verifică JWKS public
curl http://localhost:8000/.well-known/jwks.json | jq
```

Cheia nouă devine activă pentru token-uri noi, dar token-urile vechi rămân valide până la revocare explicită.

## Politici de Logare

Loggingul folosește redactare automată pentru câmpuri sensibile:
- Parolele sunt mascate complet
- Token-urile și cheile sunt trunchiate (primele/ultimele 4 caractere)
- Contextul structurat exclude PII

Exemplu:
```json
{
  "event": "User login",
  "user_id": "12345",
  "password": "***",
  "api_key": "abcd...wxyz"
}
```

## Streaming Kafka/Redpanda

Aplicația suportă procesarea evenimentelor în streaming folosind Kafka (Redpanda). Pentru a rula:

1. Pornește broker-ul Redpanda:
   ```bash
   make compose-up
   ```

2. Creează topic-ul pentru comenzi:
   ```bash
   make kafka-topics
   ```

3. Rulează worker-ul de streaming:
   ```bash
   poetry run python -m streaming.orders_worker --kafka --topic orders --group ecom-orders-dev --ensure-dim-date
   ```

Alternativ, worker-ul pornește automat cu restul serviciilor în Docker Compose.

### Format mesaje

Worker-ul așteaptă mesaje JSON cu următoarea structură:

```json
{
  "order_id": "12345",
  "order_line_nbr": 1,
  "transaction_ts": "2025-10-26T10:00:00Z",
  "customer_nk": "CUST123",
  "email": "customer@example.com",
  "sku": "PROD456",
  "quantity": 2,
  "unit_price": 99.99,
  "discount_pct": 0.1
}
```

Suportă și formatul CDC Debezium:

```json
{
  "op": "c",
  "ts_ms": 1698321600000,
  "after": {
    "order_id": "12345",
    "order_line_nbr": 1,
    ... // câmpurile de mai sus
  }
}
```

### Monitorizare

Pentru a vizualiza lag-ul și partiționarea în Redpanda Console:

1. Activează profilul `obs`:
   ```bash
   docker compose --profile obs up -d redpanda-console
   ```

2. Accesează [http://localhost:8080](http://localhost:8080)

Metricile de streaming sunt expuse la `/metrics` și includ:
- `ecom_kafka_partitions_assigned{topic,group}`
- `ecom_kafka_commits_total{topic,group}`
- `ecom_kafka_poll_records{le}`
- `ecom_kafka_consumer_lag{topic,partition,group}`
   ```bash
   make train
   ```

6. Verifică API-ul:
   ```bash
   curl http://localhost:8000/docs
   ```

## Profil prod (local)

Pentru a rula în profil producție local (fără porturi expuse):
```bash
docker compose --profile prod up -d --build
```

## Health & Troubleshooting

### Verificare servicii
```bash
docker compose ps
```

### Vizualizare loguri
```bash
docker compose logs
```

### Verificare sănătate
```bash
docker compose exec backend curl http://localhost:8000/health

## Running the API locally (against Docker services)

If you want to run the API locally (for faster edit/reload) but still use the Dockerized infrastructure
(Postgres, Redis, MinIO, Redpanda), you can:

1. Start the dev compose profile:

```powershell
docker compose -f infra/docker-compose.yml --profile dev up -d
```

2. Run the helper script from the project root (PowerShell):

```powershell
.\run_local.ps1
```

This script sets sensible environment variables that point to the services exposed on localhost
by Docker Compose and then launches `uvicorn` via `poetry run` so you get auto-reload while
still talking to the running containers.

Notes:
- If you prefer to run the API fully in Docker, use `docker compose --profile dev up` and open
  http://localhost:8000/health.
- If you run everything locally without Docker, set the same environment variables but point
  them at your local Postgres/Redis/MinIO installations instead of `localhost` ports.

```

### Verificare bază de date
```bash
docker compose exec db psql -U postgres -d ppc -c "SELECT version();"
```

## Generare date sintetice (seed)

### Prerequisites
Before running the seed commands, ensure the environment is set up:

1. Copy the environment configuration file:
   ```bash
   cp .env.example .env
   ```
   Then, edit `.env` to set the database connection variables (e.g., DB_HOST, DB_PORT, etc.).

2. Start the database services:
   ```bash
   make compose-up
   ```

### Commands
- `make seed-generate` – produce fișierele sintetice conform `infra/seed/seed_config.yaml`.
- `make seed-dim-date` – populează `dw.dim_date` pentru intervalul configurat (implicit 2023-01-01 → 2025-12-31).
- `make seed` – rulează întregul flux (dim_date → generate → ETL load). Configurarea perioadei se face cu variabilele `SEED_START_DATE` și `SEED_END_DATE`.

Examples:
```bash
make seed
make seed SEED_START_DATE=2024-01-01 SEED_END_DATE=2024-12-31
```

## Autentificare & RBAC

Platforma expune un mecanism JWT cu roluri aplicate pe endpoint-uri.

- **Roluri**:
  - `admin` – acces complet (analytics, recomandări, operațiuni administrative)
  - `analyst` – acces la agregările de vânzări
  - `app` – acces la API-ul de recomandări pentru aplicații externe
- **Variabile cheie**:
  - `AUTH_REQUIRE_AUTH` – când este `true`, toate rutele protejate cer token valid
  - `AUTH_DEV_USERS_ENABLED` – permite endpoint-ul de login în mediul de dezvoltare
  - `JWT_SECRET`, `JWT_ALG`, `AUTH_ACCESS_TOKEN_EXPIRES_SECONDS` – control pentru semnarea și durata token-urilor

### Utilizatori demo

```bash
make create-admin     # username=admin, role=admin
make create-analyst   # username=analyst, role=analyst
make create-app       # username=app, role=app
```

CLI-ul nu afisează parolele; acestea sunt hash-uite în `meta.app_user`.

### Obținerea token-ului

```bash
curl -X POST \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=admin&password=admin" \
  http://localhost:8000/auth/login
```

Exemplu de apel protejat:

```bash
TOKEN=$(curl -s -X POST -d "username=analyst&password=analyst" http://localhost:8000/auth/login | jq -r .access_token)
curl -H "Authorization: Bearer $TOKEN" "http://localhost:8000/api/v1/sales/summary?granularity=day"
```

Endpoint-uri și roluri:

- `/api/v1/sales/*` – `analyst` sau `admin`
- `/api/v1/recs/user/*`, `/api/v1/recs/similar-products/*` – `app`, `analyst`, `admin`
- `/api/v1/recs/_refresh` – doar `admin` și `RECS_ALLOW_REFRESH_ENDPOINT=true`
- `/admin/ping` – doar `admin`

În dezvoltare (`AUTH_REQUIRE_AUTH=false`) aplicația permite acces fără token pentru a facilita testarea rapidă.

## PII & Masking

- Email-urile sunt stocate ca SHA-256 (`email_hash`) și nu sunt returnate în API.
- Utilitarele din `backend/app/utils/masking.py` maschează telefonul și emailul atunci când sunt afișate.
- `safe_customer_payload` oferă un subset sigur de câmpuri despre client (fără email/telefon în clar).


## ?nc?rc?ri incrementale (CDC-like)

- Strategii suportate:
  - **watermark** (implicit) ? proceseaz? doar ?nregistr?rile cu updated_at/	ransaction_ts mai noi dec?t valoarea salvat? ?n meta.cdc_state.
  - **hash** ? folose?te fingerprint SHA-256 pe c?mpuri canonice ?i actualizeaz? numai c?nd con?inutul difer?.
- Comenzi utile:
  - make inc-customers
  - make inc-products
  - make inc-orders
  - make inc-all
- Resetarea watermark-ului: ?terge r?ndul corespunz?tor din meta.cdc_state (ex. DELETE FROM meta.cdc_state WHERE entity='customers';) sau ruleaz? incrementalul cu --from-ts pentru a seta manual punctul de plecare.

## CI/CD

- Workflow `ci.yml` rulează automat la `push`/`pull_request` și include joburile `lint_type`, `tests_unit`, `build_docker`, plus `tests_e2e` (activ automat pe `main` sau manual pe PR aplicând label-ul `run-e2e`).
- Artefactele `coverage.xml`, `pytest-report.xml`, `e2e_report.xml`, `e2e_report.md` sunt disponibile în tab-ul **Actions** pentru fiecare rulare.
- Workflow-ul `release-docker.yml` se declanșează la tag-uri `v*.*.*` (sau manual) și publică imaginile `backend`, `etl`, `ml` în GitHub Container Registry (`ghcr.io/${ORG}/${REPO}/…`).
- Pentru testare locală a pipeline-ului, rulează `make ci`, iar pentru publicarea de imagini în dev folosește `docker build` din target-urile existente.

## Teste End-to-End

Aplicația include teste end-to-end (E2E) pentru validarea completă a pipeline-ului, inclusiv verificări API și bazei de date.

### Cerințe preliminare

Înainte de rularea testelor E2E, asigurați-vă că mediul este pregătit:

1. Porniți serviciile:
   ```bash
   make compose-up
   ```

2. Aplicați migrațiile bazei de date:
   ```bash
   alembic upgrade head
   ```

### Rulare

Aveți două opțiuni pentru rularea testelor E2E:

- **Flux complet automat**: `make e2e-up`

  Acestă comandă pornește serviciile, aplică migrațiile, generează date sintetice, antrenează modelul și rulează testele E2E.

- **Pași manuali**:
  ```bash
  make seed
  poetry run python -m ml.als_train --lookback-days 365
  poetry run pytest -k test_e2e_pipeline -q
  ```

### Rapoarte

Testele E2E generează două tipuri de rapoarte în directorul rădăcină al proiectului:

- `e2e_report.xml`: Raport JUnit XML pentru integrare cu sisteme CI/CD.
- `e2e-report.md`: Raport Markdown citibil pentru oameni, cu detalii despre fiecare verificare.

### Depanare

Dacă testele E2E eșuează, verificați următoarele:

- **Număr rânduri în tabele**: Verificați că datele au fost încărcate corect:
  ```bash
  docker compose exec db psql -U postgres -d ppc -c "SELECT schemaname, tablename, n_tup_ins as rows FROM pg_stat_user_tables;"
  ```

- **Inspectare metrici**: Verificați endpoint-ul de metrici pentru probleme:
  ```bash
  curl http://localhost:8000/metrics
  ```

- **Verificare loguri**: Examinați logurile serviciilor:
  ```bash
  docker compose logs api
  docker compose logs etl
  docker compose logs ml
  ```


## API de agregare vânzări

### Exemple de cereri

- Rezumat vânzări pe zi:
  ```bash
  curl "http://localhost:8000/api/v1/sales/summary?from=2024-01-01&to=2024-01-31&granularity=day"
  ```

- Top produse după valoare netă:
  ```bash
  curl "http://localhost:8000/api/v1/sales/top-products?from=2024-01-01&to=2024-01-31&metric=net&limit=10"
  ```

### Note

- **Caching**: Răspunsurile API sunt cache-uite în Redis cu un TTL implicit de 60 de secunde. TTL-ul poate fi configurat prin variabila `SALES_CACHE_TTL_SECONDS` în fișierul `.env`.
- **Rate limiting**: Este activat implicit. Pentru a dezactiva rate limiting-ul, setați `RATE_LIMIT_ENABLED=false` în fișierul `.env`.

## API Recomandări

### Exemple de cereri

- Recomandări pentru utilizator:
  ```bash
  curl "http://localhost:8000/api/v1/recs/user/123?k=10&exclude_seen=true"
  ```

- Produse similare:
  ```bash
  curl "http://localhost:8000/api/v1/recs/similar-products/SKU-ABC123?k=10"
  ```

- Reîmprospătare model:
  ```bash
  curl -X POST "http://localhost:8000/api/v1/recs/_refresh"
  ```

### Câmpuri răspuns

- `user_id` / `sku`: ID-ul utilizatorului sau SKU-ul produsului de referință
- `k`: Numărul de recomandări solicitate
- `model_version`: Versiunea modelului ALS utilizat
- `items`: Lista de produse recomandate, fiecare conținând:
  - `sku`: Codul produsului
  - `name`: Numele produsului
  - `category`: Categoria produsului
  - `score`: Scorul de similaritate sau relevanță
  - `reason`: Motivul recomandării ("als" pentru modelul ALS, "popular" pentru fallback pe popularitate, "similarity" pentru produse similare)

### Note

- **Caching**: Răspunsurile API sunt cache-uite în Redis cu un TTL configurabil prin variabila `RECS_CACHE_TTL_SECONDS` în fișierul `.env`.
- **Endpoint reîmprospătare**: Acest endpoint este dezactivat implicit pentru siguranță. Pentru a-l activa, setați `RECS_ALLOW_REFRESH_ENDPOINT=true` în `.env`. Utilizați cu precauție în producție, deoarece reîncarcă modelul în memorie.

## Recomandări ALS (implicit)

### Antrenare model

Pentru antrenarea modelului de recomandări ALS, rulează:

```bash
make train
```

Comanda antrenează modelul folosind datele din ultimele 365 de zile din `dw.fact_sales`, aplicând filtre pentru utilizatori cu minim 3 achiziții și produse cu minim 5 achiziții. Parametrii de antrenare pot fi configurați în `ml/settings.py` sau prin variabile de mediu.

### Artefacte model

Artefactele antrenate sunt salvate în directorul `ml/artifacts/` cu nume de forma `als_model_YYYYMMDD_HHMMSS/`. Fiecare director conține:

- `user_factors.npz` și `item_factors.npz`: Factorii latenți ai modelului ALS
- `mappings.json`: Mapări între ID-urile bazei de date și indicii interni
- `popularity.json`: Date de popularitate pentru fallback
- `model.json`: Metadate model (parametri, metrici, checksum-uri)

### Metrici evaluare

Modelul este evaluat offline pe un set de test (80% antrenare, 20% test) cu metrici la K=10:
- Precision@10
- Recall@10
- MAP@10
- NDCG@10

### Limitări cold-start și strategie fallback

- **Utilizatori necunoscuți**: Pentru utilizatori fără istoric de achiziții, sistemul folosește recomandări bazate pe popularitate (număr total de interacțiuni per produs).
- **Produse noi**: Produsele fără suficiente interacțiuni sunt excluse din antrenare.
- **Fallback activat**: Când ALS nu poate genera recomandări, sistemul revine automat la modelul de popularitate.

### Note

- Pentru antrenare rapidă în dezvoltare, dezactivați rate limiting-ul setând `RATE_LIMIT_ENABLED=false` în `.env`.
- Modelul folosește biblioteca `implicit` pentru implementarea ALS eficientă.

## Caching & Invalidation

Platforma implementează un sistem de caching Redis cu două strategii de invalidare pentru optimizarea performanței API-urilor.

### Strategii de Invalidation

- **Namespace bump**: Incrementarea versiunii namespace-ului pentru invalidarea tuturor cheilor cache dintr-un domeniu (implicit)
- **Selective invalidation**: Ștergerea selectivă a cheilor cache bazată pe indexuri pentru invalidări mai precise

### Activarea Selective Invalidation

Pentru a activa invalidarea selectivă, setați `CACHE_SELECTIVE_ENABLED=true` în fișierul `.env`.

### Evenimente ETL-Driven

ETL-ul publică evenimente de invalidare către Redis Pub/Sub atunci când datele sunt actualizate cu succes. Evenimentele conțin:
- `target`: Domeniul afectat ("sales" sau "recs")
- `strategy`: Strategia de invalidare ("namespace" sau "selective")
- `payload`: Date suplimentare pentru invalidare selectivă (ex: date range, canale pentru sales; user_id/sku pentru recs)

Evenimentele sunt persistate în tabela `meta.cache_events` pentru audit.

### Endpoint-uri Admin

```bash
# Bump namespace pentru sales sau recs
curl -X POST -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/v1/cache/_bump/sales"

# Purge selectiv cache pentru sales și/sau recs
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sales": true, "recs": false}' \
  "http://localhost:8000/api/v1/cache/_purge"
```

### Metrici ETL

Pentru a trimite metricile ETL către Pushgateway Prometheus, setați variabila de mediu `PUSHGATEWAY_URL` în `.env`.

## Observabilitate

Aplicația include funcționalități de observabilitate pentru monitorizare și depanare, inclusiv logging JSON, metrici Prometheus și endpoint-uri de sănătate.

### Logging JSON

Aplicația folosește logging structurat în format JSON pentru toate mesajele de log. Logurile includ câmpuri precum timestamp, nivel, mesaj și metadate suplimentare pentru ușurința agregării și căutării în sisteme precum ELK Stack sau Loki.

### Metrici

Metricile sunt expuse prin endpoint-ul `/metrics` în format Prometheus, inclusiv:

- **requests_total**: Numărul total de cereri HTTP, etichetate după endpoint, metodă și status.
- **request_latency_seconds**: Histograma latenței cererilor, cu bucket-uri predefinite.
- **cache_hits_total** și **cache_misses_total**: Numărul de hit-uri și miss-uri în cache, etichetate după endpoint.
- **rate_limited_total**: Numărul de răspunsuri limitate de rate.
- **db_pool_connections**: Numărul de conexiuni în pool-ul bazei de date, etichetate după stare.

Exemplu de interogare metrici:
```bash
curl http://localhost:8000/metrics
```

### Endpoint-uri de sănătate

- **/health**: Verificare simplă de sănătate care returnează întotdeauna `{"status": "ok"}`.
- **/livez**: Verificare de liveness pentru a confirma că bucla de evenimente este funcțională.
- **/readyz**: Verificare de readiness care testează conectivitatea la baza de date, Redis și recomandatorul ALS (dacă este configurat).

Exemple de cereri:
```bash
curl http://localhost:8000/health
curl http://localhost:8000/livez
curl http://localhost:8000/readyz
```

### Pornirea stivei de observabilitate

Pentru a porni serviciile de observabilitate (Prometheus, Pushgateway, Grafana), utilizați:
```bash
make obs-up
```

### Acces la servicii

- **Prometheus**: http://localhost:9090
- **Pushgateway**: http://localhost:9091
- **Grafana**: http://localhost:3000

### Notă privind PUSHGATEWAY_URL

Pentru metricile ETL, setați variabila de mediu `PUSHGATEWAY_URL` în `.env` pentru a trimite metricile către Pushgateway.

## Streaming (dev)

Platforma include funcționalități de streaming pentru procesarea în timp real a comenzilor folosind Apache Kafka. În mediul de dezvoltare, poți genera fișiere NDJSON (Newline Delimited JSON) pentru simularea datelor de streaming.

### Generare fișiere NDJSON

Pentru a genera fișiere NDJSON din datele sintetice existente:

```bash
# Convertește JSON array în NDJSON (un obiect JSON per linie)
jq -c '.[]' infra/seed/data/orders_0001.json > orders.ndjson

# Sau pentru toate fișierele orders
for file in infra/seed/data/orders_*.json; do
  jq -c '.[]' "$file" >> orders.ndjson
done
```

### Pornirea worker-ului de streaming

Worker-ul de streaming este pornit automat la startup-ul aplicației FastAPI și se abonează la topic-ul `orders` din Kafka. Pentru a porni aplicația în mod dezvoltare cu streaming activat:

```bash
make compose-up
make run-api
```

Worker-ul va procesa mesajele din topic-ul Kafka și va insera comenzile în baza de date folosind `bulk_insert_orders`. Configurația streaming-ului poate fi ajustată prin variabilele de mediu cu prefixul `STREAMING_` în fișierul `.env`:

- `STREAMING_KAFKA_BOOTSTRAP_SERVERS`: Adresa serverelor Kafka (implicit `localhost:9092`)
- `STREAMING_KAFKA_TOPIC_ORDERS`: Numele topic-ului pentru comenzi (implicit `orders`)
- `STREAMING_KAFKA_GROUP_ID`: ID-ul grupului de consumatori (implicit `streaming_group`)
- `STREAMING_BATCH_SIZE`: Mărimea batch-ului pentru procesare (implicit `100`)
- `STREAMING_POLL_TIMEOUT_MS`: Timeout pentru polling (implicit `1000`)

În caz de erori, worker-ul va încerca să reînceapă procesarea după o pauză scurtă.
