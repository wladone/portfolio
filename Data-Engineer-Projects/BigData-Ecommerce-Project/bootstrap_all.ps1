# bootstrap_all.ps1
# Windows PowerShell script to bootstrap & optionally run the project end-to-end.
# Place this file in the repo root and run from there:  .\bootstrap_all.ps1 -RunMode local
param(
  [string]$ProjectId = "",
  [string]$Region = "europe-central2",
  [string]$Zone = "",
  [string]$ComposerEnv = "pde-composer",
  [switch]$SkipFlexBuild,
  [ValidateSet("none","local","flex","direct")] [string]$RunMode = "none",
  [switch]$CheapMode,           # minimize costs: tiny Dataflow, TTLs, lifecycle
  [switch]$PublishSamples       # send 3 test Pub/Sub messages at the end
)

function Test-CommandExists($name, $installHint="") {
  if (-not (Get-Command $name -ErrorAction SilentlyContinue)) {
    Write-Error "Missing command: $name. $installHint"
    exit 1
  }
}

function Exec($file, $arguments) {
  & $file @arguments
  if ($LASTEXITCODE -ne 0) { throw "Command failed: $file $($arguments -join ' ')" }
}

Write-Host "=== Bootstrap starting (PowerShell) ===" -ForegroundColor Cyan

# 0) Tool checks
Test-CommandExists gcloud "Install Google Cloud SDK."
Test-CommandExists gsutil "Installed with Google Cloud SDK."
Test-CommandExists bq "Installed with Google Cloud SDK."
# cbt is optional; we'll warn if missing
$HasCbt = $true
if (-not (Get-Command cbt -ErrorAction SilentlyContinue)) {
  $HasCbt = $false
  Write-Warning "cbt not found. Install with: gcloud components install cbt"
}
# Python for venv and run.py
$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if ($pythonCmd) { $PythonExe = $pythonCmd.Source } else { $PythonExe = $null }
if (-not $PythonExe) {
  $pythonCmd = Get-Command py -ErrorAction SilentlyContinue
  if ($pythonCmd) { $PythonExe = $pythonCmd.Source } else { $PythonExe = $null }
}
if (-not $PythonExe) { Write-Error "Python not found. Install Python 3.10+."; exit 1 }

# 1) Discover or set project
if (-not $ProjectId -or $ProjectId.Trim() -eq "") {
  $ProjectId = (gcloud config get-value project --quiet) 2>$null
}
if (-not $ProjectId -or $ProjectId.Trim() -eq "") {
  Write-Error "No project set. Run: gcloud auth login; gcloud config set project <YOUR_PROJECT_ID>"
  exit 2
}
Exec gcloud @("config","set","project",$ProjectId) | Out-Null

if (-not $Zone -or $Zone.Trim() -eq "") { $Zone = "$Region-b" }

# 2) Enable required services
$services = @(
  "dataflow.googleapis.com","pubsub.googleapis.com","bigquery.googleapis.com",
  "bigtable.googleapis.com","bigtableadmin.googleapis.com",
  "composer.googleapis.com","cloudbuild.googleapis.com"
)
Write-Host "Enabling APIs (idempotent)..." -ForegroundColor DarkCyan
foreach ($s in $services) { Exec gcloud @("services","enable",$s,"--project",$ProjectId) }

# 3) Load existing .env if present (reuse values)
$envPath = Join-Path (Get-Location) ".env"
$ENVVARS = @{}
if (Test-Path $envPath) {
  Get-Content $envPath | ForEach-Object {
    if ($_ -match "^(?<k>[^=]+)=(?<v>.*)$") { $ENVVARS[$matches.k.Trim()] = $matches.v.Trim() }
  }
}

# 4) Compute resource names (bucket must be globally unique)
function Get-RandomSuffix { ([Guid]::NewGuid().ToString("N")).Substring(0,6).ToLower() }
$safeProj = ($ProjectId.ToLower() -replace "[^a-z0-9-]","")
$DATAFLOW_BUCKET = if ($ENVVARS["DATAFLOW_BUCKET"]) { $ENVVARS["DATAFLOW_BUCKET"] } else { "pde-$safeProj-$Region-df-$(Get-RandomSuffix)" }
$BIGTABLE_INSTANCE = if ($ENVVARS["BIGTABLE_INSTANCE"]) { $ENVVARS["BIGTABLE_INSTANCE"] } else { "pde-$safeProj-bt" }
$BIGTABLE_TABLE = if ($ENVVARS["BIGTABLE_TABLE"]) { $ENVVARS["BIGTABLE_TABLE"] } else { "product_stats" }
$COMPOSER_ENV = if ($ENVVARS["COMPOSER_ENV"]) { $ENVVARS["COMPOSER_ENV"] } else { $ComposerEnv }
$TEMPLATE_PATH = "gs://$DATAFLOW_BUCKET/templates/streaming_pipeline_flex_template.json"

Write-Host "Project: $ProjectId"
Write-Host "Region:  $Region"
Write-Host "Bucket:  gs://$DATAFLOW_BUCKET"
Write-Host "Bigtable: $BIGTABLE_INSTANCE  table: $BIGTABLE_TABLE  zone: $Zone"
Write-Host "Template: $TEMPLATE_PATH"

# 5) Create bucket (regional)
Write-Host "Ensuring GCS bucket..." -ForegroundColor DarkCyan
& gsutil ls -b "gs://$DATAFLOW_BUCKET" *> $null
if ($LASTEXITCODE -ne 0) {
  Exec gsutil @("mb","-l",$Region,"gs://$DATAFLOW_BUCKET")
} else { Write-Host "Bucket exists." }

# 6) BigQuery dataset (regional)
Write-Host "Ensuring BigQuery dataset..." -ForegroundColor DarkCyan
& bq --location=$Region ls -d "$ProjectId:ecommerce" *> $null
if ($LASTEXITCODE -ne 0) {
  Exec bq @("--location=$Region","mk","--dataset","$ProjectId:ecommerce")
} else { Write-Host "Dataset exists." }

# 7) Pub/Sub topics
Write-Host "Ensuring Pub/Sub topics..." -ForegroundColor DarkCyan
foreach ($t in @("clicks","transactions","stock","dead-letter")) {
  & gcloud pubsub topics describe $t --project $ProjectId *> $null
  if ($LASTEXITCODE -ne 0) { Exec gcloud @("pubsub","topics","create",$t,"--project",$ProjectId) }
  else { Write-Host "Topic exists: $t" }
}

# 8) Bigtable instance / table
Write-Host "Ensuring Bigtable instance..." -ForegroundColor DarkCyan
& gcloud bigtable instances describe $BIGTABLE_INSTANCE --project $ProjectId *> $null
if ($LASTEXITCODE -ne 0) {
  Exec gcloud @("bigtable","instances","create",$BIGTABLE_INSTANCE,
    "--display-name=$BIGTABLE_INSTANCE","--cluster=$BIGTABLE_INSTANCE-c1",
    "--cluster-zone=$Zone","--cluster-nodes=1","--project",$ProjectId)
} else { Write-Host "Instance exists." }
if ($HasCbt) {
  $tables = (cbt -project $ProjectId -instance $BIGTABLE_INSTANCE ls 2>$null)
  if (-not ($tables -match "^\Q$BIGTABLE_TABLE\E$")) {
    Write-Host "Creating Bigtable table + family (cbt)..." -ForegroundColor DarkCyan
    & cbt -project $ProjectId -instance $BIGTABLE_INSTANCE createtable $BIGTABLE_TABLE 2>$null
    & cbt -project $ProjectId -instance $BIGTABLE_INSTANCE createfamily $BIGTABLE_TABLE stats 2>$null
  } else { Write-Host "Table exists." }
}

# 9) Build Flex template (image + template JSON)
if (-not $SkipFlexBuild) {
  Write-Host "Building Flex image (Cloud Build)..." -ForegroundColor DarkCyan
  Exec gcloud @("builds","submit","--tag","gcr.io/$ProjectId/beam-streaming:latest","beam/","--project",$ProjectId)
  Write-Host "Building Flex template spec..." -ForegroundColor DarkCyan
  Exec gcloud @("dataflow","flex-template","build",$TEMPLATE_PATH,
    "--image","gcr.io/$ProjectId/beam-streaming:latest",
    "--sdk-language","PYTHON","--metadata-file","beam/metadata.json",
    "--project",$ProjectId,"--region",$Region)
} else { Write-Host "Skipping Flex build (per flag)." }

# 10) Cheap mode: TTLs + lifecycle + tiny worker defaults note
if ($CheapMode) {
  Write-Host "Applying cheap-mode guardrails..." -ForegroundColor DarkYellow
  # BQ TTLs (3 days)
  & bq update --default_table_expiration 259200 --default_partition_expiration 259200 "$ProjectId:ecommerce" *> $null
  # Pub/Sub retention 24h
  foreach ($t in @("clicks","transactions","stock","dead-letter")) {
    & gcloud pubsub topics update $t --message-retention-duration=86400s *> $null
  }
  # GCS lifecycle for tmp/staging (3 days)
  $lifecycle = @'
{ "rule":[
  {"action":{"type":"Delete"},"condition":{"age":3,"matchesPrefix":["tmp/","staging/"]}}
]}
'@
  $tmpJson = Join-Path $env:TEMP "gcs_lifecycle.json"
  $lifecycle | Out-File -FilePath $tmpJson -Encoding ascii -Force
  Exec gsutil @("lifecycle","set",$tmpJson,"gs://$DATAFLOW_BUCKET")
}

# 11) Write .env with REAL values
$envContent = @"
PROJECT_ID=$ProjectId
REGION=$Region
DATAFLOW_BUCKET=$DATAFLOW_BUCKET
BIGTABLE_INSTANCE=$BIGTABLE_INSTANCE
BIGTABLE_TABLE=$BIGTABLE_TABLE
COMPOSER_ENV=$COMPOSER_ENV
TEMPLATE_PATH=$TEMPLATE_PATH
"@
$envContent | Out-File -FilePath $envPath -Encoding ascii -Force
Write-Host "`nWrote .env:" -ForegroundColor Green
Get-Content $envPath | ForEach-Object { "  $_" }

# 12) Create/upgrade venv and install deps
Write-Host "`nSetting up Python venv + deps..." -ForegroundColor DarkCyan
if (-not (Test-Path ".venv")) { & py -3.10 -m venv .venv }
& .\.venv\Scripts\python.exe -m pip install --upgrade pip
if (Test-Path "beam\requirements.txt") { & .\.venv\Scripts\pip.exe install -r beam\requirements.txt }
if (Test-Path "requirements-dev.txt") { & .\.venv\Scripts\pip.exe install -r requirements-dev.txt }

# 13) Export env vars for this session so run.py sees them
$env:PROJECT_ID = $ProjectId
$env:REGION = $Region
$env:DATAFLOW_BUCKET = $DATAFLOW_BUCKET
$env:BIGTABLE_INSTANCE = $BIGTABLE_INSTANCE
$env:BIGTABLE_TABLE = $BIGTABLE_TABLE
$env:COMPOSER_ENV = $COMPOSER_ENV
$env:TEMPLATE_PATH = $TEMPLATE_PATH
# ---- Run the pipeline on demand --------------------------------------------
function Start-Pipeline {
    param([string]`$Mode)
    switch (`$Mode) {
        "local"  { & .\.venv\Scripts\python.exe run.py local }
        "flex"   {
            if (`$CheapMode) {
                `$env:NUM_WORKERS    = "1"
                `$env:MAX_WORKERS    = "1"
                `$env:WORKER_MACHINE = "e2-standard-2"
            }
            & .\.venv\Scripts\python.exe run.py flex
        }
        "direct" { & .\.venv\Scripts\python.exe run.py direct }
        default  { return }
    }
}

if (`$RunMode -ne "none") {
    Write-Host "`nRunning pipeline: `$RunMode" -ForegroundColor Cyan
    Start-Pipeline -Mode `$RunMode
}

# ---- Optionally publish sample events ---------------------------------------
if (`$PublishSamples) {
    Write-Host "Publishing sample events..." -ForegroundColor DarkCyan

    # Use single-quoted here-strings so JSON braces aren't parsed by PowerShell
    `$ClicksJson = @'
{"event_time":"2025-01-01T12:00:00Z","product_id":"P123","user_id":"U9"}
'@
    `$TxJson = @'
{"event_time":"2025-01-01T12:00:10Z","product_id":"P123","store_id":"S1","qty":1}
'@
    `$StockJson = @'
{"event_time":"2025-01-01T12:00:20Z","product_id":"P123","warehouse_id":"W1","delta":5}
'@

    Exec gcloud @('pubsub','topics','publish','clicks',"--message=`$ClicksJson",'--project',$ProjectId)
    Exec gcloud @('pubsub','topics','publish','transactions',"--message=`$TxJson",'--project',$ProjectId)
    Exec gcloud @('pubsub','topics','publish','stock',"--message=`$StockJson",'--project',$ProjectId)

    Write-Host "✔ Samples published." -ForegroundColor Green
}

Write-Host "`n✔ Bootstrap complete." -ForegroundColor Green
Write-Host "Next: .\.venv\Scripts\Activate.ps1 ; python run.py local   (or flex/direct)"
