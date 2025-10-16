param(
  [Parameter(Mandatory = $true)]
  [string]$ProjectId,
  [string]$Region = "europe-central2",
  [switch]$SkipAuthCheck
)

function Require-Cmd {
  param([string]$Name, [string]$Hint)
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Missing command: $Name. $Hint"
  }
}

function Invoke-Exec {
  param([string]$Cmd, [string[]]$Args)
  & $Cmd @Args
  if ($LASTEXITCODE -ne 0) {
    throw "Command failed: $Cmd $($Args -join ' ')"
  }
}

Require-Cmd gcloud "Install Google Cloud SDK and ensure it's on PATH."
Require-Cmd gsutil "Install Google Cloud SDK and ensure it's on PATH."
Require-Cmd bq "Install Google Cloud SDK and ensure it's on PATH."
Require-Cmd python "Install Python 3.10+ and ensure it's on PATH."

$currentProject = (gcloud config get-value project --quiet) 2>$null
if ($currentProject -ne $ProjectId) {
  Write-Host "Setting default gcloud project to $ProjectId" -ForegroundColor DarkCyan
  Invoke-Exec gcloud @('config','set','project',$ProjectId)
}

if (-not $SkipAuthCheck.IsPresent) {
  $authStatus = gcloud auth list --filter=status:ACTIVE --format="value(account)" 2>$null
  if (-not $authStatus) {
    Write-Warning "No active gcloud authentication. Run 'gcloud auth login' and optionally 'gcloud auth application-default login'."
    throw "Authentication required."
  }
}

$envPath = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) '..' '.env'
if (Test-Path $envPath) {
  Get-Content $envPath | ForEach-Object {
    if ($_ -match '^(?<k>[^=]+)=(?<v>.*)$') {
      Set-Item -Path "Env:$($matches.k.Trim())" -Value $matches.v.Trim()
    }
  }
  Write-Host "Loaded .env variables." -ForegroundColor DarkGray
}

Write-Host "Environment ready for project=$ProjectId region=$Region" -ForegroundColor Green
