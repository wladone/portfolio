<#$
.SYNOPSIS
Runs the Veridion product deduplication pipeline end-to-end on CSV/Parquet.

.DESCRIPTION
- Creates/uses a local virtualenv `.venv`.
- Optionally runs tests with pytest.
- Executes the Python runner `scripts/run_dedup.py`.

.PARAMETER Input
Path to input CSV/Parquet. Alias of -InputPath. If omitted, the script tries common demo files like `data/raw/toy.csv`.

.PARAMETER OutDir
Output directory (default: `data/processed`).

.PARAMETER Threshold
Match score threshold in [0..1] (default: 0.80).

.PARAMETER Setup
Force (re)create venv and install dependencies.

.PARAMETER Test
Run `pytest -q` before executing the pipeline.

.EXAMPLE
./run.ps1 -Input 'data/raw/toy.csv' -Test

.EXAMPLE
./run.ps1 -InputPath 'data/raw/your.parquet' -OutDir 'out' -Threshold 0.82

.EXAMPLE
./run.ps1 -Setup -Input 'data/raw/toy.csv'
#>

param(
  [Alias('Input')] [string] $InputPath,
  [string] $OutDir = "data/processed",
  [double] $Threshold = 0.80,
  [switch] $Setup,
  [switch] $Test,
  [switch] $Help
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Write-Step($msg) { Write-Host "[run] $msg" -ForegroundColor Cyan }
function Write-OK($msg) { Write-Host "[ok]  $msg" -ForegroundColor Green }

$repo = $PSScriptRoot
if (-not $repo) { $repo = Get-Location }
Set-Location $repo

# Choose a Python launcher
$pythonCmd = if (Get-Command python -ErrorAction SilentlyContinue) { 'python' } else { 'py -3' }

$venvPython = Join-Path $repo ".venv/Scripts/python.exe"

# Help and input resolution
if ($Help) { Get-Help -Full $MyInvocation.MyCommand.Path; return }

if (-not $InputPath -or [string]::IsNullOrWhiteSpace($InputPath)) {
  $candidates = @(
    (Join-Path $repo 'data/raw/toy.csv'),
    (Join-Path $repo 'data/raw/veridion_product_deduplication_challenge.parquet'),
    (Join-Path $repo 'data/raw/veridion_product_deduplication_challenge.snappy (1).parquet')
  )
  $found = $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
  if ($found) {
    $InputPath = $found
    Write-Step ("No -Input provided; using demo file: {0}" -f $InputPath)
  } else {
    throw "No -Input/-InputPath provided and no demo file found. Provide a CSV/Parquet path, e.g.: ./run.ps1 -Input 'data/raw/toy.csv'"
  }
}

# If a path was provided but does not exist, try to find a close match
if (-not (Test-Path -LiteralPath $InputPath)) {
  $dir = Split-Path -Parent $InputPath
  if (-not $dir) { $dir = (Join-Path $repo 'data/raw') }
  $leaf = Split-Path -Leaf $InputPath
  $stem = [System.IO.Path]::GetFileNameWithoutExtension($leaf)
  $candidates = Get-ChildItem -Path $dir -File -ErrorAction SilentlyContinue |
    Where-Object { $_.Extension -in @('.parquet', '.csv') -and $_.Name -like ("*{0}*" -f $stem) }
  $alt = $candidates | Select-Object -First 1
  if ($alt) {
    Write-Step ("Input not found. Using closest match: {0}" -f $alt.FullName)
    $InputPath = $alt.FullName
  } else {
    throw ("Input file not found: '{0}'. Try listing files with: Get-ChildItem '{1}'" -f $InputPath, $dir)
  }
}

if ($Setup -or -not (Test-Path $venvPython)) {
  Write-Step "Creating virtual env (.venv)"
  & $pythonCmd -m venv .venv
  Write-Step "Upgrading pip and installing requirements"
  & $venvPython -m pip install -U pip > $null
  & $venvPython -m pip install -r (Join-Path $repo 'requirements.txt')
  Write-OK "Dependencies installed"
}

if ($Test) {
  Write-Step "Running tests"
  & $venvPython -m pytest -q
}

Write-Step ("Executing pipeline (input='{0}', out='{1}', th={2})" -f $InputPath, $OutDir, $Threshold)
$qin = '"' + $InputPath + '"'
$qout = '"' + $OutDir + '"'
$argsList = @((Join-Path $repo 'scripts/run_dedup.py'), '--input', $qin, '--out-dir', $qout, '--threshold', $Threshold)
$proc = Start-Process -FilePath $venvPython -ArgumentList $argsList -NoNewWindow -PassThru -Wait
$pipelineExit = $proc.ExitCode
Write-Step ("Pipeline exit code: {0}" -f $pipelineExit)

if ($pipelineExit -ne 0) {
  Write-Host "[err] Pipeline failed. Skipping summary." -ForegroundColor Red
  exit $pipelineExit
}

Write-Step "Summary"
$py = @'
import pandas as pd
from pathlib import Path
import sys
out_dir = Path(sys.argv[1])
d = pd.read_csv(out_dir / "dedup.csv")
m = pd.read_csv(out_dir / "cluster_assignments.csv")
print("dedup shape:", tuple(d.shape))
print("clusters>1:", int((d["cluster_size"]>1).sum()))
print("sum(cluster_size):", int(d["cluster_size"].sum()))
print("assignments shape:", tuple(m.shape))
print("\nSample:")
print(d[["name","title","brand","cluster_size"]].head(8).to_string(index=False))
'@
$tmp = Join-Path $env:TEMP ("dedup_summary_{0}.py" -f ([guid]::NewGuid().ToString('N')))
Set-Content -Path $tmp -Value $py -Encoding UTF8
& $venvPython $tmp $OutDir
Remove-Item $tmp -Force -ErrorAction SilentlyContinue

Write-OK "Done. Outputs in '$OutDir'"

# Propagate pipeline exit status (force 0 on success)
if ($pipelineExit -ne 0) { exit $pipelineExit } else { $global:LASTEXITCODE = 0; exit 0 }
