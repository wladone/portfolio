<#
  Wrapper script for Windows that provisions the virtual environment, runs optional tests,
  and launches the product deduplication pipeline with a single command.
#>

param(
  [Parameter(Mandatory = $true)] [string] $InputPath,
  [string] $OutDir = "data/processed",
  [double] $Threshold = 0.80,
  [switch] $Setup,
  [switch] $Test
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

Write-Step "Executing pipeline"
$argsList = @((Join-Path $repo 'scripts/run_dedup.py'), '--input', $InputPath, '--out-dir', $OutDir, '--threshold', $Threshold)
$proc = Start-Process -FilePath $venvPython -ArgumentList $argsList -NoNewWindow -PassThru -Wait
$pipelineExit = $proc.ExitCode
Write-Step "Pipeline exit code: $pipelineExit"

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

