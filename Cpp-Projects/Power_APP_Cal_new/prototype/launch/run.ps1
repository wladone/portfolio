param(
    [ValidateSet('stub','file','stdout')]
    [string]$Mode = 'stub',
    [string]$Engine = '',
    [switch]$UsePyStub,
    [switch]$KillExisting
)

$ErrorActionPreference = 'Stop'

# Helper to quote arguments safely for Start-Process
function Join-Args([string[]]$arr) {
    $escaped = @()
    foreach ($a in $arr) {
        if ($a -match '[\s\"`]' ) {
            $escaped += '"' + ($a -replace '"', '\"') + '"'
        } else {
            $escaped += $a
        }
    }
    return ($escaped -join ' ')
}

# Resolve repo root and venv python
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$venvDir = Join-Path $repo '.venv'
$venvPy = Join-Path $venvDir 'Scripts\python.exe'

Write-Host "Repo: $repo"

# Ensure venv exists
if (-not (Test-Path $venvPy)) {
    Write-Host 'Creating virtual environment (.venv)...'
    if (Get-Command py -ErrorAction SilentlyContinue) {
        & py -3 -m venv $venvDir
    } else {
        & python -m venv $venvDir
    }
}

# Install requirements
Write-Host 'Installing/validating Python dependencies...'
& $venvPy -m pip install --upgrade pip | Out-Null
& $venvPy -m pip install -r (Join-Path $repo 'prototype\requirements.txt') | Out-Null

# Optionally kill existing python processes started from this repo
if ($KillExisting) {
    Write-Host 'Stopping existing python processes from this repo...'
    Get-Process python -ErrorAction SilentlyContinue |
        Where-Object { $_.Path -like ("$repo*") } |
        Stop-Process -Force -ErrorAction SilentlyContinue
}

$launcher = Join-Path $repo 'prototype\launch\launch_engine_and_ui.py'
$args = @($launcher, '--mode', $Mode)

# If requested, synthesize engine command for stdout using the Python stub
if ($Mode -eq 'stdout' -and $UsePyStub) {
    $stubPath = Join-Path $repo 'prototype\engine\py_engine_stdout_stub.py'
    # Prefer passing args as a list to avoid quoting issues
    $args += @('--engine-args', $venvPy, $stubPath)
}

switch ($Mode) {
    'stdout' {
        if (-not $UsePyStub) {
            if (-not $Engine) { Write-Error 'stdout mode requires -Engine or -UsePyStub'; exit 1 }
            $args += @('--engine', $Engine)
        }
    }
    'file' {
        if ($Engine) { $args += @('--engine', $Engine) }
    }
}

Write-Host "Launching: $Mode (engine='$Engine')"
Start-Process -FilePath $venvPy -ArgumentList (Join-Args $args) -NoNewWindow | Out-Null
Write-Host 'Done. UI should appear shortly.'
