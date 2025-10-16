param(
    [ValidateSet('stub','file','stdout')]
    [string]$Mode = 'stub',
    [string]$Engine = '',
    [switch]$UsePyStub,
    [switch]$KillExisting,
    [switch]$Desktop,
    [switch]$Background,
    [switch]$Banner
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

# Try to ensure we don't get blocked by execution policy or MOTW.
try {
    # Process scope does not require admin and avoids changing machine/user policy.
    Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force -ErrorAction SilentlyContinue | Out-Null
} catch {}

try {
    # Unblock this script and common project scripts in case of Mark of the Web.
    $here = Split-Path -Parent $MyInvocation.MyCommand.Path
    $pathsToUnblock = @(
        $MyInvocation.MyCommand.Path,
        (Join-Path $here 'prototype\launch\run.ps1'),
        (Join-Path $here 'prototype\launch\fetch_and_run_artifact.ps1')
    ) | Where-Object { Test-Path $_ }
    foreach ($p in $pathsToUnblock) { Unblock-File -Path $p -ErrorAction SilentlyContinue }
} catch {}

# Resolve repo root
$repo = Split-Path -Parent $MyInvocation.MyCommand.Path

if ($Desktop -or $Banner) {
    # Ensure venv and dependencies once
    $venvDir = Join-Path $repo '.venv'
    $venvPy = Join-Path $venvDir 'Scripts\python.exe'
    if (-not (Test-Path $venvPy)) {
        Write-Host 'Creating virtual environment (.venv)...'
        if (Get-Command py -ErrorAction SilentlyContinue) { & py -3 -m venv $venvDir } else { & python -m venv $venvDir }
    }
    & $venvPy -m pip install --upgrade pip | Out-Null
    & $venvPy -m pip install -r (Join-Path $repo 'prototype\requirements.txt') | Out-Null

    if ($Desktop) {
        $desktopScript = Join-Path $repo 'prototype\ui\desktop_app.py'
        $desktopArgs = @($desktopScript)
        if ($Mode -eq 'stdout' -and $UsePyStub) {
            $stubPath = Join-Path $repo 'prototype\engine\py_engine_stdout_stub.py'
            $desktopArgs += @('--engine-args', $venvPy, $stubPath)
        } elseif ($Engine) {
            $desktopArgs += @('--engine', $Engine)
        }
        if ($Background) { $desktopArgs += '--background' }
        Write-Host "Launching desktop UI..."
        Start-Process -FilePath $venvPy -ArgumentList (Join-Args $desktopArgs) -NoNewWindow | Out-Null
    }

    if ($Banner) {
        $bannerScript = Join-Path $repo 'prototype\ui\banner.py'
        $bannerArgs = @($bannerScript)
        if ($Mode -eq 'stdout' -and $UsePyStub) {
            $stubPath = Join-Path $repo 'prototype\engine\py_engine_stdout_stub.py'
            $bannerArgs += @('--engine-args', $venvPy, $stubPath)
        } elseif ($Engine) {
            $bannerArgs += @('--engine', $Engine)
        }
        if ($Background) { $bannerArgs += '--background' }
        Write-Host "Launching banner overlay..."
        Start-Process -FilePath $venvPy -ArgumentList (Join-Args $bannerArgs) -NoNewWindow | Out-Null
    }

    Write-Host 'Done. Requested UI(s) should appear shortly.'
} else {
    # Delegate to the main launcher under prototype/launch
    $launcher = Join-Path $repo 'prototype\launch\run.ps1'
    if (-not (Test-Path $launcher)) { Write-Error "Launcher not found: $launcher"; exit 1 }
    $params = @{ Mode = $Mode }
    if ($Engine) { $params.Engine = $Engine }
    if ($UsePyStub) { $params.UsePyStub = $true }
    if ($KillExisting) { $params.KillExisting = $true }
    Write-Host "Using launcher: $launcher"
    & $launcher @params
}
