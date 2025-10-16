param(
    [string]$Entry = "..\ui\banner.py",
    [string]$Name = "SysMonBanner",
    [string]$Dist = "..\..\dist"
)

$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$venvDir = Join-Path $repo '.venv'
$venvPy = Join-Path $venvDir 'Scripts\python.exe'

if (-not (Test-Path $venvPy)) {
    Write-Host 'Creating virtual environment (.venv)...'
    if (Get-Command py -ErrorAction SilentlyContinue) { & py -3 -m venv $venvDir } else { & python -m venv $venvDir }
}

Write-Host 'Installing build dependencies (pyinstaller)...'
& $venvPy -m pip install --upgrade pip | Out-Null
& $venvPy -m pip install -r (Join-Path $repo 'prototype\requirements.txt') | Out-Null
& $venvPy -m pip install pyinstaller | Out-Null

$entryPath = Resolve-Path (Join-Path $PSScriptRoot $Entry)
Write-Host "Building $Name from $entryPath..."

& $venvPy -m PyInstaller --noconfirm --name $Name --clean --onefile `
    --windowed `
    --add-data ("$($repo)\\prototype\\ui;prototype\\ui") `
    $entryPath

New-Item -ItemType Directory -Path $Dist -Force | Out-Null
$src = Join-Path $repo "dist\$Name.exe"
if (Test-Path $src) {
    Copy-Item $src -Destination (Join-Path $Dist "$Name.exe") -Force
    Write-Host "Built: $(Join-Path $Dist "$Name.exe")"
} else {
    Write-Error 'Build failed; executable not found in dist/'
    exit 1
}

