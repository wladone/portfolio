<#
run_gui.ps1
Creates a venv if missing, installs deps, fixes Tcl/Tk if needed, and launches the GUI.
#>

$ErrorActionPreference = 'Stop'

$venv = Join-Path $PWD '.venv'
$py = Join-Path $venv 'Scripts\python.exe'
$pip = Join-Path $venv 'Scripts\pip.exe'

if (-not (Test-Path $venv)) {
  Write-Host 'Creating virtual environment (.venv)...' -ForegroundColor Yellow
  py -3 -m venv .venv
}

if (-not (Test-Path $py)) { throw "Venv python not found at $py" }

Write-Host 'Upgrading pip and installing requirements...' -ForegroundColor Yellow
try {
  & $py -m pip install --upgrade pip | Out-Null
  & $pip install -r requirements.txt
} catch {
  Write-Host 'Package installation failed (possibly offline). Continuing if deps already present.' -ForegroundColor Yellow
}

# Try tkinter import; if it fails, attempt to fix Tcl/Tk
Write-Host 'Checking tkinter availability...' -ForegroundColor Yellow
& $py -c "import tkinter, sys; print('tkinter OK')" | Out-Null
if ($LASTEXITCODE -ne 0) {
  if (Test-Path 'fix_tk.ps1') {
    Write-Host 'Trying to fix Tcl/Tk with fix_tk.ps1...' -ForegroundColor Yellow
    .\fix_tk.ps1
  } else {
    Write-Host 'fix_tk.ps1 not found; tkinter may not work.' -ForegroundColor Red
  }
}

Write-Host 'Launching GUI...' -ForegroundColor Green
& $py -m imcrypt.cli gui
