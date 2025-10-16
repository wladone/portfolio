if (!(Test-Path ".venv")) { py -3.10 -m venv .venv }
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r beam/requirements.txt -r requirements-dev.txt
Write-Host "✅ venv ready. Activate next time with .\.venv\Scripts\Activate.ps1"
