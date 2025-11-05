param(
  [string]$PythonExe = "python",
  [string]$ConfigPath = "config/config.yaml"
)
if (-not (Test-Path ".venv")) { & $PythonExe -m venv .venv }
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python scripts\run_er.py --qc --report --config $ConfigPath
