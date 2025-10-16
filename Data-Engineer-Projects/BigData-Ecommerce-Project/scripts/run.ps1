param(
  [ValidateSet("local","flex","direct")]
  [string]$Mode = "local"
)

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvPython = Join-Path $root '..' '.venv' 'Scripts' 'python.exe'
$python = if (Test-Path $venvPython) { $venvPython } else { (Get-Command python -ErrorAction Stop).Source }

Write-Host "Running pipeline in mode: $Mode" -ForegroundColor Cyan

& $python (Join-Path $root '..' 'run.py') $Mode
if ($LASTEXITCODE -ne 0) {
  throw "run.py exited with non-zero status."
}
