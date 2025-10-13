
param(
  [string]$TabularEditorPath2 = "C:\Program Files (x86)\Tabular Editor\TabularEditor.exe",
  [string]$TabularEditorPath3 = "C:\Program Files\Tabular Editor 3\TabularEditor.exe",
  [string]$ScriptPath = ".\CreateRelationships.csx"
)

function Run-TE {
  param([string]$exe, [string]$script)
  if (Test-Path $exe) {
    & $exe -PBI -S $script -Z
    return $true
  }
  return $false
}

Write-Host "Attempting Tabular Editor 3..."
if (-not (Run-TE -exe $TabularEditorPath3 -script $ScriptPath)) {
  Write-Host "Tabular Editor 3 not found. Trying Tabular Editor 2..."
  if (-not (Run-TE -exe $TabularEditorPath2 -script $ScriptPath)) {
    Write-Error "Tabular Editor not found. Please install TE2/TE3 and update the paths."
    exit 1
  }
}

Write-Host "Relationships script applied successfully to the open Power BI model."
