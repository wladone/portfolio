param(
  [Parameter(Mandatory=$true)]
  [string]$ProjectId,
  [string]$Region = "europe-central2",
  [ValidateSet("local","flex","direct")]
  [string]$RunMode = "local",
  [switch]$PublishSamples,
  [switch]$SkipAuthCheck
)

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$setupScript = Join-Path $root 'scripts/setup.ps1'
$runScript = Join-Path $root 'scripts/run.ps1'
$publishScript = Join-Path $root 'scripts/publish-samples.ps1'

Write-Host "=== Pipeline helper ===" -ForegroundColor Cyan

& $setupScript -ProjectId $ProjectId -Region $Region -SkipAuthCheck:$SkipAuthCheck.IsPresent
& $runScript -Mode $RunMode

if ($PublishSamples) {
    & $publishScript -ProjectId $ProjectId -Region $Region
}

Write-Host "? Pipeline tasks completed." -ForegroundColor Green
