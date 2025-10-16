param(
    [ValidateSet('Install','Remove')]
    [string]$Action = 'Install',
    [string]$Name = 'SysMonEngine',
    [string]$EnginePath = '..\..\bin\engine_stub.exe',
    [int]$Port = 20123
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$engine = Resolve-Path (Join-Path $here $EnginePath) -ErrorAction SilentlyContinue
if (-not $engine) { Write-Error "Engine executable not found at $EnginePath. Build it first."; exit 1 }

function Install-With-NSSM {
    param($svcName, $exe, $args)
    $nssm = Get-Command nssm -ErrorAction SilentlyContinue
    if (-not $nssm) { return $false }
    & nssm install $svcName $exe $args
    & nssm set $svcName Start SERVICE_AUTO_START | Out-Null
    return $true
}

function Install-As-Task {
    param($taskName, $exe, $args)
    $action = New-ScheduledTaskAction -Execute $exe -Argument $args
    $trigger = New-ScheduledTaskTrigger -AtLogOn
    Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -RunLevel Highest -Force | Out-Null
}

if ($Action -eq 'Install') {
    $args = "-t $Port"
    if (Install-With-NSSM -svcName $Name -exe $engine.Path -args $args) {
        Write-Host "Installed Windows service '$Name' via NSSM."
        Write-Host "Start it: Start-Service $Name"
    } else {
        Install-As-Task -taskName $Name -exe $engine.Path -args $args
        Write-Host "NSSM not found. Installed a Logon Scheduled Task '$Name' as a lightweight wrapper."
    }
} else {
    $nssm = Get-Command nssm -ErrorAction SilentlyContinue
    if ($nssm) {
        & nssm stop $Name | Out-Null
        & nssm remove $Name confirm | Out-Null
        Write-Host "Removed NSSM service '$Name'"
    }
    if (Get-ScheduledTask -TaskName $Name -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $Name -Confirm:$false | Out-Null
        Write-Host "Removed Scheduled Task '$Name'"
    }
}

