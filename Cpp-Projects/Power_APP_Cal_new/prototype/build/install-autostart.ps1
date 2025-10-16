param(
    [ValidateSet('Install','Remove')]
    [string]$Action = 'Install',
    [ValidateSet('Desktop','Banner')]
    [string]$Target = 'Desktop',
    [string]$ExePath = '',
    [string]$Args = ''
)

$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$runPs1 = Join-Path $repo 'run.ps1'

$runKey = 'HKCU:Software\Microsoft\Windows\CurrentVersion\Run'
$name = if ($Target -eq 'Desktop') { 'SysMonDesktop' } else { 'SysMonBanner' }

if ($Action -eq 'Install') {
    if (-not $ExePath) {
        if ($Target -eq 'Desktop') {
            $ExePath = (Join-Path $repo 'dist\SysMonDesktop.exe')
            $Args = if ($Args) { $Args } else { '--background' }
        } else {
            # Banner: use run.ps1 to manage venv and launch hidden
            $ExePath = (Get-Command powershell).Source
            $Args = if ($Args) { $Args } else { "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$runPs1`" -Banner -Background" }
        }
    }
    if (-not (Test-Path $ExePath)) { Write-Error "Path not found: $ExePath"; exit 1 }
    $value = if ($Target -eq 'Desktop' -and $Args) { '"' + $ExePath + '" ' + $Args } elseif ($Target -eq 'Desktop') { '"' + $ExePath + '"' } else { '"' + $ExePath + '" ' + $Args }
    New-Item -Path $runKey -Force | Out-Null
    New-ItemProperty -Path $runKey -Name $name -Value $value -PropertyType String -Force | Out-Null
    Write-Host "Autostart installed for $Target as '$name'"
} else {
    if (Get-ItemProperty -Path $runKey -Name $name -ErrorAction SilentlyContinue) {
        Remove-ItemProperty -Path $runKey -Name $name -ErrorAction SilentlyContinue
        Write-Host "Autostart removed for '$name'"
    } else {
        Write-Host "No autostart entry found for '$name'"
    }
}

