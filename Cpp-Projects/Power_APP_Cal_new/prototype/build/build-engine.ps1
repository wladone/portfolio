param(
    [string]$Source = "..\engine_stub.cpp",
    [string]$OutDir = "..\..\bin"
)

$ErrorActionPreference = 'Stop'
$src = Join-Path $PSScriptRoot $Source
if (-not (Test-Path $src)) { Write-Error "Source $src not found"; exit 1 }
$outDirPath = Join-Path $PSScriptRoot $OutDir
New-Item -ItemType Directory -Path $outDirPath -Force | Out-Null
$outDirAbs = Resolve-Path $outDirPath

$cl = Get-Command cl.exe -ErrorAction SilentlyContinue
if ($null -eq $cl) {
    Write-Output "MSVC cl.exe not found in PATH. Please use Visual Studio Developer Command Prompt or provide a compiler.";
    exit 1
}

$outExe = Join-Path $outDirAbs 'engine_stub.exe'
& cl.exe /Ox /EHsc /Fe:$outExe $src Ws2_32.lib
Write-Output "Built: $outExe"
