param(
    [string]$Repo = '',
    [string]$Ref = 'main',
    [string]$ArtifactName = 'engine-stub-windows',
    [string]$OutputDir = '..\..\tmp_artifacts'
)

if (-not $Repo) {
    Write-Error "Repo must be provided as 'owner/repo'"
    exit 1
}

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
$out = Resolve-Path $OutputDir

if (Get-Command gh -ErrorAction SilentlyContinue) {
    $runs = gh run list --repo $Repo --branch $Ref --limit 5 --json databaseId,headSha,conclusion
    $runsObj = $runs | ConvertFrom-Json
    if ($runsObj -and $runsObj.Count -gt 0) {
        $runId = $runsObj[0].databaseId
        gh run download $runId --repo $Repo --name $ArtifactName --dir $out
    } else {
        Write-Error "No recent runs found for $Repo@$Ref"
        exit 1
    }
} else {
    if (-not $env:GITHUB_TOKEN) { Write-Error "Set GITHUB_TOKEN env var to use API fallback"; exit 1 }
    $token = $env:GITHUB_TOKEN
    $api = "https://api.github.com/repos/$Repo/actions/runs?branch=$Ref&per_page=5"
    $runs = Invoke-RestMethod -Headers @{ Authorization = "token $token" } -Uri $api
    if ($runs.workflow_runs.Count -eq 0) { Write-Error "No runs found"; exit 1 }
    $runId = $runs.workflow_runs[0].id
    $artApi = "https://api.github.com/repos/$Repo/actions/runs/$runId/artifacts"
    $arts = Invoke-RestMethod -Headers @{ Authorization = "token $token" } -Uri $artApi
    $art = $arts.artifacts | Where-Object { $_.name -eq $ArtifactName }
    if (-not $art) { Write-Error "Artifact $ArtifactName not found"; exit 1 }
    $zipUrl = $art.archive_download_url
    Invoke-RestMethod -Headers @{ Authorization = "token $token" } -Uri $zipUrl -OutFile (Join-Path $out ($ArtifactName + '.zip'))
    Expand-Archive -Path (Join-Path $out ($ArtifactName + '.zip')) -DestinationPath $out -Force
}

$exe = Get-ChildItem -Path $out -Recurse -Filter 'engine_stub.exe' | Select-Object -First 1
if (-not $exe) { Write-Error "engine_stub.exe not found in artifact"; exit 1 }

Write-Output "Found engine: $($exe.FullName)"

Start-Process -NoNewWindow -FilePath $exe.FullName -WorkingDirectory $exe.DirectoryName
Start-Sleep -Seconds 1
Start-Process -NoNewWindow -FilePath python -ArgumentList '..\ui\banner.py'
