# PowerShell script to set up and run the Global Electronics Lakehouse locally
# This script generates sample data, ingests it into Delta tables, starts the dashboard, and opens it in the browser

Set-Location $PSScriptRoot

Write-Host "Generating synthetic data..." -ForegroundColor Green
& python src/generate_synthetic_data.py

Write-Host "Ingesting data into Delta tables..." -ForegroundColor Green
## Ensure Hadoop native helpers (winutils) are available on Windows
if ($IsWindows) {
    # If HADOOP_HOME is not set, default to a local 'hadoop' folder inside the project
    if (-not $env:HADOOP_HOME -or $env:HADOOP_HOME -eq '') {
        $defaultHadoop = Join-Path $PSScriptRoot 'hadoop'
        Write-Host "HADOOP_HOME is not set. Defaulting to: $defaultHadoop" -ForegroundColor Yellow
        Write-Host "If you don't have winutils.exe, download a matching winutils build and place it in $defaultHadoop\bin\winutils.exe" -ForegroundColor Yellow
        $env:HADOOP_HOME = $defaultHadoop
    }

    $winutilsPath = Join-Path $env:HADOOP_HOME 'bin\winutils.exe'
    if (Test-Path $winutilsPath) {
        # Prepend HADOOP bin to PATH for the child processes
        $env:Path = "$($env:HADOOP_HOME)\bin;$env:Path"
        Write-Host "Found winutils.exe at $winutilsPath. HADOOP_HOME and PATH updated for this session." -ForegroundColor Green
    } else {
        Write-Host "WARNING: winutils.exe not found at $winutilsPath." -ForegroundColor Yellow
        Write-Host "Spark on Windows may throw UnsatisfiedLinkError or permission-related errors without it." -ForegroundColor Yellow

        function Get-Detected-HadoopMajorVersion {
            # Try to detect Hadoop major version from SPARK_HOME or common spark install folders
            if ($env:SPARK_HOME -and (Test-Path $env:SPARK_HOME)) {
                $name = Split-Path $env:SPARK_HOME -Leaf
                if ($name -match 'hadoop(\d)') { return $Matches[1] }
            }
            # Look for common C:\spark installs
            $sparkRoot = 'C:\spark'
            if (Test-Path $sparkRoot) {
                Get-ChildItem -Path $sparkRoot -Directory -ErrorAction SilentlyContinue | ForEach-Object {
                    if ($_.Name -match 'hadoop(\d)') { return $Matches[1] }
                }
            }
            return $null
        }

        $detected = Get-Detected-HadoopMajorVersion
        if ($detected) {
            Write-Host "Detected Spark was built for Hadoop $detected.x (detected major: $detected)." -ForegroundColor Cyan
        } else {
            Write-Host "Could not automatically detect Spark/Hadoop version. If you know the Hadoop major version (2 or 3) enter it when prompted." -ForegroundColor Cyan
        }

        Write-Host "I can help you get a winutils.exe. You have two safe options:" -ForegroundColor Yellow
        Write-Host "  1) Open a recommended GitHub page where you can manually download a matching winutils.exe." -ForegroundColor Yellow
        Write-Host "  2) Provide a direct URL to a winutils.exe (raw binary) and I'll download it to $($env:HADOOP_HOME)\bin\winutils.exe for you." -ForegroundColor Yellow
        $choice = Read-Host "Type 'open' to open GitHub, 'dl' to provide a direct download URL, or Enter to skip"

        if ($choice -eq 'open') {
            # Suggest pages based on detected major version
            if ($detected -eq '3') {
                Start-Process 'https://github.com/kontext-tech/winutils/tree/master/hadoop-3.3.1/bin'
            } elseif ($detected -eq '2') {
                Start-Process 'https://github.com/kontext-tech/winutils/tree/master/hadoop-2.7.1/bin'
            } else {
                Start-Process 'https://github.com/search?q=winutils+hadoop'
            }
            Write-Host "Opened browser to a repository listing; download winutils.exe matching your Hadoop version, place it under $($env:HADOOP_HOME)\bin\ and re-run this script." -ForegroundColor Green
            Read-Host "Press Enter to continue (script will attempt to continue without winutils)" | Out-Null
        } elseif ($choice -eq 'dl') {
            $dlUrl = Read-Host "Paste the direct URL to winutils.exe (raw binary). Example: https://raw.githubusercontent.com/<user>/<repo>/.../winutils.exe"
            if ($dlUrl -and $dlUrl -ne '') {
                try {
                    New-Item -ItemType Directory -Path (Join-Path $env:HADOOP_HOME 'bin') -Force | Out-Null
                    $target = $winutilsPath
                    Write-Host "Downloading winutils.exe to $target ..." -ForegroundColor Cyan
                    Invoke-WebRequest -Uri $dlUrl -OutFile $target -UseBasicParsing -ErrorAction Stop
                    if (Test-Path $target) {
                        Write-Host "Downloaded winutils.exe successfully." -ForegroundColor Green
                        $env:Path = "$($env:HADOOP_HOME)\bin;$env:Path"
                    } else {
                        Write-Host "Download finished but file not found at $target." -ForegroundColor Red
                    }
                } catch {
                    Write-Host "Download failed: $_" -ForegroundColor Red
                    Write-Host "You can manually download winutils.exe and put it under $($env:HADOOP_HOME)\bin\winutils.exe" -ForegroundColor Yellow
                }
            } else {
                Write-Host "No URL provided; skipping download." -ForegroundColor Yellow
            }
        } else {
            Write-Host "Skipping winutils helper. You can still set up winutils manually later." -ForegroundColor Yellow
        }

        # Offer to set HADOOP_HOME permanently
        $setPersist = Read-Host "Would you like to set HADOOP_HOME permanently for your account (setx)? Type Y to apply, otherwise press Enter"
        if ($setPersist -and ($setPersist.ToUpper() -eq 'Y')) {
            try {
                Write-Host "Setting HADOOP_HOME permanently (setx) to $($env:HADOOP_HOME). This requires opening a new shell to take effect." -ForegroundColor Cyan
                setx HADOOP_HOME "$($env:HADOOP_HOME)" | Out-Null
                Write-Host "HADOOP_HOME set permanently for your user. Open a new PowerShell window to pick it up." -ForegroundColor Green
            } catch {
                Write-Host "Failed to set HADOOP_HOME permanently: $_" -ForegroundColor Red
            }
        }
    }
}

# Run the ingest script (keeps environment variables set for child process)
& python notebooks_local/1_local_ingest_delta.py

Write-Host "Starting Streamlit dashboard..." -ForegroundColor Green
# Start the dashboard in a background job so the script can continue
$dashboardJob = Start-Job -ScriptBlock {
    Set-Location $using:PSScriptRoot
    & python run.py
}

# Wait a few seconds for the dashboard to start
Start-Sleep -Seconds 5

# Open the dashboard in the default browser
Write-Host "Opening dashboard in browser..." -ForegroundColor Green
Start-Process "http://localhost:8501"

Write-Host "Dashboard is running. Press Ctrl+C to stop." -ForegroundColor Yellow

# Keep the script running to show status
try {
    while ($true) {
        Start-Sleep -Seconds 1
    }
} finally {
    # Clean up the background job when the script is interrupted
    Stop-Job $dashboardJob
    Remove-Job $dashboardJob
}