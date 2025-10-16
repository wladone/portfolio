# Start E-Commerce Analytics Dashboard
# This script starts the local web server and opens the dashboard in your default browser

# Colors for output
$Green = "Green"
$Yellow = "Yellow"
$Cyan = "Cyan"
$Red = "Red"

function Write-Step {
    param([string]$Message)
    Write-Host "`n==> $Message" -ForegroundColor $Cyan
    Write-Host ("=" * 60) -ForegroundColor $Cyan
}

function Write-Success {
    param([string]$Message)
    Write-Host "[OK] $Message" -ForegroundColor $Green
}

function Write-Error-Info {
    param([string]$Message)
    Write-Host "[ERROR] $Message" -ForegroundColor $Red
}

# Check if we're in the project root
if (!(Test-Path "build.sbt")) {
    Write-Error-Info "Please run this script from the project root directory"
    exit 1
}

Write-Host @"

*** E-COMMERCE DASHBOARD LAUNCHER ***
$( "=" * 50 )

This launcher will:
* Start Python HTTP server on port 8000
* Serve all project files (HTML, CSS, JS, data)
* Auto-open dashboard in your browser
* Display real-time server logs

"@ -ForegroundColor $Green

Write-Step "Starting Dashboard Server"

# Kill any existing Python servers
Write-Host "Cleaning up existing servers..." -ForegroundColor $Yellow
taskkill /F /IM python.exe /T 2>$null | Out-Null
taskkill /F /IM python3.exe /T 2>$null | Out-Null

# Wait a moment
Start-Sleep -Seconds 1

# Check if Python is available
$pythonCmd = ""
try {
    $pythonVersion = & python --version 2>$null
    if ($LASTEXITCODE -eq 0) {
        $pythonCmd = "python"
        Write-Success "Found Python: $pythonVersion"
    }
} catch {
    try {
        $pythonVersion = & python3 --version 2>$null
        if ($LASTEXITCODE -eq 0) {
            $pythonCmd = "python3"
            Write-Success "Found Python3: $pythonVersion"
        }
    } catch {
        Write-Error-Info "Python not found. Please install Python 3.x"
        exit 1
    }
}

# Verify dashboard files exist
$dashboardFile = "test_dashboard.html"
if (!(Test-Path $dashboardFile)) {
    Write-Error-Info "Dashboard file not found: $dashboardFile"
    Write-Host "Make sure you're in the project root directory" -ForegroundColor $Yellow
    exit 1
}

Write-Success "Dashboard file found: $dashboardFile"

# Start the server
Write-Host "Starting HTTP server on port 8000..." -ForegroundColor $Yellow

try {
    # Start server in background job
    $serverJob = Start-Job -ScriptBlock {
        param($pythonCmd)
        Set-Location $using:PWD
        & $pythonCmd -m http.server 8000
    } -ArgumentList $pythonCmd

    # Wait for server to start
    Write-Host "Waiting for server to initialize..." -ForegroundColor $Yellow
    Start-Sleep -Seconds 3

    # Test if server is responding
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8000/" -TimeoutSec 5 -ErrorAction SilentlyContinue
        if ($response.StatusCode -eq 200) {
            Write-Success "Server is responding correctly"
        }
    } catch {
        Write-Host "Server may still be starting up..." -ForegroundColor $Yellow
    }

    # Open browser
    Write-Host "Opening dashboard in browser..." -ForegroundColor $Green
    Start-Process "http://localhost:8000/"

    Write-Host ""
    Write-Host "==========================================" -ForegroundColor $Cyan
    Write-Host "DASHBOARD IS NOW RUNNING!" -ForegroundColor $Green
    Write-Host "==========================================" -ForegroundColor $Cyan
    Write-Host ""
    Write-Host "Access URLs:" -ForegroundColor $Cyan
    Write-Host "   Main:     http://localhost:8000/" -ForegroundColor $Green
    Write-Host "   Dashboard: http://localhost:8000/test_dashboard.html" -ForegroundColor $Green
    Write-Host ""
    Write-Host "Features Available:" -ForegroundColor $Cyan
    Write-Host "   - Modern glassmorphism design" -ForegroundColor $Green
    Write-Host "   - Dark mode toggle" -ForegroundColor $Green
    Write-Host "   - Interactive charts and visualizations" -ForegroundColor $Green
    Write-Host "   - Performance metrics dashboard" -ForegroundColor $Green
    Write-Host ""
    Write-Host "To stop the server:" -ForegroundColor $Yellow
    Write-Host "   Press Ctrl+C in this terminal" -ForegroundColor $Yellow
    Write-Host "   Or close this PowerShell window" -ForegroundColor $Yellow
    Write-Host ""
    Write-Host "==========================================" -ForegroundColor $Cyan

    # Wait for the server job and display output
    Receive-Job -Job $serverJob -Wait

} catch {
    Write-Error-Info "Failed to start server: $($_.Exception.Message)"

    # Clean up
    if ($serverJob) {
        Stop-Job -Job $serverJob -ErrorAction SilentlyContinue
        Remove-Job -Job $serverJob -ErrorAction SilentlyContinue
    }

    exit 1
}