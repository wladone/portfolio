#!/usr/bin/env pwsh
<#
.SYNOPSIS
    E-Commerce Data Pipeline Showcase

.DESCRIPTION
    Comprehensive demonstration of the e-commerce data ingestion pipeline.
    Shows all components working together: ingestion, ETL, generation, and streaming.

.PARAMETER SkipIngestion
    Skip the data ingestion step (if data already exists)

.PARAMETER SkipStreaming
    Skip the streaming analytics step

.PARAMETER IngestionPages
    Number of pages to ingest (default: 2)

.PARAMETER GeneratorDuration
    How long to run the order generator in seconds (default: 30)

.PARAMETER GeneratorRate
    Events per second for order generator (default: 5)

.EXAMPLE
    .\run_ecommerce_showcase.ps1

.EXAMPLE
    .\run_ecommerce_showcase.ps1 -IngestionPages 3 -GeneratorDuration 60
#>

param(
    [switch]$SkipIngestion,
    [switch]$SkipStreaming,
    [int]$IngestionPages = 2,
    [int]$GeneratorDuration = 30,
    [int]$GeneratorRate = 5
)

$ErrorActionPreference = "Stop"

# Colors for output
$Green = "Green"
$Yellow = "Yellow"
$Cyan = "Cyan"
$Red = "Red"
$White = "White"

function Write-Step {
    param([string]$Message)
    Write-Host "`n==> $Message" -ForegroundColor $Cyan
    Write-Host ("=" * 60) -ForegroundColor $Cyan
}

function Write-Success {
    param([string]$Message)
    Write-Host "[OK] $Message" -ForegroundColor $Green
}

function Write-Info {
    param([string]$Message)
    Write-Host "[INFO] $Message" -ForegroundColor $Yellow
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

*** E-COMMERCE DATA PIPELINE SHOWCASE ***
$("=" * 50)

This showcase demonstrates:
* API Data Ingestion (DummyJSON)
* Spark Batch ETL (Bronze -> Silver)
* Synthetic Order Generation
* Real-time Streaming Analytics

"@ -ForegroundColor $Green

# Step 1: Compile the project
Write-Step "Compiling Project"
try {
    Write-Info "Running sbt compile..."
    & sbt compile | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Compilation failed" }
    Write-Success "Project compiled successfully"
} catch {
    Write-Error-Info "Compilation failed: $($_.Exception.Message)"
    exit 1
}

# Step 2: Data Ingestion
if (!$SkipIngestion) {
    Write-Step "Step 1: Data Ingestion from API"

    Write-Info "Ingesting $IngestionPages pages of product data from DummyJSON API..."
    Write-Info "This will create NDJSON files in data/ecommerce/raw/"

    try {
        $ingestArgs = @(
            "--source", "dummyjson",
            "--page-size", "50",
            "--max-pages", $IngestionPages.ToString(),
            "--rps", "3"
        )

        # Run ingestion - use cmd /c to avoid PowerShell exception handling
        $ingestionOutput = cmd /c "sbt `"runMain com.example.ecommerce.ingest.EcomIngestor $ingestArgs`" 2>&1"
        $ingestionSuccess = $false

        $ingestionOutput | ForEach-Object {
            $line = $_.ToString().Trim()

            if ($line -match "Ingestion completed successfully") {
                Write-Success $line
                $ingestionSuccess = $true
            } elseif ($line -match "rows,") {
                Write-Info "Ingestion stats: $line"
                $ingestionSuccess = $true
            } elseif ($line -match "Processed page") {
                Write-Info "Processing: $line"
            } elseif ($line -notmatch "SLF4J:|Generated .bloop|welcome to sbt|loading|success|Total time|set current project|running") {
                # Skip common sbt output but show actual processing messages
                if ($line -match "INFO|DEBUG|ERROR" -or $line.Length -gt 0) {
                    Write-Host $line
                }
            }
        }

        if (-not $ingestionSuccess) {
            Write-Error-Info "Ingestion did not complete successfully"
            throw "Ingestion failed"
        }

        # Verify data was created
        $rawDataPath = "data/ecommerce/raw/source=dummyjson"
        if (Test-Path $rawDataPath) {
            $files = Get-ChildItem -Path $rawDataPath -Recurse -Filter "*.ndjson" | Measure-Object
            Write-Success "Created $($files.Count) raw data files"
        } else {
            Write-Error-Info "Raw data directory not found"
        }

    } catch {
        Write-Error-Info "Data ingestion failed: $($_.Exception.Message)"
        exit 1
    }
} else {
    Write-Info "Skipping data ingestion (using existing data)"
}

# Step 3: Batch ETL Processing
Write-Step "Step 2: Spark Batch ETL Processing"

Write-Info "Processing raw data through Spark ETL pipeline..."
Write-Info "Bronze -> Silver transformation with deduplication"

try {
    # Run ETL - use cmd /c to avoid PowerShell exception handling
    $etlOutput = cmd /c "sbt `"runMain com.example.ecommerce.spark.EcomBatchJob`" 2>&1"
    $etlSuccess = $false

    $etlOutput | ForEach-Object {
        $line = $_.ToString().Trim()

        if ($line -match "Batch ETL job completed successfully") {
            Write-Success $line
            $etlSuccess = $true
        } elseif ($line -match "records after") {
            Write-Info "Processing: $line"
        } elseif ($line -match "=== Sample Analytics") {
            Write-Host "`n*** ANALYTICS RESULTS ***" -ForegroundColor $Green
        } elseif ($line -match "Top|Average|Price distribution") {
            Write-Host $line -ForegroundColor $Cyan
        } elseif ($line -notmatch "SLF4J:|Generated .bloop|welcome to sbt|loading|success|Total time|set current project|running") {
            # Skip common sbt output but show actual processing messages
            if ($line -match "INFO|DEBUG|ERROR" -or $line.Length -gt 0) {
                Write-Host $line
            }
        }
    }

    if (-not $etlSuccess) {
        Write-Error-Info "Batch ETL did not complete successfully"
        throw "Batch ETL failed"
    }

    # Verify silver data was created
    $silverDataPath = "data/ecommerce/silver/products"
    if (Test-Path $silverDataPath) {
        $parquetFiles = Get-ChildItem -Path $silverDataPath -Recurse -Filter "*.parquet" | Measure-Object
        Write-Success "Created $($parquetFiles.Count) Parquet files in silver layer"
    }

} catch {
    Write-Error-Info "Batch ETL failed: $($_.Exception.Message)"
    exit 1
}

# Step 4: Order Generation
Write-Step "Step 3: Synthetic Order Generation"

Write-Info "Generating synthetic order events for $GeneratorDuration seconds..."
Write-Info "Rate: $GeneratorRate events/second, Output: data/ecommerce/orders_incoming/"

try {
    $job = Start-Job -ScriptBlock {
        param($rate, $duration, $out)
        Set-Location $using:PWD
        & sbt "runMain com.example.ecommerce.generator.OrdersGenerator --rate $rate --duration $duration --out $out" 2>&1
    } -ArgumentList $GeneratorRate, $GeneratorDuration, "data/ecommerce/orders_incoming"

    # Show progress
    $startTime = Get-Date
    $endTime = $startTime.AddSeconds($GeneratorDuration + 2)

    while ((Get-Date) -lt $endTime) {
        $elapsed = [math]::Round(((Get-Date) - $startTime).TotalSeconds)
        $progress = [math]::Min(100, ($elapsed / $GeneratorDuration) * 100)
        Write-Progress -Activity "Generating Orders" -Status "$elapsed/$GeneratorDuration seconds" -PercentComplete $progress
        Start-Sleep -Seconds 1

        # Check if job completed
        if ($job.State -eq "Completed") { break }
    }

    Write-Progress -Activity "Generating Orders" -Completed

    # Get job results
    $results = Receive-Job -Job $job -Wait
    $results | Select-Object -Last 10 | ForEach-Object {
        if ($_ -match "Generator completed") {
            Write-Success $_.Trim()
        } elseif ($_ -match "Generated \d+ events") {
            Write-Info $_.Trim()
        }
    }

    Remove-Job -Job $job

    # Verify orders were created
    $ordersPath = "data/ecommerce/orders_incoming"
    if (Test-Path $ordersPath) {
        $orderFiles = Get-ChildItem -Path $ordersPath -Filter "*.ndjson" | Measure-Object
        Write-Success "Created $($orderFiles.Count) order data files"
    }

} catch {
    Write-Error-Info "Order generation failed: $($_.Exception.Message)"
    exit 1
}

# Step 5: Streaming Analytics (Optional)
if (!$SkipStreaming) {
    Write-Step "Step 4: Real-time Streaming Analytics"

    Write-Info "Starting streaming analytics to process order events..."
    Write-Info "This will show real-time KPIs: GMV/minute, orders/minute, top products"
    Write-Info "Press Ctrl+C to stop streaming (will run for 30 seconds)"

    try {
        $job = Start-Job -ScriptBlock {
            Set-Location $using:PWD
            & sbt "runMain com.example.ecommerce.spark.EcomOrdersStream" 2>&1
        }

        # Let it run for 30 seconds to show streaming in action
        Write-Info "Streaming for 30 seconds to demonstrate real-time processing..."
        Start-Sleep -Seconds 30

        # Stop the streaming job
        Stop-Job -Job $job -Confirm:$false
        Remove-Job -Job $job

        Write-Success "Streaming analytics demonstration completed"

        # Check if metrics were created
        $metricsPath = "data/ecommerce/metrics"
        if (Test-Path $metricsPath) {
            $metricFiles = Get-ChildItem -Path $metricsPath -Recurse -Filter "*.parquet" | Measure-Object
            if ($metricFiles.Count -gt 0) {
                Write-Success "Created $($metricFiles.Count) metrics files"
            }
        }

    } catch {
        Write-Error-Info "Streaming analytics failed: $($_.Exception.Message)"
        # Don't exit - streaming failure shouldn't stop the showcase
    }
} else {
    Write-Info "Skipping streaming analytics demonstration"
}

# Final Summary
Write-Step "Pipeline Showcase Complete!"

Write-Host @"

*** SHOWCASE SUMMARY ***

Data Pipeline Components Demonstrated:
[OK] API Data Ingestion (DummyJSON/FakeStore)
[OK] Spark Batch ETL (Bronze -> Silver)
[OK] Data Deduplication & Partitioning
[OK] Synthetic Data Generation
[OK] Real-time Streaming Analytics
[OK] Multiple Output Formats (NDJSON, Parquet)

Generated Data Locations:
[*] Raw Data: data/ecommerce/raw/
[*] Silver Data: data/ecommerce/silver/products/
[*] Orders: data/ecommerce/orders_incoming/
[*] Metrics: data/ecommerce/metrics/

Key Features Showcased:
* Rate Limiting & Retry Logic
* Configurable Data Sources
* Windows-Compatible Scripts
* Real-time KPI Computation
* Fault-Tolerant Processing

"@ -ForegroundColor $Green

Write-Success "E-Commerce Data Pipeline Showcase Completed Successfully!"
Write-Info "You can now explore the generated data and run individual components as needed."