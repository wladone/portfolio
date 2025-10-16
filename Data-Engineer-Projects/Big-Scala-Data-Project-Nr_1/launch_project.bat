@echo off
echo.
echo ============================================
echo 🚀 E-COMMERCE PROJECT LAUNCHER
echo ============================================
echo.
echo This launcher provides multiple options:
echo.
echo 1. Full Pipeline Demo (run_ecommerce_showcase.ps1)
echo 2. Dashboard Only (start_dashboard.ps1)
echo 3. Manual Server Start (restart_server.bat)
echo.
echo ============================================
echo.

:menu
echo Choose an option:
echo.
echo [1] Run Full Pipeline Demo (Data Processing + Dashboard)
echo [2] Start Dashboard Only (View Results)
echo [3] Restart Server (Fix Port Issues)
echo [4] Exit
echo.
set /p choice="Enter your choice (1-4): "

if "%choice%"=="1" goto full_demo
if "%choice%"=="2" goto dashboard_only
if "%choice%"=="3" goto restart_server
if "%choice%"=="4" goto exit

echo Invalid choice. Please try again.
goto menu

:full_demo
echo.
echo ============================================
echo 📊 FULL PIPELINE DEMO
echo ============================================
echo.
echo This will run the complete data pipeline:
echo • API Data Ingestion
echo • Spark Batch ETL Processing
echo • Synthetic Order Generation
echo • Real-time Streaming Analytics
echo.
powershell -Command "& '.\scripts\run_ecommerce_showcase.ps1'"
goto end

:dashboard_only
echo.
echo ============================================
echo 🖥️ DASHBOARD ONLY
echo ============================================
echo.
echo This will start the dashboard server:
echo • Python HTTP Server on port 8000
echo • Auto-open browser to dashboard
echo • Serve all project files
echo.
powershell -Command "& '.\start_dashboard.ps1'"
goto end

:restart_server
echo.
echo ============================================
echo 🔄 SERVER RESTART
echo ============================================
echo.
echo This will fix port conflicts and restart:
echo • Kill existing Python servers
echo • Start fresh HTTP server
echo • Auto-open dashboard
echo.
call restart_server.bat
goto end

:exit
echo.
echo Goodbye!
echo.

:end