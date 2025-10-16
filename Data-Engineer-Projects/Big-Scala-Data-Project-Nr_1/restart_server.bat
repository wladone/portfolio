@echo off
echo.
echo ============================================
echo 🔄 Restarting Dashboard Server
echo ============================================
echo.

echo Killing existing processes...
taskkill /F /IM python.exe /T >nul 2>&1
taskkill /F /IM python3.exe /T >nul 2>&1
timeout /t 2 /nobreak >nul

echo Starting fresh server...
echo.
echo Server will run at: http://localhost:8000/
echo Dashboard: http://localhost:8000/test_dashboard.html
echo.
echo Press Ctrl+C to stop the server
echo ============================================
echo.

python -m http.server 8000