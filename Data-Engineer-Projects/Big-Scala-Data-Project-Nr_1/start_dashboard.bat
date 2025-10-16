@echo off
echo.
echo ============================================
echo 🛒 E-Commerce Analytics Dashboard
echo ============================================
echo.
echo Starting local web server...
echo Dashboard will open at: http://localhost:8000/
echo.
echo Press Ctrl+C to stop the server
echo ============================================
echo.

REM Try python first, then python3
python -m http.server 8000 >nul 2>&1
if %errorlevel% neq 0 (
    echo Python not found, trying python3...
    python3 -m http.server 8000 >nul 2>&1
    if %errorlevel% neq 0 (
        echo ERROR: Python is not installed or not in PATH
        echo Please install Python and try again.
        pause
        exit /b 1
    )
)

pause