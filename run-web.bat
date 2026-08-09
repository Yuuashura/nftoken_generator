@echo off
REM Launcher web NFToken generator. Double-click to run.
cd /d "%~dp0"

python --version >nul 2>&1
if errorlevel 1 (
    echo Python tidak ditemukan di PATH. Install Python dulu: https://python.org
    pause
    exit /b 1
)

REM Pastikan flask ada; kalau belum, install dari requirements.
python -c "import flask" >nul 2>&1
if errorlevel 1 (
    echo Menginstall dependency...
    python -m pip install -r requirements.txt
    if errorlevel 1 (
        echo Gagal install dependency.
        pause
        exit /b 1
    )
)

python nf-token-web.py %*
pause
