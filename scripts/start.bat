@echo off
setlocal
cd /d "%~dp0.."
if not exist "benchmark-results" mkdir "benchmark-results"
icacls "benchmark-results" /grant "%USERDOMAIN%\%USERNAME%:(OI)(CI)M" /T >nul 2>&1
docker compose up -d --build
if errorlevel 1 exit /b %ERRORLEVEL%
docker compose ps
