@echo off
setlocal
title Kafka Performance Lab
cd /d "%~dp0"
if not exist "compose.yaml" (
  echo ERROR: compose.yaml not found. Run this launcher from the repository.
  pause
  exit /b 1
)
where docker >nul 2>&1 || (echo ERROR: Install Docker Desktop with Docker Compose v2.& pause& exit /b 1)
docker info >nul 2>&1
if errorlevel 1 (
  if exist "%ProgramFiles%\Docker\Docker\Docker Desktop.exe" (
    echo Starting Docker Desktop...
    start "" "%ProgramFiles%\Docker\Docker\Docker Desktop.exe"
  ) else (
    echo ERROR: Docker Engine is stopped and Docker Desktop was not found.
    pause
    exit /b 1
  )
  for /l %%i in (1,1,60) do (
    docker info >nul 2>&1
    if not errorlevel 1 goto docker_ready
    timeout /t 2 /nobreak >nul
  )
  echo ERROR: Docker did not become ready within 120 seconds.
  pause
  exit /b 1
)
:docker_ready
if not exist "benchmark-results" mkdir "benchmark-results"
icacls "benchmark-results" /grant "*S-1-1-0:(OI)(CI)M" /T >nul 2>&1
echo Starting Kafka, the application, Prometheus and Grafana...
docker compose up -d --build
if errorlevel 1 goto failed
echo.
echo Starting the 1,000,000-message benchmark.
call "scripts\run-benchmark.bat" 1000000
set "RC=%ERRORLEVEL%"
echo.
if "%RC%"=="0" (
  echo Benchmark finished. Reports are in benchmark-results.
) else (
  echo Benchmark failed or was blocked. Review benchmark-results and Docker logs.
)
pause
exit /b %RC%
:failed
echo ERROR: Docker Compose could not start the stack.
pause
exit /b 1
