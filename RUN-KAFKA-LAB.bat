@echo off
setlocal
title Kafka Performance Lab
cd /d "%~dp0"
if not exist "PRD.md" (
  echo ERROR: Run this file from the cloned claude-all repository.
  pause
  exit /b 1
)
where git >nul 2>&1 || (echo ERROR: Install Git for Windows.& pause& exit /b 1)
where claude >nul 2>&1 || (echo ERROR: Install and authenticate Claude Code CLI.& pause& exit /b 1)
where docker >nul 2>&1 || (echo ERROR: Install Docker Desktop.& pause& exit /b 1)

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
echo Running the complete project autonomously.
echo WARNING: --dangerously-skip-permissions grants Claude broad access.
echo Run only in this trusted repository and review changes afterwards.
claude --dangerously-skip-permissions "Read PRD.md and CLAUDE.md. Execute the full specification. Minimize narration and token use. Implement, build, test, start local Kafka with Docker Compose, run the 1M benchmark, independently validate outputs, save truthful reports, and fix failures. Ask no questions. Never fabricate results."
set "RC=%ERRORLEVEL%"
echo.
if "%RC%"=="0" (echo Claude finished.) else (echo Claude exit code: %RC%.)
pause
exit /b %RC%
