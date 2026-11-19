@echo off
setlocal
title Kafka Performance Lab - Claude Code
cd /d "%~dp0"

echo ==================================================
echo   KAFKA PERFORMANCE LAB - AUTONOMOUS EXECUTION
echo ==================================================
echo.

where git >nul 2>&1
if errorlevel 1 (
  echo [ERROR] Git is not installed or not in PATH.
  echo Install Git for Windows and run again.
  pause
  exit /b 1
)

where claude >nul 2>&1
if errorlevel 1 (
  echo [ERROR] Claude Code CLI not found in PATH.
  echo Install and authenticate Claude Code, then run again.
  pause
  exit /b 1
)

if not exist ".git" (
  echo [INFO] Cloning repository...
  cd /d "%TEMP%"
  if not exist "claude-all" (
    git clone https://github.com/juanfranciscofernandezherreros/claude-all.git
    if errorlevel 1 (
      echo [ERROR] Clone failed.
      pause
      exit /b 1
    )
  ) else (
    cd /d "%TEMP%\claude-all"
    git pull
  )
  cd /d "%TEMP%\claude-all"
)

if not exist "PRD.md" (
  echo [ERROR] PRD.md not found.
  pause
  exit /b 1
)

where docker >nul 2>&1
if errorlevel 1 (
  echo [WARNING] Docker CLI not found. Claude will inspect this blocker.
) else (
  docker info >nul 2>&1
  if errorlevel 1 echo [WARNING] Docker Engine is not running. Start Docker Desktop.
)

echo [INFO] Launching Claude Code. Review permissions implications before use.
echo.
claude --dangerously-skip-permissions "Read PRD.md and CLAUDE.md completely. Execute the full Kafka Performance Lab autonomously. Ask no questions. Inspect environment, implement all files, start local Kafka KRaft using Docker Compose, wait for broker readiness, create topics, build and test Java, generate and process 1,000,000 messages, validate output integrity, run benchmarks, optimize, rerun tests and save truthful reports. Never invent results. If an external permission or dependency blocks execution, document the exact blocker and continue independent work."

set "RC=%ERRORLEVEL%"
echo.
if "%RC%"=="0" (
  echo [SUCCESS] Claude Code exited successfully.
) else (
  echo [WARNING] Claude Code exit code: %RC%. Check output and benchmark reports.
)
pause
exit /b %RC%
