@echo off
setlocal
cd /d "%~dp0.."
docker compose up -d --build
if errorlevel 1 exit /b %ERRORLEVEL%
docker compose ps
