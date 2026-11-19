@echo off
setlocal
cd /d "%~dp0.."
set "BASH="
where bash >nul 2>&1
if not errorlevel 1 set "BASH=bash"
if not defined BASH if exist "%ProgramFiles%\Git\bin\bash.exe" set "BASH=%ProgramFiles%\Git\bin\bash.exe"
if not defined BASH (
  echo ERROR: Git Bash is required. Install Git for Windows.
  exit /b 1
)
%BASH% "%~dp0run-full-suite.sh" %*
exit /b %ERRORLEVEL%
