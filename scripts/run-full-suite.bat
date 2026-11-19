@echo off
setlocal
cd /d "%~dp0.."
where bash >nul 2>&1
if not errorlevel 1 (bash "%~dp0run-full-suite.sh" %* & exit /b %ERRORLEVEL%)
if exist "%ProgramFiles%\Git\bin\bash.exe" ("%ProgramFiles%\Git\bin\bash.exe" "%~dp0run-full-suite.sh" %* & exit /b %ERRORLEVEL%)
echo ERROR: Git Bash is required. Install Git for Windows.
exit /b 1
