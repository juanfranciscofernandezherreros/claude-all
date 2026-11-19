@echo off
setlocal
set "MAVEN_VERSION=3.9.9"
if "%MAVEN_USER_HOME%"=="" (set "WRAPPER_HOME=%USERPROFILE%\.m2\wrapper\dists\apache-maven-%MAVEN_VERSION%") else (set "WRAPPER_HOME=%MAVEN_USER_HOME%\wrapper\dists\apache-maven-%MAVEN_VERSION%")
set "MAVEN_BIN=%WRAPPER_HOME%\apache-maven-%MAVEN_VERSION%\bin\mvn.cmd"
if not exist "%MAVEN_BIN%" (
  if not exist "%WRAPPER_HOME%" mkdir "%WRAPPER_HOME%"
  powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='Stop'; $u='https://archive.apache.org/dist/maven/maven-3/%MAVEN_VERSION%/binaries/apache-maven-%MAVEN_VERSION%-bin.zip'; Invoke-WebRequest -Uri $u -OutFile '%WRAPPER_HOME%\maven.zip'; Expand-Archive -Force '%WRAPPER_HOME%\maven.zip' '%WRAPPER_HOME%'; Remove-Item '%WRAPPER_HOME%\maven.zip'"
  if errorlevel 1 exit /b 1
)
call "%MAVEN_BIN%" -f "%~dp0pom.xml" %*
exit /b %ERRORLEVEL%
