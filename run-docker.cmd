@echo off
set "PROJECT_DIR=%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%PROJECT_DIR%docker.ps1" start
if errorlevel 1 (
  echo.
  echo Docker startup failed. See the message above or run docker.ps1 status.
  pause
)
