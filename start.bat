@echo off
rem Double-click to start the Money Flow Map (opens the dashboard). Close this window to stop it.
rem Options are passed through, e.g.  start.bat -NoDashboard   or   start.bat -Port 8800
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start.ps1" %*
if errorlevel 1 pause
