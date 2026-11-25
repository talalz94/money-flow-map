@echo off
rem Double-click to start the Money Flow Map (opens the dashboard). Close this window to stop it;
rem it also stops by itself 5 minutes after the last dashboard tab closes (-StayRunning: never).
rem Options are passed through, e.g.  start.bat -NoDashboard   or   start.bat -Port 8800
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start.ps1" %*
if errorlevel 1 pause
