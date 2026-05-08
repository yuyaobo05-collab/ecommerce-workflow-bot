@echo off
setlocal
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0restart.ps1"
pause
