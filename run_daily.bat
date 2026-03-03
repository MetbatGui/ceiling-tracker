@echo off
cd %~dp0
echo Starting Ceiling Tracker Daily Job...
uv run src/cli.py daily-update --drive
pause
