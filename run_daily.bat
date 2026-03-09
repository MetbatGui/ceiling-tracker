@echo off
chcp 65001 > nul
cd /d "%~dp0"

echo === [1/2] 데이터 수집 (daily-update) ===
uv run python src/cli.py daily-update
if errorlevel 1 (
    echo.
    echo [오류] 데이터 수집 실패. 리포트 생성을 건너뜁니다.
    pause
    exit /b 1
)

echo.
echo === [2/2] 리포트 생성 (export-excel) ===
set YEAR=%date:~0,4%
uv run python src/cli.py export-excel --year %YEAR% --drive
if errorlevel 1 (
    echo [오류] 리포트 생성 실패.
    pause
    exit /b 1
)
