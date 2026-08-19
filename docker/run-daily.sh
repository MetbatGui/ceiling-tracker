#!/bin/sh
# run_daily.bat을 그대로 이식한 데일리 파이프라인.
#   1. daily-update (당일 상한가 수집 + 백필)
#   2. export-excel --drive (엑셀 리포트 생성 + db/{year}.db Drive 백업)
# set -e: daily-update가 실패(exit != 0)하면 export-excel을 건너뛴다.
set -e
cd /app

PYTHON=/app/.venv/bin/python
YEAR=$(date +%Y)

echo "=== [1/2] 데이터 수집 (daily-update) ==="
"$PYTHON" src/cli.py daily-update

echo "=== [2/2] 리포트 생성 (export-excel) ==="
"$PYTHON" src/cli.py export-excel --year "$YEAR" --drive
