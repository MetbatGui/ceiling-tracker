#!/bin/sh
# 데일리 파이프라인. daily-update가 수집 + 백필 + 엑셀 리포트 생성 +
# 산출물/DB Drive 업로드까지 전부 소유한다(orchestration_guide.md §1/§3) -
# 예전엔 export-excel을 이 스크립트가 별도 2단계로 이어붙였지만, 그러면
# "사람이 잊지 않고 실행해야 얻어지는 부수 단계"가 되어 데일리 파이프라인이
# 아닌 경로(예: 수동 재실행)에서 리포트 생성이 누락될 위험이 있었다.
set -e
cd /app

PYTHON=/app/.venv/bin/python

echo "=== 데일리 파이프라인 (daily-update: 수집 + 리포트 생성 + 업로드) ==="
"$PYTHON" src/cli.py daily-update
