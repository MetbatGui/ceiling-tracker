#!/bin/sh
# 데일리 파이프라인. daily-update가 수집 + 백필 + 엑셀 리포트 생성 +
# 산출물/DB Drive 업로드까지 전부 소유한다(orchestration_guide.md §1/§3) -
# 예전엔 export-excel을 이 스크립트가 별도 2단계로 이어붙였지만, 그러면
# "사람이 잊지 않고 실행해야 얻어지는 부수 단계"가 되어 데일리 파이프라인이
# 아닌 경로(예: 수동 재실행)에서 리포트 생성이 누락될 위험이 있었다.
# crontab이 이 스크립트 자체는 root로 돌린다(/proc/1/fd/1 리다이렉션이 root
# 전용 권한이라 - crontab 주석 참고). 하지만 실제 수집/파싱/Drive 업로드
# 페이로드까지 root로 돌 이유는 없으므로, su로 nonroot로 낮춰서 실행한다
# (docker_guide.md §3 - root는 cron 데몬 기동/fd 리다이렉션에만, 페이로드는
# nonroot로). 리다이렉션은 부모(root) 셸이 이미 열어놓은 fd를 su의 자식
# 프로세스가 그대로 물려받으므로(open 시점에만 권한 체크) nonroot로 내려도
# 로그는 정상적으로 계속 써진다.
set -e
cd /app

echo "=== 데일리 파이프라인 (daily-update: 수집 + 리포트 생성 + 업로드) ==="
exec su -s /bin/sh -c '/app/.venv/bin/python /app/src/cli.py daily-update' nonroot
