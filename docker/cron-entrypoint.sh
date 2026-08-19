#!/bin/sh
# cron 데몬 기동에 root 권한이 필요해 이 컨테이너만 root로 띄운다
# (docker-compose.yml의 ceiling-tracker-cron 서비스 참고).
#
# krx-auto-crawling과 달리 여기서는 OS 환경변수를 파일로 덤프해 잡에
# 다시 주입하는 과정이 필요 없다 - cli.py가 python-dotenv로 /app/.env를
# 직접 읽으므로(OS 환경변수 상속 여부와 무관) run-daily.sh가 /app에서
# 실행되기만 하면 .env가 자동으로 로드된다.
set -e
exec cron -f
