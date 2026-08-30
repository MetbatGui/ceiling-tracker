#!/bin/sh
# cron 데몬 기동에 root 권한이 필요해 이 컨테이너만 root로 띄운다
# (docker-compose.yml의 ceiling-tracker-cron 서비스 참고).
#
# krx-auto-crawling과 달리 여기서는 OS 환경변수를 파일로 덤프해 잡에
# 다시 주입하는 과정이 필요 없다 - cli.py가 python-dotenv로 /app/.env를
# 직접 읽으므로(OS 환경변수 상속 여부와 무관) run-daily.sh가 /app에서
# 실행되기만 하면 .env가 자동으로 로드된다.
set -e

# 볼륨 마운트(db/, data/, secrets/token.json)는 호스트 소유권을 그대로
# 가져오므로, 호스트 uid가 999(nonroot)와 다르면 su로 낮춘 페이로드
# (run-daily.sh)가 쓰기 실패할 수 있다. 컨테이너 시작 시 한 번 nonroot 소유로
# 맞춰둔다 (docker_guide.md §3.1). client_secret.json은 :ro로 마운트돼 있어
# chown 대상에서 뺀다 - 읽기 전용 마운트에 chown하면 실패해 set -e로 컨테이너가
# 죽는다.
chown -R nonroot:nonroot /app/db /app/data /app/secrets/token.json

exec cron -f
