# uv 공식 이미지 (python3.12 - pyproject.toml requires-python과 일치)
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# non-root 유저 (기본 실행용. cron 데몬 기동에는 docker-compose에서 root로 오버라이드)
RUN groupadd --system --gid 999 nonroot \
    && useradd --system --gid 999 --uid 999 --create-home nonroot

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# 의존성만 먼저 설치 (레이어 캐시 활용)
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

ENV PATH="/app/.venv/bin:$PATH"

COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# 컨테이너 내장 cron (호스트에 별도 스케줄러가 없는 서버 배포용)
RUN apt-get update \
    && apt-get install -y --no-install-recommends cron tzdata \
    && rm -rf /var/lib/apt/lists/*
ENV TZ=Asia/Seoul
# ENV TZ는 cron 데몬(crond) 자체가 crontab 시각을 해석하는 기준에는 반영되지 않음 -
# crond는 /etc/localtime을 봄. 이걸 안 하면 crontab에 적어둔 시각이 KST가 아니라
# UTC로 해석돼 실제 발화 시각이 9시간 어긋날 수 있음.
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY docker/crontab /etc/cron.d/ceiling-tracker-cron
RUN chmod 0644 /etc/cron.d/ceiling-tracker-cron \
    && chmod +x /app/docker/run-daily.sh /app/docker/cron-entrypoint.sh

RUN chown -R nonroot:nonroot /app

USER nonroot

# 절대경로 python 사용 (cron 잡은 ENV PATH를 상속받지 않음)
ENTRYPOINT ["/app/.venv/bin/python", "/app/src/cli.py"]
CMD ["--help"]
