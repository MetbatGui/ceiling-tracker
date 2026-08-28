# 🚀 Ceiling Tracker

KRX(한국거래소)의 국내 상장 전종목을 매일 스캔해 **상한가(가격제한폭 상단 도달)** 종목을
추적하고, 코호트(상한가 진입 후 N거래일 추이)를 SQLite에 쌓아 연도별/기간별 엑셀
리포트로 만드는 배치 프로그램입니다. 생성된 리포트는 구글 드라이브에 자동 업로드됩니다.

이 문서는 다음 개발자 또는 시스템 관리자가 프로젝트를 빠르고 정확하게 파악하고 인수인계받을
수 있도록 작성되었습니다.

---

## ✨ 주요 기능

- **일일 자동 추적** (`daily-update`): 당일 상한가 종목을 KRX에서 수집하고, 휴장일 사이에
  빠진 구간이 있으면 자동으로 백필합니다. 컨테이너 내장 cron이 평일 15:55 KST에 자동 실행합니다.
- **기간/연도 백필** (`range-update`, `annual-update`): 임의 기간 또는 연도 범위를 한 번에
  수집합니다.
- **엑셀 리포트 생성** (`export-excel`): SQLite에 쌓인 데이터를 연도/기간 단위로 조회해
  엑셀로 렌더링합니다(`--drive`로 구글 드라이브 업로드까지).
- **구글 드라이브 동기화**: 엑셀 산출물과 `db/{year}.db`(SQLite SSOT)를 함께 업로드합니다.

---

## 🏗 아키텍처

포트-어댑터(Hexagonal Architecture) 구조로, 비즈니스 로직이 외부 인프라(KRX, SQLite,
구글 드라이브)에 직접 의존하지 않습니다.

```
ceiling-tracker/
├── docker/              # Docker 환경 구축 파일 (Dockerfile, docker-compose, cron 스크립트)
├── secrets/             # 인증 자격 증명 키 저장소 (Git 제외 대상)
│   ├── client_secret.json     # Google OAuth 클라이언트 보안 비밀
│   └── token.json              # 최초 실행 시 생성되는 OAuth 토큰
├── db/                  # SQLite SSOT DB, 연도별 {year}.db 분리 (Git 제외 대상)
├── data/                # 로컬 저장소용 엑셀 산출물 + 로그 (Git 제외 대상)
├── src/
│   ├── domain/           # 순수 도메인 모델 (model.py, ports.py, constants.py)
│   ├── application/      # DailyRoutineService, RangeRoutineService, ExcelExportService 등
│   ├── infrastructure/   # KrxDirectStockInfoAdapter, SqliteCohortRepository,
│   │                     # LocalStorageAdapter/GoogleDriveAdapter, ExcelRenderer, CalendarService
│   └── cli.py            # CLI 진입점 (daily-update / range-update / annual-update / export-excel)
└── tests/
```

- `DailyRoutineService`가 휴장일 판정(`CalendarService`) + 수집(`DailyUpdateService`) +
  백필을 오케스트레이션합니다.
- `SqliteCohortRepository`가 SSOT이며, `db/{year}.db`로 연도별 분리 저장합니다.
- 저장소는 `LocalStorageAdapter`/`GoogleDriveAdapter` 두 구현체가 있고, `export-excel`에
  `--drive`를 주면 Drive에 저장 후 로컬에도 자동 백업합니다(`_dual_save_workbook`).
- DB 업로드 대상은 리포트가 조회한 날짜 범위가 아니라 **로컬에 실제 존재하는 `db/*.db` 파일
  전체**입니다 — 연도 경계(예: 12월 코호트가 1월까지 미완결 추적 중)에서 리포트 조회 범위만
  보고 업로드 대상을 정하면 실제로 바뀐 지난 연도 DB가 누락되기 때문입니다
  (`cli.py`의 `_upload_db_files` 참고, `orchestration_guide.md` §3 원칙).

---

## 🚀 환경 설정 및 설치

### 1. 사전 요구 사항
- **Python 3.12** 이상 및 **`uv`** 패키지 관리자
- KRX 정보데이터시스템 계정 (백필/조회에 필요)
- **Docker 및 Docker Compose** (컨테이너 실행 시)

### 2. 패키지 설치
```bash
uv sync
```

### 3. 환경 변수 설정 (`.env`)
```env
KRX_USERNAME=your_krx_username
KRX_PASSWORD=your_krx_password

GOOGLE_DRIVE_ROOT_FOLDER_ID=your_google_drive_folder_id
GOOGLE_DRIVE_TOKEN_FILE=secrets/token.json
GOOGLE_DRIVE_CLIENT_SECRET_FILE=secrets/client_secret.json

LOCAL_STORAGE_BASE_PATH=data
SQLITE_DB_DIR=db
```

### 4. 시크릿 설정
`secrets/client_secret.json`(Google Cloud Console에서 발급받은 OAuth 2.0 Desktop app 클라이언트)을
넣어두면, 최초 실행(구글 드라이브 사용 시) 시 브라우저 인증을 거쳐 `secrets/token.json`이
자동 생성됩니다.

---

## 💻 사용법

```bash
# 당일 상한가 수집 + 휴장일 사이 공백 자동 백필
uv run python src/cli.py daily-update

# 특정 날짜 기준 수집
uv run python src/cli.py daily-update --date 2026-08-28

# 기간 백필
uv run python src/cli.py range-update --start 2026-01-01 --end 2026-08-28

# 연도 범위 백필 (수집만, 리포트는 별도)
uv run python src/cli.py annual-update --start-year 2020 --end-year 2024

# 엑셀 리포트 생성 (로컬)
uv run python src/cli.py export-excel --year 2026

# 엑셀 리포트 생성 + 구글 드라이브 업로드 (+ db/*.db 백업 업로드)
uv run python src/cli.py export-excel --year 2026 --drive
```

---

## 🐳 Docker로 실행

```bash
docker compose -f docker/docker-compose.yml build
docker compose -f docker/docker-compose.yml run --rm ceiling-tracker daily-update
docker compose -f docker/docker-compose.yml up -d ceiling-tracker-cron
```

컨테이너 내장 cron이 스케줄에 따라 데일리 파이프라인(`daily-update` → `export-excel --drive`)을
자동 실행합니다. 스케줄은 `docker/crontab`을 참고하세요(기본: 평일 15:55 KST — 장마감 이후
시세 확정 대기).

---

## 🧪 테스트

```bash
uv run pytest
```

---

## 💡 인수인계 시 주의 사항 (개발 팁)

1. **SQLite가 SSOT, `db/{year}.db`로 연도 분리**: `--drive` 업로드 시 리포트가 조회한
   연도만이 아니라 로컬에 실제 존재하는 모든 `db/*.db` 파일을 업로드 대상으로 삼습니다 —
   연도 경계에서 지난 연도 DB가 조용히 업로드 누락되는 걸 방지하기 위함입니다.
2. **`run-daily.sh`는 `set -e`로 파이프라인 단계를 제어**: `daily-update`가 실패(exit != 0)하면
   `export-excel`을 건너뜁니다. 새 CLI 커맨드를 추가할 때 예외를 삼키고 조용히 exit 0으로
   끝나지 않도록 주의하세요(`docker_guide.md` §10).
3. **Drive 저장 시 로컬 백업이 자동으로 따라붙음**: `_dual_save_workbook`/`export-excel --drive`
   경로는 Drive 저장 성공 여부와 무관하게 로컬에도 항상 저장합니다 — 순수 로컬 저장소로
   전환할 필요 없이 항상 로컬에 최신 산출물이 남습니다.
4. **의존성 패키지 관리 (`uv`)**: `pip install` 대신 `uv add <패키지명>`을 사용해
   `pyproject.toml`/`uv.lock`을 자동 최신화하세요.
