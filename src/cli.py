"""상한가 추적 시스템의 명령행 인터페이스(CLI)를 정의합니다.

이 모듈은 데이터 수집(daily-update), 범위 업데이트(range-update), 엑셀 내보내기(export-excel) 등의 명령을 제공합니다.
"""
import click
from datetime import date, datetime
from pathlib import Path
import sys
import os
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

# Add project root to sys.path
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_path)

# Windows Console Encoding Fix
if sys.platform.startswith('win'):
    (sys.stdout).reconfigure(encoding='utf-8')  # type: ignore[attr-defined, union-attr]

try:
    from src.infrastructure.krx_adapter import KrxDirectStockInfoAdapter
    from src.infrastructure.sqlite_repository import SqliteCohortRepository
    from src.infrastructure.storage_adapters import LocalStorageAdapter, GoogleDriveAdapter
    from src.infrastructure.excel_renderer import ExcelRenderer
    from src.infrastructure.calendar_service import CalendarService
    from src.application.daily_update_service import DailyUpdateService
    from src.application.daily_routine_service import DailyRoutineService
    from src.application.range_update_service import RangeUpdateService
    from src.application.range_routine_service import RangeRoutineService
    from src.application.excel_export_service import ExcelExportService
except ImportError:
    # 레거시 경로 대응 (필요한 경우)
    pass


# ---------------------------------------------------------------------------
# 공통 헬퍼
# ---------------------------------------------------------------------------

def _build_storage(use_drive: bool):
    """Storage 인스턴스를 생성합니다."""
    if use_drive:
        token_file = os.getenv("GOOGLE_DRIVE_TOKEN_FILE", "secrets/token.json")
        client_secret = os.getenv("GOOGLE_DRIVE_CLIENT_SECRET_FILE", "secrets/client_secret.json")
        folder_id = os.getenv("GOOGLE_DRIVE_ROOT_FOLDER_ID")

        if not folder_id:
            raise ValueError("GOOGLE_DRIVE_ROOT_FOLDER_ID 환경변수가 설정되지 않았습니다.")

        return GoogleDriveAdapter(
            token_file=token_file,
            root_folder_id=folder_id,
            client_secret_file=client_secret
        )
    else:
        base_path = os.getenv("LOCAL_STORAGE_BASE_PATH", "data")
        return LocalStorageAdapter(base_path=base_path)


def _build_repo():
    return SqliteCohortRepository(db_dir=os.getenv("SQLITE_DB_DIR", "db"))


def _try_build_drive_storage():
    """GOOGLE_DRIVE_ROOT_FOLDER_ID가 설정돼 있을 때만 Drive storage를 만든다.

    설정 자체가 없으면(로컬 전용 개발 환경 등) None을 반환해 다운로드 단계를
    건너뛴다 - 이건 "다운로드 실패"가 아니라 "Drive 미사용"이라 구분해야 한다
    (db_ssot_guide.md §6.1).
    """
    if not os.getenv("GOOGLE_DRIVE_ROOT_FOLDER_ID"):
        return None
    return _build_storage(True)


def _sync_db_down(storage, db_dir: str, years) -> bool:
    """GDrive에 있는 {year}.db를 로컬로 받아 덮어쓴다 (db_ssot_guide.md §6.1/§6.2).

    로컬에 이미 파일이 있어도 항상 다시 받는다("로컬은 항상 불신의 대상"). 원격에
    파일이 있는 게 확인됐는데 다운로드가 실패하면 "없음"과 구분해 실패로 보고한다 -
    빈/낡은 로컬로 계속 진행하다 그대로 재업로드하면 기존 데이터가 사라진다.

    Returns:
        bool: 대상 연도 전부가 정상(원격에 없거나 다운로드 성공)이면 True, 원격에
            파일이 있는데 다운로드가 실패한 연도가 하나라도 있으면 False.
    """
    db_path = Path(db_dir)
    db_path.mkdir(parents=True, exist_ok=True)
    ok = True
    for year in sorted(set(years)):
        remote_path = f"db/{year}.db"
        if not storage.path_exists(remote_path):
            continue  # 원격에 아직 없음 - 최초 수집 전이라 정상
        data = storage.get_file(remote_path)
        if data is None:
            click.echo(f"❌ {year}.db 다운로드 실패 - 원격에 파일은 있는데 읽지 못했습니다.")
            ok = False
            continue
        (db_path / f"{year}.db").write_bytes(data)
    return ok


def _upload_db_files(storage, db_dir: str) -> list:
    """db_dir에 있는 모든 {year}.db 파일을 storage에 업로드합니다.

    리포트 조회 날짜 범위가 아니라 로컬에 실제 존재하는 파일 전체를 대상으로
    한다 - 연도 경계에서는 지난 연도 코호트도 여전히 업데이트될 수 있어서
    (예: 12월 코호트가 1월에도 미완결 추적 중), 날짜 범위만 보고 업로드
    대상을 정하면 실제로 바뀐 지난 연도 db가 누락될 수 있다.

    Returns:
        업로드된 파일명(예: "2026.db") 리스트.
    """
    uploaded = []
    for local_path in sorted(Path(db_dir).glob("*.db")):
        if not local_path.stem.isdigit():
            continue
        with open(local_path, 'rb') as f:
            data = f.read()
        if storage.put_file(f"db/{local_path.name}", data):
            uploaded.append(local_path.name)
    return uploaded


def _dual_save_workbook(wb, filename: str, storage):
    """지정된 storage(드라이브 등)와 로컬 파일 시스템 모두에 엑셀을 저장합니다."""
    # 1. 지정된 저장소에 저장 (Drive 등)
    ok = storage.save_workbook(wb, filename)
    
    # 2. 로컬에도 강제로 백업 (storage가 로컬이 아닌 경우에만 중복 실행 방지)
    if not isinstance(storage, LocalStorageAdapter):
        local_base = os.getenv("LOCAL_STORAGE_BASE_PATH", "data")
        local_storage = LocalStorageAdapter(base_path=local_base)
        local_storage.save_workbook(wb, filename)
        click.echo(f"💾 로컬 백업 완료: {os.path.join(local_base, filename)}")
    
    return ok


# ---------------------------------------------------------------------------
# CLI 그룹
# ---------------------------------------------------------------------------

@click.group()
def cli():
    """상한가 추적 시스템 CLI."""
    pass


# ---------------------------------------------------------------------------
# daily-update
# ---------------------------------------------------------------------------

@cli.command()
@click.option('--date', 'target_date_str',
              help='YYYY-MM-DD 형식의 날짜 (기본값: 오늘)', default=None)
def daily_update(target_date_str):
    """일일 상한가 추적 작업을 실행합니다."""
    if target_date_str:
        try:
            target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
        except ValueError:
            click.echo("날짜 형식이 올바르지 않습니다. YYYY-MM-DD 형식을 사용해주세요.")
            return
    else:
        target_date = date.today()

    click.echo(f"=== Daily Update Start: {target_date} ===")

    db_dir = os.getenv("SQLITE_DB_DIR", "db")
    drive_storage = _try_build_drive_storage()
    if drive_storage is not None:
        if not _sync_db_down(drive_storage, db_dir, [target_date.year, target_date.year - 1]):
            click.echo("❌ DB 다운로드 실패 - 로컬 상태를 신뢰할 수 없어 중단합니다.")
            sys.exit(1)

    repo = _build_repo()
    routine = DailyRoutineService(
        calendar=CalendarService(),
        update_service_factory=lambda: DailyUpdateService(KrxDirectStockInfoAdapter(), repo),
        repo=repo,
    )

    try:
        result = routine.run(target_date)
        if result.skipped:
            click.echo(f"⏭️ 휴장일({result.reason})이라 스킵합니다: {target_date}")
        else:
            if result.backfilled:
                click.echo(f"🔁 공백 {len(result.backfilled)}일 백필: {result.backfilled}")
            click.echo("✅ SQLite 데이터 업데이트 완료")
            click.echo("✨ Daily Update Completed Successfully")
            click.echo(" 리포트 생성: uv run python src/cli.py export-excel --year " + str(target_date.year))
    except Exception as e:
        click.echo(f"❌ Error during update: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


# ---------------------------------------------------------------------------
# range-update
# ---------------------------------------------------------------------------

@cli.command()
@click.option('--start', 'start_date_str',
              help='YYYY-MM-DD 형식의 시작 날짜', required=True)
@click.option('--end', 'end_date_str',
              help='YYYY-MM-DD 형식의 종료 날짜 (기본값: 오늘)', default=None)
def range_update(start_date_str, end_date_str):
    """기간 단위 상한가 추적 작업을 실행합니다 (성능 최적화 버전)."""
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date = (
            datetime.strptime(end_date_str, "%Y-%m-%d").date()
            if end_date_str else date.today()
        )
        if start_date > end_date:
            click.echo("시작 날짜가 종료 날짜보다 뒤에 있을 수 없습니다.")
            return
    except ValueError:
        click.echo("날짜 형식이 올바르지 않습니다. YYYY-MM-DD 형식을 사용해주세요.")
        return

    click.echo(f"=== Range Update Start: {start_date} ~ {end_date} ===")

    db_dir = os.getenv("SQLITE_DB_DIR", "db")
    drive_storage = _try_build_drive_storage()
    if drive_storage is not None:
        if not _sync_db_down(drive_storage, db_dir, range(start_date.year, end_date.year + 1)):
            click.echo("❌ DB 다운로드 실패 - 로컬 상태를 신뢰할 수 없어 중단합니다.")
            sys.exit(1)

    routine = RangeRoutineService(
        range_service_factory=lambda: RangeUpdateService(KrxDirectStockInfoAdapter(), _build_repo()),
    )

    try:
        routine.run_range(start_date, end_date)
        click.echo("✅ Range Update Completed Successfully")
    except Exception as e:
        click.echo(f"❌ Error during update: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


# ---------------------------------------------------------------------------
# annual-update
# ---------------------------------------------------------------------------

@cli.command()
@click.option('--start-year', type=int, required=True, help='시작 연도 (예: 2020)')
@click.option('--end-year', type=int, required=True, help='종료 연도 (예: 2024)')
def annual_update(start_year, end_year):
    """연도별 상한가 분석 작업을 실행합니다.

    지정된 시작 연도부터 종료 연도까지 데이터를 Parquet에 수집합니다.
    엑셀 리포트는 별도로 export-excel 명령어를 사용하세요.
    """
    click.echo(f"=== Annual Update Start: {start_year} ~ {end_year} ===")

    db_dir = os.getenv("SQLITE_DB_DIR", "db")
    drive_storage = _try_build_drive_storage()
    if drive_storage is not None:
        if not _sync_db_down(drive_storage, db_dir, range(start_year, end_year + 1)):
            click.echo("❌ DB 다운로드 실패 - 로컬 상태를 신뢰할 수 없어 중단합니다.")
            sys.exit(1)

    routine = RangeRoutineService(
        range_service_factory=lambda: RangeUpdateService(KrxDirectStockInfoAdapter(), _build_repo()),
    )
    result = routine.run_annual(start_year, end_year)

    for year in result.completed_years:
        click.echo(f"✅ {year} 수집 완료.")

    if result.failed_year is not None:
        click.echo(f"❌ Error processing {result.failed_year}: {result.error}")
        click.echo("⚠️ 오류로 인해 annual update를 중단합니다.")
        sys.exit(1)

    click.echo("\n=== All Annual Updates Completed ===")
    click.echo("💡 엑셀 리포트를 생성하려면: uv run python -m src.cli export-excel --year <연도>")


# ---------------------------------------------------------------------------
# export-excel  (신규)
# ---------------------------------------------------------------------------

@cli.command()
@click.option('--year', type=int, default=None,
              help='내보낼 연도 (예: 2026). --start와 함께 사용 불가 (--end와는 사용 가능).')
@click.option('--start', 'start_date_str', default=None,
              help='YYYY-MM-DD 형식의 시작 날짜')
@click.option('--end', 'end_date_str', default=None,
              help='YYYY-MM-DD 또는 MM-DD 형식의 종료 날짜')
@click.option('--file', 'file_path', default=None,
              help='출력 엑셀 파일명 (기본값: 상한가분석({year}년).xlsx)')
@click.option('--drive', 'use_drive', is_flag=True,
              help='구글 드라이브에 저장 (기본값: 로컬 저장)')
def export_excel(year, start_date_str, end_date_str, file_path, use_drive):
    """Parquet 데이터를 엑셀 리포트로 내보냅니다.

    예시:
        uv run python src/cli.py export-excel --year 2026 --end 03-05
        uv run python src/cli.py export-excel --start 2026-01-01 --end 2026-02-28
    """
    # 날짜 범위 결정
    if year and start_date_str:
        click.echo("❌ --year 와 --start 는 동시에 사용할 수 없습니다. (단, --end 와는 함께 사용 가능합니다)")
        return

    def parse_end_date(end_str, default_year):
        try:
            return datetime.strptime(end_str, "%Y-%m-%d").date()
        except ValueError:
            try:
                # "MM-DD" 혹은 "M-D" 지원
                return datetime.strptime(f"{default_year}-{end_str}", "%Y-%m-%d").date()
            except ValueError:
                return None

    if year:
        start_date = date(year, 1, 1)
        if end_date_str:
            end_date = parse_end_date(end_date_str, year)
            if not end_date:
                click.echo("❌ 날짜 형식이 올바르지 않습니다. YYYY-MM-DD 또는 MM-DD 형식을 사용해주세요.")
                return
        else:
            end_date = date(year, 12, 31)
        default_filename = f"상한가분석({year}년).xlsx"
    elif start_date_str:
        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
            end_date = (
                parse_end_date(end_date_str, start_date.year)
                if end_date_str else date.today()
            )
            if end_date is None:
                click.echo("❌ 종료 날짜 형식이 올바르지 않습니다.")
                return
        except ValueError:
            click.echo("❌ 시작 날짜 형식이 올바르지 않습니다. YYYY-MM-DD 형식을 사용해주세요.")
            return
        default_filename = f"상한가분석({start_date}~{end_date}).xlsx"
    else:
        # 기본: 현재 연도
        current_year = date.today().year
        start_date = date(current_year, 1, 1)
        if end_date_str:
            end_date = parse_end_date(end_date_str, current_year)
            if not end_date:
                click.echo("❌ 종료 날짜 형식이 올바르지 않습니다.")
                return
        else:
            end_date = date(current_year, 12, 31)
        default_filename = f"상한가분석({current_year}년).xlsx"

    output_file = file_path or default_filename

    click.echo(f"=== Excel Export: {start_date} ~ {end_date} ===")
    click.echo(f"📄 출력 파일: {output_file}")

    try:
        storage = _build_storage(use_drive)
        click.echo(f"📁 저장소: {'Google Drive' if use_drive else '로컬 파일 시스템'}")
    except Exception as e:
        click.echo(f"❌ Storage 초기화 실패: {e}")
        if use_drive:
            click.echo("💡 로컬 저장소로 전환합니다.")
            storage = LocalStorageAdapter(base_path=os.getenv("LOCAL_STORAGE_BASE_PATH", "data"))
        else:
            sys.exit(1)

    # Parquet 데이터베이스는 무조건 로컬(가장 최신 수집본)에서 읽어옵니다.
    repo = _build_repo()
    calendar = CalendarService()
    renderer = ExcelRenderer()

    # 결과물 출력 대상(storage)으로 전달 (Drive일 수도, 로컬일 수도 있음)
    service = ExcelExportService(repo, calendar, renderer, storage)
    ok = service.generate_report(start_date, end_date, output_file)

    # Drive 업로드인 경우, 완성된 결과물(.xlsx)을 로컬에도 백업 저장
    if ok and isinstance(storage, GoogleDriveAdapter):
        local_storage = LocalStorageAdapter(base_path=os.getenv("LOCAL_STORAGE_BASE_PATH", "data"))
        local_service = ExcelExportService(repo, calendar, renderer, local_storage)
        local_service.generate_report(start_date, end_date, output_file)
        click.echo("💾 로컬 백업 완료")

        db_dir = os.getenv("SQLITE_DB_DIR", "db")
        db_files_present = [p.name for p in sorted(Path(db_dir).glob("*.db")) if p.stem.isdigit()]
        uploaded = _upload_db_files(storage, db_dir)
        if uploaded:
            click.echo(f"☁️ DB 파일 업로드 완료: {', '.join(uploaded)}")
        failed = sorted(set(db_files_present) - set(uploaded))
        if failed:
            # db_ssot_guide.md §6.2: 업로드 실패를 조용히 넘기면 이번에 계산한
            # 최신 데이터가 다음 실행의 무조건 다운로드로 낡은 원격 사본에 덮여
            # 사라질 수 있다 - 반드시 0이 아닌 exit code로 알려야 한다.
            click.echo(f"❌ DB 파일 업로드 실패: {', '.join(failed)}")
            sys.exit(1)

    if ok:
        click.echo(f"✅ 엑셀 리포트 생성 완료: {output_file}")
    else:
        click.echo("❌ 엑셀 저장 실패")
        sys.exit(1)


if __name__ == '__main__':
    cli()
