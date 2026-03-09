"""상한가 추적 시스템의 명령행 인터페이스(CLI)를 정의합니다.

이 모듈은 데이터 수집(daily-update), 범위 업데이트(range-update), 엑셀 내보내기(export-excel) 등의 명령을 제공합니다.
"""
import click
from datetime import date, datetime
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
    from src.infrastructure.repository import ParquetCohortRepository
    from src.infrastructure.storage_adapters import LocalStorageAdapter, GoogleDriveAdapter
    from src.infrastructure.excel_renderer import ExcelRenderer
    from src.application.daily_update_service import DailyUpdateService
    from src.application.range_update_service import RangeUpdateService
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


def _build_repo(parquet_path: str = "cohorts.parquet"):
    local_storage = LocalStorageAdapter(base_path=os.getenv("LOCAL_STORAGE_BASE_PATH", "data"))
    return ParquetCohortRepository(storage=local_storage, parquet_path=parquet_path)


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

    provider = KrxDirectStockInfoAdapter()
    repo = _build_repo()
    service = DailyUpdateService(provider, repo)

    try:
        service.execute_daily_update(target_date)
        click.echo("✅ Parquet 데이터 업데이트 완료")
        click.echo("✨ Daily Update Completed Successfully")
        click.echo(" 리포트 생성: uv run python src/cli.py export-excel --year " + str(target_date.year))
    except Exception as e:
        click.echo(f"❌ Error during update: {e}")
        import traceback
        traceback.print_exc()


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

    provider = KrxDirectStockInfoAdapter()
    repo = _build_repo()
    service = RangeUpdateService(provider, repo)

    try:
        service.execute_range_update(start_date, end_date)
        click.echo("✅ Range Update Completed Successfully")
    except Exception as e:
        click.echo(f"❌ Error during update: {e}")
        import traceback
        traceback.print_exc()


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

    for year in range(start_year, end_year + 1):
        start_date = date(year, 1, 2)
        end_date = date(year, 12, 30)

        click.echo(f"\n>>> Processing Year: {year} ({start_date} ~ {end_date})")

        try:
            provider = KrxDirectStockInfoAdapter()
            repo = _build_repo()
            service = RangeUpdateService(provider, repo)
            service.execute_range_update(start_date, end_date)
            click.echo(f"✅ {year} 수집 완료.")
        except Exception as e:
            click.echo(f"❌ Error processing {year}: {e}")
            import traceback
            traceback.print_exc()
            click.echo("⚠️ 오류로 인해 annual update를 중단합니다.")
            return

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
            return

    # Parquet 데이터베이스는 무조건 로컬(가장 최신 수집본)에서 읽어옵니다.
    repo = _build_repo()
    provider = KrxDirectStockInfoAdapter()
    renderer = ExcelRenderer()

    # 결과물 출력 대상(storage)으로 전달 (Drive일 수도, 로컬일 수도 있음)
    service = ExcelExportService(repo, provider, renderer, storage)
    ok = service.generate_report(start_date, end_date, output_file)

    # Drive 업로드인 경우, 완성된 결과물(.xlsx)을 로컬에도 백업 저장
    if ok and use_drive:
        local_storage = LocalStorageAdapter(base_path=os.getenv("LOCAL_STORAGE_BASE_PATH", "data"))
        local_service = ExcelExportService(repo, provider, renderer, local_storage)
        local_service.generate_report(start_date, end_date, output_file)
        click.echo("💾 로컬 백업 완료")

    if ok:
        click.echo(f"✅ 엑셀 리포트 생성 완료: {output_file}")
    else:
        click.echo("❌ 엑셀 저장 실패")


if __name__ == '__main__':
    cli()
