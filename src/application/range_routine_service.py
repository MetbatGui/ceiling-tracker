"""기간/연도 단위 대량 백필 루틴을 총괄하는 오케스트레이션 서비스입니다."""
from dataclasses import dataclass, field
from datetime import date
from typing import Callable, List, Optional

from src.application.range_update_service import RangeUpdateService
from src.domain.ports import StoragePort
from src.infrastructure.db_sync import sync_db_down


@dataclass
class RangeRunResult:
    """RangeRoutineService.run_range()의 실행 결과."""
    start_date: date
    end_date: date
    download_failed: bool = False


@dataclass
class AnnualRunResult:
    """RangeRoutineService.run_annual()의 실행 결과."""
    completed_years: List[int] = field(default_factory=list)
    failed_year: Optional[int] = None
    error: str = ""


class RangeRoutineService:
    """기간/연도 단위 백필을 실행하는 오케스트레이션 서비스.

    range_service_factory는 호출마다(연도별로) 새 RangeUpdateService를 만듭니다 —
    기존 CLI 동작(연도마다 새 KRX 세션)과 동일하게 유지하면서, 테스트에서는
    mock factory로 실제 로그인 없이 검증할 수 있게 하기 위함입니다.

    drive_storage_factory가 주어지면(None이 아니면) 백필 시작 전에 GDrive에서
    대상 연도 DB를 받아 로컬을 덮어쓴다(db_ssot_guide.md §6.1/§6.2). None이면
    (로컬 전용 모드) 다운로드 없이 수집만 한다.
    """

    def __init__(self, range_service_factory: Callable[[], RangeUpdateService],
                 drive_storage_factory: Optional[Callable[[], Optional[StoragePort]]] = None,
                 db_dir: str = "db"):
        self.range_service_factory = range_service_factory
        self.drive_storage_factory = drive_storage_factory
        self.db_dir = db_dir

    def run_range(self, start_date: date, end_date: date) -> RangeRunResult:
        """지정된 기간을 한 번에 백필합니다."""
        drive_storage = self.drive_storage_factory() if self.drive_storage_factory else None
        if drive_storage is not None:
            years = range(start_date.year, end_date.year + 1)
            if not sync_db_down(drive_storage, self.db_dir, years):
                return RangeRunResult(start_date=start_date, end_date=end_date, download_failed=True)

        service = self.range_service_factory()
        service.execute_range_update(start_date, end_date)
        return RangeRunResult(start_date=start_date, end_date=end_date)

    def run_annual(self, start_year: int, end_year: int) -> AnnualRunResult:
        """연도별로 순차 백필합니다. 한 연도가 실패하면 즉시 중단합니다."""
        completed: List[int] = []
        for year in range(start_year, end_year + 1):
            try:
                result = self.run_range(date(year, 1, 2), date(year, 12, 30))
                if result.download_failed:
                    return AnnualRunResult(
                        completed_years=completed, failed_year=year, error="DB 다운로드 실패"
                    )
                completed.append(year)
            except Exception as e:
                return AnnualRunResult(completed_years=completed, failed_year=year, error=str(e))
        return AnnualRunResult(completed_years=completed)
