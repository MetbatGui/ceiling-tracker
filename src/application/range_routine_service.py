"""기간/연도 단위 대량 백필 루틴을 총괄하는 오케스트레이션 서비스입니다."""
from dataclasses import dataclass, field
from datetime import date
from typing import Callable, List, Optional

from src.application.range_update_service import RangeUpdateService


@dataclass
class RangeRunResult:
    """RangeRoutineService.run_range()의 실행 결과."""
    start_date: date
    end_date: date


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
    """

    def __init__(self, range_service_factory: Callable[[], RangeUpdateService]):
        self.range_service_factory = range_service_factory

    def run_range(self, start_date: date, end_date: date) -> RangeRunResult:
        """지정된 기간을 한 번에 백필합니다."""
        service = self.range_service_factory()
        service.execute_range_update(start_date, end_date)
        return RangeRunResult(start_date=start_date, end_date=end_date)

    def run_annual(self, start_year: int, end_year: int) -> AnnualRunResult:
        """연도별로 순차 백필합니다. 한 연도가 실패하면 즉시 중단합니다."""
        completed: List[int] = []
        for year in range(start_year, end_year + 1):
            try:
                self.run_range(date(year, 1, 2), date(year, 12, 30))
                completed.append(year)
            except Exception as e:
                return AnnualRunResult(completed_years=completed, failed_year=year, error=str(e))
        return AnnualRunResult(completed_years=completed)
