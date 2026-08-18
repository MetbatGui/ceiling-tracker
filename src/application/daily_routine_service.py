"""일일 상한가 추적 루틴(휴장일 게이트 + 수집/업데이트)을 총괄하는 오케스트레이션 서비스입니다."""
from dataclasses import dataclass
from datetime import date
from typing import Callable

from src.application.daily_update_service import DailyUpdateService
from src.infrastructure.calendar_service import CalendarService


@dataclass
class DailyRunResult:
    """DailyRoutineService.run()의 실행 결과."""
    skipped: bool
    reason: str = ""


class DailyRoutineService:
    """휴장일 게이트를 거쳐 DailyUpdateService를 실행하는 오케스트레이션 서비스.

    update_service_factory는 게이트를 통과했을 때만 호출됩니다 — KRX 로그인처럼
    비용이 드는 초기화를 휴장일에는 아예 타지 않게 하기 위함입니다.
    """

    def __init__(self, calendar: CalendarService,
                 update_service_factory: Callable[[], DailyUpdateService]):
        self.calendar = calendar
        self.update_service_factory = update_service_factory

    def run(self, target_date: date) -> DailyRunResult:
        """휴장일이면 스킵하고, 거래일이면 일일 업데이트를 실행합니다."""
        if self.calendar.is_holiday(target_date):
            return DailyRunResult(skipped=True, reason="holiday")

        update_service = self.update_service_factory()
        update_service.execute_daily_update(target_date)
        return DailyRunResult(skipped=False)
