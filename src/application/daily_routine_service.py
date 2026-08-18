"""일일 상한가 추적 루틴(휴장일 게이트 + 갭 백필 + 수집/업데이트)을 총괄하는
오케스트레이션 서비스입니다.
"""
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Callable, List

from src.application.daily_update_service import DailyUpdateService
from src.domain.ports import CohortRepository
from src.infrastructure.calendar_service import CalendarService

# 자동 백필이 되돌아볼 최근 거래일 수 상한. 이보다 오래된 공백은 자동으로
# 안 채워지고 range-update로 수동 백필해야 함 (무한정 과거로 확장하면
# 장애 시 KRX 호출이 통제 불가능하게 늘어날 수 있어 고정 상한을 둠).
AUTO_BACKFILL_LOOKBACK_TRADING_DAYS = 10


@dataclass
class DailyRunResult:
    """DailyRoutineService.run()의 실행 결과."""
    skipped: bool
    reason: str = ""
    backfilled: List[date] = field(default_factory=list)


class DailyRoutineService:
    """휴장일 게이트 + 갭 백필을 거쳐 DailyUpdateService를 실행하는 오케스트레이션 서비스.

    update_service_factory는 게이트를 통과했을 때만 호출됩니다 — KRX 로그인처럼
    비용이 드는 초기화를 휴장일에는 아예 타지 않게 하기 위함입니다.
    """

    def __init__(self, calendar: CalendarService,
                 update_service_factory: Callable[[], DailyUpdateService],
                 repo: CohortRepository,
                 lookback_days: int = AUTO_BACKFILL_LOOKBACK_TRADING_DAYS):
        self.calendar = calendar
        self.update_service_factory = update_service_factory
        self.repo = repo
        self.lookback_days = lookback_days

    def run(self, target_date: date) -> DailyRunResult:
        """휴장일이면 스킵하고, 거래일이면 최근 공백을 백필한 뒤 오늘을 실행합니다."""
        if self.calendar.is_holiday(target_date):
            return DailyRunResult(skipped=True, reason="holiday")

        gap_dates = self._detect_recent_gaps(target_date)
        if len(gap_dates) == self.lookback_days:
            print(
                f"⚠️ 자동 백필 상한({self.lookback_days}거래일) 도달 — "
                "그 이상 공백이 있을 수 있습니다. range-update로 수동 확인 필요"
            )

        update_service = self.update_service_factory()
        for gap_date in gap_dates:
            update_service.execute_daily_update(gap_date)
        update_service.execute_daily_update(target_date)

        return DailyRunResult(skipped=False, backfilled=gap_dates)

    def _detect_recent_gaps(self, before_date: date) -> List[date]:
        """before_date 이전 최근 거래일 중 아직 수집되지 않은 날짜를 오래된 순으로 반환합니다."""
        candidates = []
        cursor = before_date - timedelta(days=1)
        while len(candidates) < self.lookback_days:
            if not self.calendar.is_holiday(cursor):
                candidates.append(cursor)
            cursor -= timedelta(days=1)
        candidates.reverse()

        return [d for d in candidates if not self.repo.is_date_collected(d)]
