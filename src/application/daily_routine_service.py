"""일일 상한가 추적 루틴(휴장일 게이트 + 갭 백필 + 수집/업데이트)을 총괄하는
오케스트레이션 서비스입니다.
"""
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Callable, List, Optional

from src.application.daily_update_service import DailyUpdateService
from src.application.report_publish_service import PublishResult, ReportPublishService
from src.domain.ports import CohortRepository, StoragePort
from src.infrastructure.calendar_service import CalendarService
from src.infrastructure.db_sync import sync_db_down
from src.logger import logger

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
    download_failed: bool = False
    publish: Optional[PublishResult] = None


class DailyRoutineService:
    """휴장일 게이트 + 갭 백필 + (Drive 사용 시) DB 다운로드/업로드 + export를
    전부 소유하는 오케스트레이션 서비스 (orchestration_guide.md §1/§3).

    update_service_factory는 게이트를 통과했을 때만 호출됩니다 — KRX 로그인처럼
    비용이 드는 초기화를 휴장일에는 아예 타지 않게 하기 위함입니다.

    drive_storage_factory가 주어지면(None이 아니면) 실행 시작 시 GDrive에서 대상
    연도 DB를 받아 로컬을 덮어쓰고(§6.1/§6.2), 수집이 끝나면 publish_service_factory로
    엑셀 리포트를 렌더링해 산출물→DB 순서로 업로드까지 수행한다(§3). 둘 다 None이면
    (로컬 전용 모드) 다운로드/업로드 없이 수집만 한다.
    """

    def __init__(self, calendar: CalendarService,
                 update_service_factory: Callable[[], DailyUpdateService],
                 repo: CohortRepository,
                 lookback_days: int = AUTO_BACKFILL_LOOKBACK_TRADING_DAYS,
                 drive_storage_factory: Optional[Callable[[], Optional[StoragePort]]] = None,
                 db_dir: str = "db",
                 publish_service_factory: Optional[
                     Callable[[Optional[StoragePort]], ReportPublishService]
                 ] = None):
        self.calendar = calendar
        self.update_service_factory = update_service_factory
        self.repo = repo
        self.lookback_days = lookback_days
        self.drive_storage_factory = drive_storage_factory
        self.db_dir = db_dir
        self.publish_service_factory = publish_service_factory

    def run(self, target_date: date) -> DailyRunResult:
        """휴장일이면 스킵하고, 거래일이면 최근 공백을 백필한 뒤 오늘을 실행하고
        (설정돼 있으면) 리포트를 렌더링·업로드합니다."""
        if self.calendar.is_holiday(target_date):
            return DailyRunResult(skipped=True, reason="holiday")

        drive_storage = self.drive_storage_factory() if self.drive_storage_factory else None
        if drive_storage is not None:
            if not sync_db_down(drive_storage, self.db_dir, [target_date.year, target_date.year - 1]):
                return DailyRunResult(skipped=False, download_failed=True)

        gap_dates = self._detect_recent_gaps(target_date)
        if len(gap_dates) == self.lookback_days:
            logger.info(
                f"⚠️ 자동 백필 상한({self.lookback_days}거래일) 도달 — "
                "그 이상 공백이 있을 수 있습니다. range-update로 수동 확인 필요"
            )

        update_service = self.update_service_factory()
        for gap_date in gap_dates:
            # 과거 공백 하나가 실패해도 오늘 실행(가장 중요한 부분)까지 막히면 안 된다.
            try:
                update_service.execute_daily_update(gap_date)
            except Exception as e:
                logger.error(f"[DailyRoutineService] 백필 실패 ({gap_date}): {e}")
        update_service.execute_daily_update(target_date)

        publish_result = None
        if self.publish_service_factory is not None:
            publish_service = self.publish_service_factory(drive_storage)
            output_file = f"상한가분석({target_date.year}년).xlsx"
            publish_result = publish_service.publish(
                date(target_date.year, 1, 1), date(target_date.year, 12, 31), output_file
            )

        return DailyRunResult(skipped=False, backfilled=gap_dates, publish=publish_result)

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
