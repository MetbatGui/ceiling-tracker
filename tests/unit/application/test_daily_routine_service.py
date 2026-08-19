"""DailyRoutineService 유닛 테스트

CalendarService/DailyUpdateService/CohortRepository를 mock으로 격리해
오케스트레이션 로직(휴장일 게이트, factory 지연 호출, 갭 백필)만 검증합니다.
"""
from datetime import date, timedelta
from unittest.mock import MagicMock

import pytest

from src.application.daily_routine_service import DailyRoutineService


@pytest.fixture
def mock_calendar():
    """기본값: 모든 날짜를 거래일(휴장 아님)로 취급."""
    calendar = MagicMock()
    calendar.is_holiday.return_value = False
    return calendar


@pytest.fixture
def mock_repo():
    """기본값: 모든 날짜가 이미 수집 완료된 것으로 취급 (백필 대상 없음)."""
    repo = MagicMock()
    repo.is_date_collected.return_value = True
    return repo


@pytest.fixture
def mock_update_service():
    return MagicMock()


@pytest.fixture
def factory(mock_update_service):
    return MagicMock(return_value=mock_update_service)


@pytest.fixture
def service(mock_calendar, mock_repo, factory):
    return DailyRoutineService(
        calendar=mock_calendar, update_service_factory=factory, repo=mock_repo
    )


# ---------------------------------------------------------------------------
# 휴장일 게이트
# ---------------------------------------------------------------------------

def test_holiday_skips_without_building_update_service(mock_calendar, mock_repo):
    """휴장일이면 factory를 호출하지 않고 스킵 결과를 반환해야 합니다."""
    mock_calendar.is_holiday.return_value = True
    factory = MagicMock(side_effect=AssertionError("휴장일인데 factory가 호출됨"))
    service = DailyRoutineService(calendar=mock_calendar, update_service_factory=factory, repo=mock_repo)

    result = service.run(date(2026, 5, 1))

    assert result.skipped is True
    assert result.reason == "holiday"
    factory.assert_not_called()


def test_trading_day_builds_and_executes_update(service, factory, mock_update_service):
    """거래일이면 factory로 서비스를 만들어 execute_daily_update를 호출해야 합니다."""
    result = service.run(date(2026, 8, 18))

    assert result.skipped is False
    factory.assert_called_once()
    mock_update_service.execute_daily_update.assert_called_with(date(2026, 8, 18))


# ---------------------------------------------------------------------------
# 갭 백필
# ---------------------------------------------------------------------------

def test_no_gaps_when_all_recent_dates_collected(service, mock_update_service):
    """최근 거래일이 전부 수집 완료 상태면 백필 없이 오늘만 실행합니다."""
    result = service.run(date(2026, 8, 18))

    assert result.backfilled == []
    mock_update_service.execute_daily_update.assert_called_once_with(date(2026, 8, 18))


def test_backfills_gap_dates_in_chronological_order(mock_calendar, mock_repo, factory, mock_update_service):
    """미수집 날짜를 오래된 순으로 백필한 뒤 오늘을 실행해야 합니다."""
    target = date(2026, 8, 18)  # 화
    gap1 = date(2026, 8, 14)   # 금 (더 오래됨)
    gap2 = date(2026, 8, 17)   # 월

    def is_collected(d):
        return d not in (gap1, gap2)

    mock_repo.is_date_collected.side_effect = is_collected
    service = DailyRoutineService(calendar=mock_calendar, update_service_factory=factory, repo=mock_repo)

    result = service.run(target)

    assert result.backfilled == [gap1, gap2]
    calls = [c.args[0] for c in mock_update_service.execute_daily_update.call_args_list]
    assert calls == [gap1, gap2, target]


def test_gap_scan_skips_holidays(mock_calendar, mock_repo, factory):
    """공백 탐색 중 휴장일은 후보에서 아예 제외되어야 합니다 (수집 여부를 묻지 않음)."""
    target = date(2026, 8, 18)  # 화
    weekend = date(2026, 8, 16)  # 일 (휴장)

    mock_calendar.is_holiday.side_effect = lambda d: d.weekday() >= 5
    mock_repo.is_date_collected.return_value = True
    service = DailyRoutineService(calendar=mock_calendar, update_service_factory=factory, repo=mock_repo)

    service.run(target)

    checked_dates = [c.args[0] for c in mock_repo.is_date_collected.call_args_list]
    assert weekend not in checked_dates


def test_gap_backfill_failure_does_not_block_today(mock_calendar, mock_repo, factory, mock_update_service, capsys):
    """공백 날짜 백필 중 하나가 실패해도 오늘 실행은 계속되어야 합니다."""
    target = date(2026, 8, 18)
    gap = date(2026, 8, 17)
    mock_repo.is_date_collected.side_effect = lambda d: d != gap
    mock_update_service.execute_daily_update.side_effect = [RuntimeError("KRX 조회 실패"), None]
    service = DailyRoutineService(calendar=mock_calendar, update_service_factory=factory, repo=mock_repo)

    result = service.run(target)

    calls = [c.args[0] for c in mock_update_service.execute_daily_update.call_args_list]
    assert calls == [gap, target]
    assert result.skipped is False


def test_backfill_cap_reached_logs_warning(mock_calendar, mock_repo, factory, caplog):
    """공백 후보가 lookback_days만큼 꽉 차면 경고를 출력해야 합니다."""
    mock_repo.is_date_collected.return_value = False
    service = DailyRoutineService(
        calendar=mock_calendar, update_service_factory=factory, repo=mock_repo, lookback_days=3
    )

    result = service.run(date(2026, 8, 18))

    assert len(result.backfilled) == 3
    assert "백필 상한" in caplog.text
