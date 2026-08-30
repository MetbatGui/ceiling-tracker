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


# ---------------------------------------------------------------------------
# DB 다운로드 세션 소유 (orchestration_guide.md §1/§3, db_ssot_guide.md §6)
# ---------------------------------------------------------------------------

def test_drive_storage_factory_none_skips_download(mock_calendar, mock_repo, factory, mock_update_service):
    """drive_storage_factory를 안 주면(로컬 전용) 다운로드 없이 그냥 수집만 한다."""
    service = DailyRoutineService(
        calendar=mock_calendar, update_service_factory=factory, repo=mock_repo,
    )

    result = service.run(date(2026, 8, 18))

    assert result.download_failed is False
    mock_update_service.execute_daily_update.assert_called_once_with(date(2026, 8, 18))


def test_download_success_proceeds_to_collection(mock_calendar, mock_repo, factory, mock_update_service):
    from unittest.mock import patch

    drive_storage = MagicMock()
    service = DailyRoutineService(
        calendar=mock_calendar, update_service_factory=factory, repo=mock_repo,
        drive_storage_factory=lambda: drive_storage, db_dir="db",
    )

    with patch("src.application.daily_routine_service.sync_db_down", return_value=True) as mock_sync:
        result = service.run(date(2026, 8, 18))

    mock_sync.assert_called_once_with(drive_storage, "db", [2026, 2025])
    assert result.download_failed is False
    mock_update_service.execute_daily_update.assert_called_once_with(date(2026, 8, 18))


def test_download_failure_aborts_before_collection(mock_calendar, mock_repo, factory, mock_update_service):
    """db_ssot_guide.md §6.1: 원격에 파일은 있는데 다운로드가 실패하면 로컬 상태를
    신뢰할 수 없으므로 수집을 아예 시작하지 말고 중단해야 한다."""
    from unittest.mock import patch

    drive_storage = MagicMock()
    service = DailyRoutineService(
        calendar=mock_calendar, update_service_factory=factory, repo=mock_repo,
        drive_storage_factory=lambda: drive_storage, db_dir="db",
    )

    with patch("src.application.daily_routine_service.sync_db_down", return_value=False):
        result = service.run(date(2026, 8, 18))

    assert result.download_failed is True
    mock_update_service.execute_daily_update.assert_not_called()


# ---------------------------------------------------------------------------
# export/업로드가 오케스트레이션 흐름 안에 포함 (orchestration_guide.md §1)
# ---------------------------------------------------------------------------

def test_publish_service_factory_none_skips_publish(mock_calendar, mock_repo, factory):
    """publish_service_factory를 안 주면 publish 결과 없이 수집만 한다(하위 호환)."""
    service = DailyRoutineService(calendar=mock_calendar, update_service_factory=factory, repo=mock_repo)

    result = service.run(date(2026, 8, 18))

    assert result.publish is None


def test_publish_service_factory_called_with_drive_storage_after_collection(
    mock_calendar, mock_repo, factory, mock_update_service
):
    """수집이 끝난 뒤 publish_service_factory가 (다운로드에 쓴 것과 같은) drive_storage로
    호출되고, 그 결과가 DailyRunResult.publish에 담겨야 한다."""
    from unittest.mock import patch

    drive_storage = MagicMock()
    publish_service = MagicMock()
    publish_service.publish.return_value = "PUBLISH_RESULT_SENTINEL"
    publish_factory = MagicMock(return_value=publish_service)

    service = DailyRoutineService(
        calendar=mock_calendar, update_service_factory=factory, repo=mock_repo,
        drive_storage_factory=lambda: drive_storage, db_dir="db",
        publish_service_factory=publish_factory,
    )

    with patch("src.application.daily_routine_service.sync_db_down", return_value=True):
        result = service.run(date(2026, 8, 18))

    publish_factory.assert_called_once_with(drive_storage)
    publish_service.publish.assert_called_once_with(
        date(2026, 1, 1), date(2026, 12, 31), "상한가분석(2026년).xlsx"
    )
    assert result.publish == "PUBLISH_RESULT_SENTINEL"


def test_publish_not_attempted_when_download_fails(mock_calendar, mock_repo, factory):
    from unittest.mock import patch

    drive_storage = MagicMock()
    publish_factory = MagicMock()

    service = DailyRoutineService(
        calendar=mock_calendar, update_service_factory=factory, repo=mock_repo,
        drive_storage_factory=lambda: drive_storage, db_dir="db",
        publish_service_factory=publish_factory,
    )

    with patch("src.application.daily_routine_service.sync_db_down", return_value=False):
        service.run(date(2026, 8, 18))

    publish_factory.assert_not_called()
