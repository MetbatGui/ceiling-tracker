"""DailyRoutineService 유닛 테스트

CalendarService/DailyUpdateService를 mock으로 격리해 오케스트레이션 로직
(휴장일 게이트, factory 지연 호출)만 검증합니다.
"""
from datetime import date
from unittest.mock import MagicMock

import pytest

from src.application.daily_routine_service import DailyRoutineService


@pytest.fixture
def mock_calendar():
    return MagicMock()


@pytest.fixture
def mock_update_service():
    return MagicMock()


def test_holiday_skips_without_building_update_service(mock_calendar):
    """휴장일이면 factory를 호출하지 않고 스킵 결과를 반환해야 합니다."""
    mock_calendar.is_holiday.return_value = True
    factory = MagicMock(side_effect=AssertionError("휴장일인데 factory가 호출됨"))
    service = DailyRoutineService(calendar=mock_calendar, update_service_factory=factory)

    result = service.run(date(2026, 5, 1))

    assert result.skipped is True
    assert result.reason == "holiday"
    factory.assert_not_called()


def test_trading_day_builds_and_executes_update(mock_calendar, mock_update_service):
    """거래일이면 factory로 서비스를 만들어 execute_daily_update를 호출해야 합니다."""
    mock_calendar.is_holiday.return_value = False
    factory = MagicMock(return_value=mock_update_service)
    service = DailyRoutineService(calendar=mock_calendar, update_service_factory=factory)

    result = service.run(date(2026, 8, 18))

    assert result.skipped is False
    factory.assert_called_once()
    mock_update_service.execute_daily_update.assert_called_once_with(date(2026, 8, 18))
