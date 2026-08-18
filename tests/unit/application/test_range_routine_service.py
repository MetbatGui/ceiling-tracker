"""RangeRoutineService 유닛 테스트

RangeUpdateService를 mock으로 격리해 오케스트레이션 로직(연도 루프, 실패 시 중단)만 검증합니다.
"""
from datetime import date
from unittest.mock import MagicMock

import pytest

from src.application.range_routine_service import RangeRoutineService


@pytest.fixture
def mock_range_service():
    return MagicMock()


@pytest.fixture
def factory(mock_range_service):
    return MagicMock(return_value=mock_range_service)


@pytest.fixture
def routine(factory):
    return RangeRoutineService(range_service_factory=factory)


# ---------------------------------------------------------------------------
# run_range
# ---------------------------------------------------------------------------

def test_run_range_calls_execute_range_update(routine, factory, mock_range_service):
    result = routine.run_range(date(2026, 1, 1), date(2026, 3, 31))

    factory.assert_called_once()
    mock_range_service.execute_range_update.assert_called_once_with(date(2026, 1, 1), date(2026, 3, 31))
    assert result.start_date == date(2026, 1, 1)
    assert result.end_date == date(2026, 3, 31)


# ---------------------------------------------------------------------------
# run_annual
# ---------------------------------------------------------------------------

def test_run_annual_processes_each_year_in_range(routine, factory, mock_range_service):
    result = routine.run_annual(2024, 2026)

    calls = [c.args for c in mock_range_service.execute_range_update.call_args_list]
    assert calls == [
        (date(2024, 1, 2), date(2024, 12, 30)),
        (date(2025, 1, 2), date(2025, 12, 30)),
        (date(2026, 1, 2), date(2026, 12, 30)),
    ]
    assert result.completed_years == [2024, 2025, 2026]
    assert result.failed_year is None


def test_run_annual_stops_on_first_failure(routine, factory, mock_range_service):
    mock_range_service.execute_range_update.side_effect = [None, RuntimeError("KRX 실패")]

    result = routine.run_annual(2024, 2026)

    assert result.completed_years == [2024]
    assert result.failed_year == 2025
    assert "KRX 실패" in result.error
    # 실패한 연도 이후(2026)는 시도하지 않아야 함
    assert mock_range_service.execute_range_update.call_count == 2
