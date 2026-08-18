"""DailyUpdateService 유닛 테스트

StockDataProvider/CohortRepository를 mock으로 격리해 오케스트레이션 로직만 검증합니다.
"""
from datetime import date
from unittest.mock import MagicMock

import pytest

from src.application.daily_update_service import DailyUpdateService


@pytest.fixture
def mock_provider():
    provider = MagicMock()
    provider.fetch_today_ceiling_stocks.return_value = []
    return provider


@pytest.fixture
def mock_repo():
    repo = MagicMock()
    repo.load_incomplete_cohorts.return_value = []
    return repo


@pytest.fixture
def service(mock_provider, mock_repo):
    return DailyUpdateService(mock_provider, mock_repo)


# ---------------------------------------------------------------------------
# mark_collected 호출 검증
# ---------------------------------------------------------------------------

def test_marks_collected_with_zero_when_no_ceiling_stocks(service, mock_provider, mock_repo):
    """상한가가 0건이어도 mark_collected가 ceiling_count=0으로 호출되어야 합니다."""
    mock_provider.fetch_today_ceiling_stocks.return_value = []
    target_date = date(2026, 3, 1)

    service.execute_daily_update(target_date)

    mock_repo.mark_collected.assert_called_once_with(target_date, ceiling_count=0)
    mock_repo.save_cohort.assert_not_called()


def test_marks_collected_with_actual_count_when_stocks_found(service, mock_provider, mock_repo):
    """상한가가 있으면 그 개수로 mark_collected가 호출되고 코호트가 저장됩니다."""
    mock_provider.fetch_today_ceiling_stocks.return_value = [
        {'name': '삼성전자', 'code': '005930', 'close': 80000, 'new_high_status': ''},
    ]
    target_date = date(2026, 3, 1)

    service.execute_daily_update(target_date)

    mock_repo.mark_collected.assert_called_once_with(target_date, ceiling_count=1)
    mock_repo.save_cohort.assert_called_once()


def test_provider_failure_prevents_mark_collected(service, mock_provider, mock_repo):
    """provider가 예외를 던지면 mark_collected가 호출되면 안 됩니다 (실패 vs 0건 구분)."""
    mock_provider.fetch_today_ceiling_stocks.side_effect = RuntimeError("KRX 조회 실패")

    with pytest.raises(RuntimeError):
        service.execute_daily_update(date(2026, 3, 1))

    mock_repo.mark_collected.assert_not_called()


# ---------------------------------------------------------------------------
# load_incomplete_cohorts 사용 검증
# ---------------------------------------------------------------------------

def test_does_not_backfill_price_before_cohorts_own_start_date(service, mock_provider, mock_repo):
    """target_date보다 나중에 시작된 코호트에는 target_date 가격을 기록하면 안 됩니다.

    load_incomplete_cohorts()는 날짜 상한이 없으므로, 과거 gap_date를 백필할 때
    그보다 나중에 시작된(cohort_date가 미래인) 코호트가 섞여 들어올 수 있습니다.
    """
    from src.domain.model import CeilingCohort

    future_cohort = CeilingCohort(cohort_date=date(2026, 8, 17))
    future_cohort.add_stock("삼성전자", "005930", 80000)
    mock_repo.load_incomplete_cohorts.return_value = [future_cohort]
    mock_provider.fetch_current_prices.return_value = {"삼성전자": 81000}

    service.execute_daily_update(date(2026, 8, 14))  # future_cohort보다 과거인 gap_date 백필

    mock_repo.save_cohorts_batch.assert_not_called()


def test_update_past_cohorts_uses_incomplete_cohorts_not_recent_window(
    service, mock_provider, mock_repo
):
    """과거 코호트 갱신은 load_recent_cohorts(달력 윈도우)가 아니라
    load_incomplete_cohorts(완결 여부 기준)를 써야 합니다."""
    from src.domain.model import CeilingCohort

    old_cohort = CeilingCohort(cohort_date=date(2025, 1, 2))
    old_cohort.add_stock("카카오", "035720", 50000)
    mock_repo.load_incomplete_cohorts.return_value = [old_cohort]
    mock_provider.fetch_current_prices.return_value = {"카카오": 51000}

    service.execute_daily_update(date(2026, 3, 1))

    mock_repo.load_incomplete_cohorts.assert_called_once()
    mock_repo.load_recent_cohorts.assert_not_called()
    mock_repo.save_cohorts_batch.assert_called_once()
