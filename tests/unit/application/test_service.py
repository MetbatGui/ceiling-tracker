import pytest
from unittest.mock import MagicMock
from datetime import date
from src.application.service import DailyUpdateService
from src.domain.model import CeilingCohort


@pytest.fixture
def mock_provider():
    return MagicMock()


@pytest.fixture
def mock_repo():
    return MagicMock()


@pytest.fixture
def service(mock_provider, mock_repo):
    return DailyUpdateService(mock_provider, mock_repo)


def test_execute_daily_update_creates_cohort(service, mock_provider, mock_repo):
    target_date = date(2026, 1, 26)

    mock_provider.fetch_today_ceiling_stocks.return_value = [
        {'name': 'Samsung', 'code': '005930', 'close': 80000, 'rate': 30.0}
    ]
    mock_repo.load_recent_cohorts.return_value = []

    service.execute_daily_update(target_date)

    mock_repo.save_cohort.assert_called()
    saved_cohort = mock_repo.save_cohort.call_args[0][0]
    assert saved_cohort.cohort_date == target_date
    assert len(saved_cohort.stocks) == 1
    assert saved_cohort.stocks[0].stock.name == 'Samsung'


def test_execute_daily_update_updates_past_cohorts(service, mock_provider, mock_repo):
    today = date(2026, 1, 27)
    past_date = date(2026, 1, 26)

    mock_provider.fetch_today_ceiling_stocks.return_value = []

    past_cohort = CeilingCohort(past_date)
    past_cohort.add_stock("Samsung", "005930", 80000)
    mock_repo.load_recent_cohorts.return_value = [past_cohort]

    mock_provider.fetch_current_prices.return_value = {"Samsung": 88000}

    service.execute_daily_update(today)

    mock_repo.save_cohort.assert_called()
    saved_cohort = mock_repo.save_cohort.call_args[0][0]
    assert saved_cohort.cohort_date == past_date
    stock = saved_cohort.stocks[0]
    assert stock.price_history[today] == 88000
    assert stock.current_fluctuation_rate == pytest.approx(0.1)
