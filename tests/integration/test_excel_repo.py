"""통합 테스트: ParquetCohortRepository round-trip"""
import pytest
from datetime import date
from src.infrastructure.repository import ParquetCohortRepository
from src.infrastructure.storage_adapters import LocalStorageAdapter
from src.domain.model import CeilingCohort


@pytest.fixture
def repo(tmp_path):
    storage = LocalStorageAdapter(base_path=str(tmp_path))
    return ParquetCohortRepository(storage=storage, parquet_path="cohorts.parquet")


def test_save_and_load_cohort(repo):
    c_date = date(2026, 1, 26)
    cohort = CeilingCohort(c_date)
    cohort.add_stock("TestStock", "001000", 1000)

    repo.save_cohort(cohort)

    loaded_cohorts = repo.load_recent_cohorts(limit_days=1, base_date=date(2026, 1, 27))
    assert len(loaded_cohorts) == 1
    loaded = loaded_cohorts[0]

    assert loaded.cohort_date == c_date
    assert len(loaded.stocks) == 1
    assert loaded.stocks[0].stock.name == "TestStock"
    assert loaded.stocks[0].initial_price == 1000


def test_update_cohort_with_new_price(repo):
    c_date = date(2026, 1, 26)
    cohort = CeilingCohort(c_date)
    cohort.add_stock("Samsung", "005930", 50000)
    repo.save_cohort(cohort)

    next_day = date(2026, 1, 27)
    cohort.update_prices(next_day, {"Samsung": 55000})
    repo.save_cohort(cohort)

    loaded_cohorts = repo.load_recent_cohorts(limit_days=5, base_date=date(2026, 1, 30))
    assert len(loaded_cohorts) == 1
    stock = loaded_cohorts[0].stocks[0]

    assert stock.price_history[next_day] == 55000
    assert abs(stock.current_fluctuation_rate - 0.1) < 0.0001
