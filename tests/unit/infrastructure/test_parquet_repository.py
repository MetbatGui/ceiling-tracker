"""ParquetCohortRepository 유닛 테스트"""
import pytest
import pandas as pd
from datetime import date
from unittest.mock import MagicMock

from src.infrastructure.repository import ParquetCohortRepository
from src.domain.model import CeilingCohort


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_storage():
    """빈 Parquet을 반환하는 mock StoragePort."""
    storage = MagicMock()
    storage.load_parquet.return_value = pd.DataFrame()
    storage.save_parquet.return_value = True
    return storage


@pytest.fixture
def repo(mock_storage):
    return ParquetCohortRepository(storage=mock_storage, parquet_path="test.parquet")


def _make_cohort(cohort_date: date, stocks: list) -> CeilingCohort:
    """테스트용 코호트를 생성합니다. stocks = [(name, code, price, status)]"""
    cohort = CeilingCohort(cohort_date=cohort_date)
    for name, code, price, status in stocks:
        cohort.add_stock(name, code, price, status)
    return cohort


# ---------------------------------------------------------------------------
# save_cohort 테스트
# ---------------------------------------------------------------------------

def test_save_cohort_calls_storage(repo, mock_storage):
    """save_cohort가 storage.save_parquet을 호출하는지 확인합니다."""
    cohort = _make_cohort(date(2026, 1, 5), [
        ("삼성전자", "005930", 80000, "역·신"),
    ])

    repo.save_cohort(cohort)

    mock_storage.save_parquet.assert_called_once()
    saved_df = mock_storage.save_parquet.call_args[0][0]

    # 당일 가격 행이 포함되어야 함
    assert len(saved_df) == 1
    assert saved_df.iloc[0]['stock_name'] == "삼성전자"
    assert saved_df.iloc[0]['stock_code'] == "005930"
    assert saved_df.iloc[0]['initial_price'] == 80000
    assert saved_df.iloc[0]['price'] == 80000
    assert saved_df.iloc[0]['new_high_status'] == "역·신"


def test_save_cohort_with_price_history(repo, mock_storage):
    """가격 히스토리가 있는 코호트를 저장하면 여러 행이 생성됩니다."""
    cohort = _make_cohort(date(2026, 1, 5), [
        ("SK하이닉스", "000660", 90000, "52·신"),
    ])
    cohort.stocks[0].add_price(date(2026, 1, 6), 92000)
    cohort.stocks[0].add_price(date(2026, 1, 7), 88000)

    repo.save_cohort(cohort)

    saved_df = mock_storage.save_parquet.call_args[0][0]
    # 당일(1) + 추적 2일 = 3행
    assert len(saved_df) == 3


def test_save_cohort_merges_with_existing(mock_storage):
    """기존 Parquet 데이터와 병합 시 중복 제거가 올바르게 동작합니다."""
    # 기존 데이터: (2026-01-05, 005930, 2026-01-05) 행이 있음
    existing_df = pd.DataFrame([{
        'cohort_date': pd.Timestamp('2026-01-05'),
        'stock_name': '삼성전자',
        'stock_code': '005930',
        'new_high_status': '',
        'initial_price': 80000,
        'price_date': pd.Timestamp('2026-01-05'),
        'price': 80000,
    }])
    mock_storage.load_parquet.return_value = existing_df

    repo = ParquetCohortRepository(storage=mock_storage, parquet_path="test.parquet")

    # 같은 날짜 코호트를 새 가격으로 저장
    cohort = _make_cohort(date(2026, 1, 5), [
        ("삼성전자", "005930", 81000, "역·신"),  # 가격 변경
    ])
    repo.save_cohort(cohort)

    saved_df = mock_storage.save_parquet.call_args[0][0]
    # 중복 없이 1행만 있어야 함
    assert len(saved_df) == 1
    assert saved_df.iloc[0]['price'] == 81000  # 새 가격으로 교체


# ---------------------------------------------------------------------------
# load_recent_cohorts 테스트
# ---------------------------------------------------------------------------

def _make_sample_parquet_df():
    """테스트용 Parquet DataFrame을 생성합니다."""
    rows = [
        # 코호트 2026-01-05: 당일 + D+1
        {
            'cohort_date': pd.Timestamp('2026-01-05'),
            'stock_name': '삼성전자',
            'stock_code': '005930',
            'new_high_status': '역·신',
            'initial_price': 80000,
            'price_date': pd.Timestamp('2026-01-05'),
            'price': 80000,
        },
        {
            'cohort_date': pd.Timestamp('2026-01-05'),
            'stock_name': '삼성전자',
            'stock_code': '005930',
            'new_high_status': '역·신',
            'initial_price': 80000,
            'price_date': pd.Timestamp('2026-01-06'),
            'price': 84000,
        },
        # 코호트 2026-01-10: 당일만
        {
            'cohort_date': pd.Timestamp('2026-01-10'),
            'stock_name': 'SK하이닉스',
            'stock_code': '000660',
            'new_high_status': '52·신',
            'initial_price': 90000,
            'price_date': pd.Timestamp('2026-01-10'),
            'price': 90000,
        },
    ]
    return pd.DataFrame(rows)


def test_load_recent_cohorts_returns_correct_objects(mock_storage):
    """load_recent_cohorts가 CeilingCohort 객체로 올바르게 복원합니다."""
    mock_storage.load_parquet.return_value = _make_sample_parquet_df()
    repo = ParquetCohortRepository(storage=mock_storage, parquet_path="test.parquet")

    cohorts = repo.load_recent_cohorts(limit_days=30, base_date=date(2026, 1, 15))

    assert len(cohorts) == 2
    first = cohorts[0]
    assert first.cohort_date == date(2026, 1, 5)
    assert len(first.stocks) == 1
    assert first.stocks[0].stock.name == '삼성전자'
    assert first.stocks[0].initial_price == 80000
    assert first.stocks[0].new_high_status == '역·신'


def test_load_recent_cohorts_restores_price_history(mock_storage):
    """load_recent_cohorts가 가격 히스토리를 올바르게 복원합니다."""
    mock_storage.load_parquet.return_value = _make_sample_parquet_df()
    repo = ParquetCohortRepository(storage=mock_storage, parquet_path="test.parquet")

    cohorts = repo.load_recent_cohorts(limit_days=30, base_date=date(2026, 1, 15))
    first = cohorts[0]
    tracked = first.stocks[0]

    # D+1 가격이 히스토리에 복원되어야 함 (당일은 initial_price)
    assert date(2026, 1, 6) in tracked.price_history
    assert tracked.price_history[date(2026, 1, 6)] == 84000


def test_load_recent_cohorts_filters_by_date(mock_storage):
    """limit_days 필터링이 올바르게 동작합니다."""
    mock_storage.load_parquet.return_value = _make_sample_parquet_df()
    repo = ParquetCohortRepository(storage=mock_storage, parquet_path="test.parquet")

    # 2026-01-10 기준 5일 이내 → 2026-01-05일 코호트는 제외
    cohorts = repo.load_recent_cohorts(limit_days=5, base_date=date(2026, 1, 15))

    assert len(cohorts) == 1
    assert cohorts[0].cohort_date == date(2026, 1, 10)


def test_load_recent_cohorts_returns_empty_when_no_data(mock_storage):
    """Parquet이 없을 때 빈 리스트를 반환합니다."""
    mock_storage.load_parquet.return_value = pd.DataFrame()
    repo = ParquetCohortRepository(storage=mock_storage, parquet_path="test.parquet")

    cohorts = repo.load_recent_cohorts(limit_days=30)

    assert cohorts == []


# ---------------------------------------------------------------------------
# load_cohorts_in_range 테스트
# ---------------------------------------------------------------------------

def test_load_cohorts_in_range(mock_storage):
    """날짜 범위 로드가 올바르게 동작합니다."""
    mock_storage.load_parquet.return_value = _make_sample_parquet_df()
    repo = ParquetCohortRepository(storage=mock_storage, parquet_path="test.parquet")

    # 2026-01-05 ~ 2026-01-07 범위: 첫 번째 코호트만
    cohorts = repo.load_cohorts_in_range(date(2026, 1, 5), date(2026, 1, 7))
    assert len(cohorts) == 1
    assert cohorts[0].cohort_date == date(2026, 1, 5)

    # 2026-01-01 ~ 2026-01-31 범위: 모두
    cohorts_all = repo.load_cohorts_in_range(date(2026, 1, 1), date(2026, 1, 31))
    assert len(cohorts_all) == 2


# ---------------------------------------------------------------------------
# round-trip 테스트 (저장 → 복원 일치)
# ---------------------------------------------------------------------------

def test_round_trip(tmp_path):
    """실제 파일에 저장 후 복원했을 때 데이터가 일치하는지 확인합니다."""
    from src.infrastructure.storage_adapters import LocalStorageAdapter

    storage = LocalStorageAdapter(base_path=str(tmp_path))
    repo = ParquetCohortRepository(storage=storage, parquet_path="test.parquet")

    # 코호트 생성
    cohort = _make_cohort(date(2026, 3, 1), [
        ("현대차", "005380", 200000, "역·신"),
        ("기아", "000270", 95000, "52·신"),
    ])
    cohort.stocks[0].add_price(date(2026, 3, 2), 210000)
    cohort.stocks[0].add_price(date(2026, 3, 3), 205000)

    repo.save_cohort(cohort)

    # 복원
    restored_cohorts = repo.load_cohorts_in_range(date(2026, 3, 1), date(2026, 3, 31))

    assert len(restored_cohorts) == 1
    restored = restored_cohorts[0]
    assert restored.cohort_date == date(2026, 3, 1)
    assert len(restored.stocks) == 2

    hyundai = next(s for s in restored.stocks if s.stock.name == "현대차")
    assert hyundai.initial_price == 200000
    assert hyundai.new_high_status == "역·신"
    assert hyundai.price_history[date(2026, 3, 2)] == 210000
    assert hyundai.price_history[date(2026, 3, 3)] == 205000
