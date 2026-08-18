"""RangeUpdateService 유닛 테스트

StockDataProvider/CohortRepository를 mock으로 격리해 오케스트레이션 로직만 검증합니다.
"""
from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.application.range_update_service import RangeUpdateService
from src.domain.model import CeilingCohort


@pytest.fixture
def mock_provider():
    provider = MagicMock()
    provider.fetch_candidates_in_range.return_value = {}
    provider.fetch_ohlcv_bulk.return_value = {}
    return provider


@pytest.fixture
def mock_repo():
    repo = MagicMock()
    repo.load_recent_cohorts.return_value = []
    return repo


@pytest.fixture
def service(mock_provider, mock_repo):
    return RangeUpdateService(mock_provider, mock_repo)


def _make_ohlcv_df(rows):
    """rows: [(date, close), ...] 형태를 OHLCV DataFrame으로 변환."""
    df = pd.DataFrame(rows, columns=['날짜', '종가']).set_index('날짜')
    df.index = pd.to_datetime(df.index)
    return df


def test_rebuild_clears_stale_price_not_reconfirmed_by_source(service, mock_provider, mock_repo):
    """기존에 저장된 가격이 새 소스 데이터로 재확인되지 않으면(예: 휴장일 0원 오염)
    범위 내 재구성 시 제거되어야 합니다."""
    cohort = CeilingCohort(cohort_date=date(2026, 1, 5))
    cohort.add_stock("삼성전자", "005930", 80000)
    cohort.stocks[0].add_price(date(2026, 5, 1), 0)  # 오염된 데이터 (휴장일)

    mock_repo.load_recent_cohorts.return_value = [cohort]
    # fetch_candidates_in_range이 비어있으면 execute_range_update가 조기 반환하므로
    # (기존 코호트 재구성과 무관한 후보 탐지 단계) 더미 후보를 하나 넣어 통과시킴
    mock_provider.fetch_candidates_in_range.return_value = {
        date(2026, 1, 5): [{'name': '삼성전자', 'code': '005930', 'close': 80000, 'rate': 0.3}]
    }
    # Naver 재수집 결과엔 휴장일(5/1) 데이터가 아예 없음 (실제 동작과 동일)
    mock_provider.fetch_ohlcv_bulk.return_value = {
        "005930": _make_ohlcv_df([
            (date(2026, 1, 5), 80000),
            (date(2026, 1, 6), 84000),
        ])
    }

    service.execute_range_update(date(2026, 1, 1), date(2026, 12, 31))

    assert date(2026, 5, 1) not in cohort.stocks[0].price_history
    assert cohort.stocks[0].price_history[date(2026, 1, 6)] == 84000


def test_rebuild_clears_stale_price_before_cohort_start(service, mock_provider, mock_repo):
    """코호트 시작일보다 이전 날짜에 잘못 기록된 가격도 재구성 시 제거되어야 합니다."""
    cohort = CeilingCohort(cohort_date=date(2026, 3, 3))
    cohort.add_stock("SK하이닉스", "000660", 90000)
    cohort.stocks[0].add_price(date(2026, 2, 27), 70000)  # 시작일 이전 오염 데이터

    mock_repo.load_recent_cohorts.return_value = [cohort]
    mock_provider.fetch_candidates_in_range.return_value = {
        date(2026, 3, 3): [{'name': 'SK하이닉스', 'code': '000660', 'close': 90000, 'rate': 0.3}]
    }
    mock_provider.fetch_ohlcv_bulk.return_value = {
        "000660": _make_ohlcv_df([
            (date(2026, 3, 3), 90000),
            (date(2026, 3, 4), 92000),
        ])
    }

    service.execute_range_update(date(2026, 1, 1), date(2026, 12, 31))

    assert date(2026, 2, 27) not in cohort.stocks[0].price_history
    assert cohort.stocks[0].price_history[date(2026, 3, 4)] == 92000


def test_rebuild_preserves_prices_outside_requested_range(service, mock_provider, mock_repo):
    """요청 범위 밖의 기존 가격 기록은 건드리지 않아야 합니다."""
    cohort = CeilingCohort(cohort_date=date(2025, 12, 20))
    cohort.add_stock("카카오", "035720", 50000)
    cohort.stocks[0].add_price(date(2025, 12, 22), 51000)  # 2025년, 범위 밖

    mock_repo.load_recent_cohorts.return_value = [cohort]
    mock_provider.fetch_candidates_in_range.return_value = {
        date(2026, 1, 5): [{'name': '다른종목', 'code': '999999', 'close': 10000, 'rate': 0.3}]
    }
    mock_provider.fetch_ohlcv_bulk.return_value = {}  # 2026년 범위 재수집이라 카카오 데이터는 안 옴

    service.execute_range_update(date(2026, 1, 1), date(2026, 12, 31))

    assert cohort.stocks[0].price_history[date(2025, 12, 22)] == 51000
