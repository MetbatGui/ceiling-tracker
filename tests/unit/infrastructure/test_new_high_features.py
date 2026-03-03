import pytest
import pandas as pd
from datetime import date, timedelta
from unittest.mock import MagicMock, patch
from src.infrastructure.adapters import PykrxStockInfoAdapter
from src.infrastructure.repository import ExcelCohortRepository
from src.domain.model import CeilingCohort, Stock, TrackedStock
import os

# --- Adapter Tests ---

@patch('src.infrastructure.adapters.stock.get_market_price_change_by_ticker')
@patch('src.infrastructure.adapters.stock.get_market_ohlcv_by_date')
def test_adapter_identifies_all_time_high(mock_ohlcv, mock_price_change):
    # Setup - Target Date
    target_date = date(2026, 1, 23)
    target_date_str = "20260123"
    
    # Mock Price Change (Today's Ceiling Candidate)
    # columns: 종목명, 시가, 종가, 등락률, 거래량, ...
    # We need '종목명', '종가', '등락률'
    # return DataFrame indexed by Ticker
    mock_price_change.return_value = pd.DataFrame({
        '종목명': ['Samsung'],
        '종가': [100000],
        '등락률': [30.00] # 30%
    }, index=['005930'])
    
    # Mock OHLCV (History)
    # return DataFrame with '고가' (High)
    def history_side_effect(start, end, ticker):
        if ticker == "005930":
            dates = pd.date_range(start="2020-01-01", periods=5)
            return pd.DataFrame({
                '고가': [50000, 60000, 80000, 90000, 100000] # Max is 100000 (Current)
            }, index=dates)
        return pd.DataFrame()
        
    mock_ohlcv.side_effect = history_side_effect
    
    adapter = PykrxStockInfoAdapter()
    results = adapter.fetch_today_ceiling_stocks(target_date)
    
    assert len(results) == 2  # KOSPI + KOSDAQ 각각 호출
    assert results[0]['new_high_status'] == "역·신"

@patch('src.infrastructure.adapters.stock.get_market_price_change_by_ticker')
@patch('src.infrastructure.adapters.stock.get_market_ohlcv_by_date')
def test_adapter_identifies_52_week_near(mock_ohlcv, mock_price_change):
    target_date = date(2026, 1, 23)
    
    mock_price_change.return_value = pd.DataFrame({
        '종목명': ['SkHynix'],
        '종가': [92000],
        '등락률': [30.0]
    }, index=['000660'])
    
    def history_side_effect(start, end, ticker):
        if ticker == "000660":
            dates = pd.date_range(start="2020-01-01", end="2026-01-23", freq='M')
            data = {'고가': [50000] * len(dates)}
            df = pd.DataFrame(data, index=dates)
            
            # Set All Time High long ago
            df.loc['2020-01-31', '고가'] = 200000
            
            # Set 52 Week High recently (within last year from 2026-01-23)
            # A year ago is 2025-01-23
            df.loc['2025-06-30', '고가'] = 100000
            
            return df
        return pd.DataFrame()

    mock_ohlcv.side_effect = history_side_effect
    
    adapter = PykrxStockInfoAdapter()
    results = adapter.fetch_today_ceiling_stocks(target_date)
    
    # 92000 vs 52w Max 100000 -> 0.92 -> "52·근"
    # All time max is 200000.
    assert len(results) == 2  # KOSPI + KOSDAQ 각각 호출
    assert results[0]['new_high_status'] == "52·근"


# --- Repository Tests ---

def test_repo_saves_new_high_column(tmp_path):
    """ParquetCohortRepository가 신고가 상태를 올바르게 보존하는지 확인합니다."""
    from src.infrastructure.repository import ParquetCohortRepository
    from src.infrastructure.storage_adapters import LocalStorageAdapter

    storage = LocalStorageAdapter(base_path=str(tmp_path))
    repo = ParquetCohortRepository(storage=storage, parquet_path="cohorts.parquet")

    cohort_date = date(2026, 1, 23)
    cohort = CeilingCohort(cohort_date)

    # 신고가 상태가 있는 종목 추가
    cohort.add_stock("TestStock", "001000", 10000, new_high_status="역·신")

    repo.save_cohort(cohort)

    # 복원 후 신고가 상태 확인
    restored = repo.load_cohorts_in_range(cohort_date, cohort_date)
    assert len(restored) == 1
    stock = restored[0].stocks[0]
    assert stock.stock.name == "TestStock"
    assert stock.new_high_status == "역·신"
    assert stock.initial_price == 10000

