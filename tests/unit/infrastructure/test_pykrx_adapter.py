import pytest
from unittest.mock import patch, MagicMock
from datetime import date
import pandas as pd
from src.infrastructure.pykrx_adapter import PykrxStockInfoAdapter

@pytest.fixture
def adapter():
    return PykrxStockInfoAdapter()

@pytest.fixture
def mock_stock():
    with patch('src.infrastructure.pykrx_adapter.stock') as mock:
        yield mock

def test_fetch_today_ceiling_stocks(adapter, mock_stock):
    target_date = date(2026, 1, 26)
    
    # Mock return for get_market_price_change_by_ticker
    # Columns required: 종목명, 종가, 등락률
    data = {
        '종목명': ['Stock_001', 'Stock_002', 'Stock_003'],
        '종가': [1000, 2000, 3000],
        '등락률': [29.9, 10.0, 30.0]
    }
    # Index is ticker
    index = ['001', '002', '003']
    df = pd.DataFrame(data, index=index)
    
    mock_stock.get_market_price_change_by_ticker.return_value = df
    
    # Mock NEW HIGH analysis (get_market_ohlcv_by_date)
    # Just return empty or valid df to avoid errors
    mock_stock.get_market_ohlcv_by_date.return_value = pd.DataFrame()

    # Call
    stocks = adapter.fetch_today_ceiling_stocks(target_date)
    
    # KOSPI + KOSDAQ 각각 호출하므로 결과가 2배 (001, 003 각 2개)
    # 중복을 허용하는 현재 어댑터 동작 반영 (get_market_price_change_by_ticker x2)
    assert len(stocks) == 4
    codes = [s['code'] for s in stocks]
    assert '000001' in codes
    assert '000003' in codes
    
    assert mock_stock.get_market_price_change_by_ticker.call_count == 2

def test_fetch_current_prices(adapter, mock_stock):
    target_date = date(2026, 1, 26)
    
    # Mock data for price fetching
    data = {
        '종목명': ['Stock_001', 'Stock_002'],
        '종가': [1000, 2000],
        '등락률': [0.0, 0.0]
    }
    df = pd.DataFrame(data, index=['001', '002'])
    
    mock_stock.get_market_price_change_by_ticker.return_value = df
    
    # Call with names or codes?
    # Adapter checks both code dict and name dict.
    # Case 1: By Name
    prices = adapter.fetch_current_prices(['Stock_001'], target_date)
    assert prices['Stock_001'] == 1000
    
    # Case 2: By Code
    prices_2 = adapter.fetch_current_prices(['002'], target_date) # 002 matches index
    assert prices_2['002'] == 2000
    
    # Verify calls
    # fetch_current_prices는 KOSPI + KOSDAQ 각각 호출 → 총 4회
    assert mock_stock.get_market_price_change_by_ticker.call_count == 4
