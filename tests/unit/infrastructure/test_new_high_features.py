import pytest
import pandas as pd
from datetime import date, timedelta
from unittest.mock import MagicMock, patch
from src.infrastructure.pykrx_adapter import PykrxStockInfoAdapter
from src.infrastructure.repository import ExcelCohortRepository
from src.domain.model import CeilingCohort, Stock, TrackedStock
import os

# --- Adapter Tests ---

@patch('src.infrastructure.krx_adapter.KrxDirectStockInfoAdapter._fetch_all_markets')
@patch('src.infrastructure.krx_adapter.requests.Session.post')
def test_adapter_identifies_all_time_high(mock_post, mock_fetch_all):
    # Setup - Target Date
    target_date = date(2026, 1, 23)
    target_date_str = "20260123"
    
    # Mock _fetch_all_markets (Today's Ceiling Candidate)
    mock_fetch_all.return_value = [
        {
            'ISU_ABBRV': 'Samsung',
            'ISU_SRT_CD': '005930',
            'ISU_CD': 'KR7005930003',
            'TDD_CLSPRC': '100,000',
            'FLUC_RT': '30.00' # 30%
        }
    ]
    
    # Mock OHLCV History API (MDCSTAT01701)
    mock_response = MagicMock()
    mock_response.json.return_value = {
        'output': [
            {'TRD_DD': '2020/01/01', 'TDD_HGPRC': '50,000'},
            {'TRD_DD': '2020/01/02', 'TDD_HGPRC': '60,000'},
            {'TRD_DD': '2020/01/03', 'TDD_HGPRC': '80,000'},
            {'TRD_DD': '2020/01/04', 'TDD_HGPRC': '90,000'},
            {'TRD_DD': '2020/01/05', 'TDD_HGPRC': '100,000'},
        ]
    }
    mock_post.return_value = mock_response
    
    # Ensure env vars are set for test to not trigger ValueError
    os.environ['KRX_USERNAME'] = 'testuser'
    os.environ['KRX_PASSWORD'] = 'testpw'
    
    from src.infrastructure.krx_adapter import KrxDirectStockInfoAdapter
    # Override _login to do nothing to avoid HTTP call during init
    with patch.object(KrxDirectStockInfoAdapter, '_login', return_value=None):
        adapter = KrxDirectStockInfoAdapter()
        results = adapter.fetch_today_ceiling_stocks(target_date)
        
        assert len(results) == 1
        assert results[0]['new_high_status'] == "역·신"

@patch('src.infrastructure.krx_adapter.KrxDirectStockInfoAdapter._fetch_all_markets')
@patch('src.infrastructure.krx_adapter.requests.Session.post')
def test_adapter_identifies_52_week_near(mock_post, mock_fetch_all):
    target_date = date(2026, 1, 23)
    
    # Mock _fetch_all_markets
    mock_fetch_all.return_value = [
        {
            'ISU_ABBRV': 'SkHynix',
            'ISU_SRT_CD': '000660',
            'ISU_CD': 'KR7000660001',
            'TDD_CLSPRC': '92,000',
            'FLUC_RT': '30.0'
        }
    ]
    
    # Mock OHLCV History API (MDCSTAT01701)
    # Target date is 2026/01/23. 
    # All-time high: 200,000 in 2020/01/31
    # 52w high: 100,000 in 2025/06/30
    mock_response = MagicMock()
    mock_response.json.return_value = {
        'output': [
            {'TRD_DD': '2020/01/31', 'TDD_HGPRC': '200,000'},
            {'TRD_DD': '2025/06/30', 'TDD_HGPRC': '100,000'},
        ]
    }
    mock_post.return_value = mock_response
    
    os.environ['KRX_USERNAME'] = 'testuser'
    os.environ['KRX_PASSWORD'] = 'testpw'
    
    from src.infrastructure.krx_adapter import KrxDirectStockInfoAdapter
    with patch.object(KrxDirectStockInfoAdapter, '_login', return_value=None):
        adapter = KrxDirectStockInfoAdapter()
        results = adapter.fetch_today_ceiling_stocks(target_date)
        
        # 92000 vs 52w Max 100000 -> 0.92 -> "52·근"
        assert len(results) == 1
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

