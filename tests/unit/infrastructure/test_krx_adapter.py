"""KrxDirectStockInfoAdapter 유닛 테스트 (네트워크 mock)"""
from unittest.mock import patch, MagicMock

import pytest

from src.infrastructure.krx_adapter import KrxDirectStockInfoAdapter


@pytest.fixture
def adapter(monkeypatch):
    """_login을 no-op으로 대체해 실제 KRX 세션 없이 인스턴스를 만듭니다."""
    monkeypatch.setenv("KRX_USERNAME", "test-user")
    monkeypatch.setenv("KRX_PASSWORD", "test-pw")
    with patch.object(KrxDirectStockInfoAdapter, '_login', return_value=None):
        return KrxDirectStockInfoAdapter()


def test_fetch_all_markets_raises_on_http_failure(adapter):
    """HTTP 상태코드가 200이 아니면 조용히 []를 반환하지 않고 예외를 던져야 합니다.

    "조회 실패"와 "조회 성공했는데 0건"을 구분하기 위한 계약입니다.
    """
    adapter.session.post = MagicMock(return_value=MagicMock(status_code=500))

    with pytest.raises(RuntimeError):
        adapter._fetch_all_markets("20260101")


def test_fetch_all_markets_returns_empty_list_when_success_but_no_data(adapter):
    """HTTP 200이고 데이터가 없으면(진짜 0건) 빈 리스트를 반환합니다."""
    resp = MagicMock(status_code=200)
    resp.json.return_value = {"OutBlock_1": []}
    adapter.session.post = MagicMock(return_value=resp)

    result = adapter._fetch_all_markets("20260101")

    assert result == []
