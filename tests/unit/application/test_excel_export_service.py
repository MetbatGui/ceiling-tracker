"""ExcelExportService 유닛 테스트 (mock 기반)"""
from datetime import date
from unittest.mock import MagicMock

from src.application.excel_export_service import ExcelExportService


def _make_service(cohorts):
    repo = MagicMock()
    repo.load_cohorts_in_range.return_value = cohorts
    calendar = MagicMock()
    calendar.get_trading_days.return_value = [date(2026, 3, 2), date(2026, 3, 3)]
    renderer = MagicMock()
    storage = MagicMock()
    storage.save_workbook.return_value = True
    service = ExcelExportService(repo, calendar, renderer, storage)
    return service, repo, calendar, renderer, storage


def test_generate_report_uses_calendar_for_trading_days():
    """provider(KRX 로그인) 대신 CalendarService로 거래일을 조회합니다."""
    service, repo, calendar, renderer, storage = _make_service(cohorts=[MagicMock()])

    ok = service.generate_report(date(2026, 3, 1), date(2026, 3, 3), "out.xlsx")

    assert ok is True
    calendar.get_trading_days.assert_called_once_with(date(2026, 3, 1), date(2026, 3, 3))
    renderer.render.assert_called_once()
    _, kwargs = renderer.render.call_args
    assert kwargs["trading_days"] == [date(2026, 3, 2), date(2026, 3, 3)]


def test_generate_report_returns_false_when_no_cohorts():
    service, repo, calendar, renderer, storage = _make_service(cohorts=[])

    ok = service.generate_report(date(2026, 3, 1), date(2026, 3, 3), "out.xlsx")

    assert ok is False
    calendar.get_trading_days.assert_not_called()
