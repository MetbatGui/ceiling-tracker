"""통합 테스트: CalendarService의 실제 KRX 휴장일 API 조회 검증"""
from datetime import date
from src.infrastructure.calendar_service import CalendarService


def test_fetch_krx_holidays_real_network():
    """실제 KRX API를 호출해 2026년 휴장일 목록을 가져옵니다."""
    service = CalendarService()

    holidays = service._fetch_krx_holidays("2026")

    assert len(holidays) > 0
    assert date(2026, 1, 1) in holidays  # 신정


def test_is_holiday_labor_day_covered_by_krx_notice():
    """근로자의 날(5/1)이 KRX 고시 휴장일에 실제로 포함되어 있는지 확인합니다.

    하드코딩 보정 없이도 실제 KRX 데이터로 휴장일 판정이 맞는지 검증하는 것이 목적입니다.
    """
    service = CalendarService()

    assert service.is_holiday(date(2026, 5, 1)) is True


def test_is_holiday_regular_weekday():
    """평일이고 휴장일이 아닌 날은 거래일로 판정됩니다."""
    service = CalendarService()

    assert service.is_holiday(date(2026, 8, 18)) is False  # 화요일, 휴장일 아님
