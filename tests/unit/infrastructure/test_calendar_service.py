"""CalendarService 유닛 테스트"""
from datetime import date

from src.infrastructure.calendar_service import CalendarService


def _make_service(holidays: set) -> CalendarService:
    """_fetch_krx_holidays를 고정된 휴장일 집합으로 대체한 서비스를 만듭니다."""
    service = CalendarService()
    service._fetch_krx_holidays = lambda year: holidays  # type: ignore[method-assign]
    return service


# ---------------------------------------------------------------------------
# is_holiday
# ---------------------------------------------------------------------------

def test_is_holiday_weekend():
    """주말은 KRX 고시와 무관하게 휴장일입니다."""
    service = _make_service(holidays=set())

    assert service.is_holiday(date(2026, 8, 15)) is True  # 토
    assert service.is_holiday(date(2026, 8, 16)) is True  # 일


def test_is_holiday_krx_notice():
    """평일이라도 KRX 고시 휴장일이면 휴장일입니다."""
    service = _make_service(holidays={date(2026, 5, 1)})  # 근로자의 날 (금)

    assert service.is_holiday(date(2026, 5, 1)) is True


def test_is_holiday_weekday_trading_day():
    """평일이고 고시 휴장일도 아니면 거래일입니다."""
    service = _make_service(holidays={date(2026, 5, 1)})

    assert service.is_holiday(date(2026, 8, 18)) is False  # 화


# ---------------------------------------------------------------------------
# get_last_trading_day / get_first_trading_day
# ---------------------------------------------------------------------------

def test_get_last_trading_day_skips_weekend():
    """일요일 기준 직전 거래일은 금요일입니다."""
    service = _make_service(holidays=set())

    assert service.get_last_trading_day(date(2026, 8, 16)) == date(2026, 8, 14)


def test_get_first_trading_day_skips_holiday():
    """휴장일이면 다음 거래일로 넘어갑니다."""
    service = _make_service(holidays={date(2026, 5, 1)})

    assert service.get_first_trading_day(date(2026, 5, 1)) == date(2026, 5, 4)  # 월


# ---------------------------------------------------------------------------
# get_trading_range_in_period
# ---------------------------------------------------------------------------

def test_get_trading_range_in_period_excludes_holiday():
    """기간 내 휴장일을 제외한 첫/마지막 거래일을 반환합니다."""
    service = _make_service(holidays={date(2026, 5, 1)})

    first, last = service.get_trading_range_in_period(date(2026, 4, 27), date(2026, 5, 1))

    assert first == date(2026, 4, 27)
    assert last == date(2026, 4, 30)  # 5/1 휴장으로 목요일로 축소


def test_get_trading_range_in_period_no_trading_days():
    """전체 기간이 휴장일이면 (None, None)을 반환합니다."""
    service = _make_service(holidays=set())

    first, last = service.get_trading_range_in_period(date(2026, 8, 15), date(2026, 8, 16))

    assert (first, last) == (None, None)
