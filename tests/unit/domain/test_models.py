import pytest
from datetime import date, timedelta
from src.domain.model import CeilingCohort, Stock, TrackedStock

def test_fluctuation_rate_calculation():
    stock = Stock("Samsung", "005930")
    # Initial price 100
    tracked = TrackedStock(stock, 100)
    
    assert tracked.current_fluctuation_rate == 0.0
    
    # Price increased to 130
    tracked.add_price(date(2026, 1, 27), 130)
    assert pytest.approx(tracked.current_fluctuation_rate) == 0.3
    
    # Price dropped to 90
    tracked.add_price(date(2026, 1, 28), 90)
    assert pytest.approx(tracked.current_fluctuation_rate) == -0.1

def test_is_tracking_complete_false_when_below_fixed_slots():
    """가격 히스토리가 FIXED_DATE_SLOTS-1(9)개 미만이면 미완결입니다."""
    stock = Stock("Samsung", "005930")
    tracked = TrackedStock(stock, 100)

    base = date(2026, 1, 27)
    for i in range(8):  # D+1 ~ D+8 (8개)
        tracked.add_price(base + timedelta(days=i), 100 + i)

    assert tracked.is_tracking_complete() is False


def test_is_tracking_complete_true_when_reaches_fixed_slots():
    """가격 히스토리가 FIXED_DATE_SLOTS-1(9)개 이상이면 완결입니다."""
    stock = Stock("Samsung", "005930")
    tracked = TrackedStock(stock, 100)

    base = date(2026, 1, 27)
    for i in range(9):  # D+1 ~ D+9 (9개)
        tracked.add_price(base + timedelta(days=i), 100 + i)

    assert tracked.is_tracking_complete() is True


def test_cohort_update_prices():
    cohort_date = date(2026, 1, 26)
    cohort = CeilingCohort(cohort_date)
    
    cohort.add_stock("StockA", "001", 1000)
    cohort.add_stock("StockB", "002", 2000)
    
    # Update prices for next day
    next_day = date(2026, 1, 27)
    price_map = {
        "StockA": 1100,
        "StockB": 2200
    }
    
    cohort.update_prices(next_day, price_map)
    
    data = cohort.get_stocks_data()
    
    # Check StockA
    stock_a = next(filter(lambda x: x['name'] == "StockA", data))
    assert stock_a['current_rate'] == pytest.approx(0.1)
    assert stock_a['history'][next_day] == 1100
    
    # Check StockB
    stock_b = next(filter(lambda x: x['name'] == "StockB", data))
    assert stock_b['current_rate'] == pytest.approx(0.1)
