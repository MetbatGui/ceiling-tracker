"""상한가 추적 도메인의 핵심 모델(Entity, Value Object, Aggregate Root)을 정의합니다.

이 모듈은 상한가 종목과 그들의 가격 이력을 관리하는 비즈니스 로직을 포함합니다.
"""
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import date

@dataclass(frozen=True)
class Stock:
    """주식 종목을 나타내는 Value Object입니다.

    Attributes:
        name (str): 종목명
        code (str): 종목 코드
    """
    name: str
    code: str
    new_high_status: str = "" # 역·신, 역·근, 52·신, 52·근 etc.

@dataclass
class TrackedStock:
    """코호트 내에서 추적되는 주식 엔티티입니다.

    Attributes:
        stock (Stock): 추적 대상 주식 정보
        initial_price (int): 기록 시작일(상한가 달성일)의 종가
        price_history (Dict[date, int]): 날짜별 주가 히스토리
        new_high_status (str): 신고가 상태 (역·신, 역·근, 52·신, 52·근 등)
        cohort_date (Optional[date]): 이 종목이 속한 코호트의 날짜 (등락률 계산용)
    """
    stock: Stock
    initial_price: int
    price_history: Dict[date, int] = field(default_factory=dict)
    new_high_status: str = ""
    cohort_date: Optional[date] = None

    def __post_init__(self):
        """초기화 후 Stock의 new_high_status를 복사합니다."""
        if not self.new_high_status and self.stock.new_high_status:
            self.new_high_status = self.stock.new_high_status

    def add_price(self, date_key: date, price: int) -> None:
        """해당 날짜의 주가를 기록합니다.

        Args:
            date_key (date): 주가 날짜
            price (int): 종가
        """
        self.price_history[date_key] = price

    def get_latest_price(self) -> int:
        """가장 최근 기록된 주가를 반환합니다.

        기록이 없으면 초기 가격을 반환합니다.
        cohort_date 이후의 가격만 고려합니다.

        Returns:
            int: 최근 주가 또는 초기 가격
        """
        if not self.price_history:
            return self.initial_price
        
        # cohort_date 이후의 날짜만 필터링
        if self.cohort_date:
            future_prices = {d: p for d, p in self.price_history.items() if d > self.cohort_date}
            if not future_prices:
                return self.initial_price
            latest_date = sorted(future_prices.keys())[-1]
            return future_prices[latest_date]
        else:
            # cohort_date가 없으면 기존 로직 사용
            latest_date = sorted(self.price_history.keys())[-1]
            return self.price_history[latest_date]

    @property
    def current_fluctuation_rate(self) -> float:
        """현재 누적 등락률을 계산합니다.

        계산식: (최근 주가 / 초기 가격) - 1
        초기 가격이 0인 경우(예외 상황) 0.0을 반환합니다.

        Returns:
            float: 등락률 (예: 0.3 -> 30%)
        """
        if self.initial_price == 0:
            return 0.0
        
        latest_price = self.get_latest_price()
        return (latest_price / self.initial_price) - 1.0
    
    def calculate_consecutive_ceilings(self) -> int:
        """연속 상한가 일수를 계산합니다.
        
        상한가 기준: 전일 대비 29.5% 이상 상승
        
        Returns:
            int: 연속 상한가 일수 (최소 1, 상한가 당일 포함)
        """
        from src.domain.constants import TradingConstants
        
        cons_count = 1  # 초기 상한가 당일
        sorted_dates = sorted(self.price_history.keys())
        last_price = self.initial_price
        
        for d in sorted_dates:
            if self.cohort_date and d <= self.cohort_date:
                continue
            
            price = self.price_history[d]
            if last_price > 0:
                rate = (price - last_price) / last_price
                if rate >= TradingConstants.CEILING_RATE_MIN:
                    cons_count += 1
                else:
                    break  # 연속 끊김
            last_price = price
        
        return cons_count

@dataclass
class CeilingCohort:
    """특정 날짜(상한가 발생일)의 상한가 종목 그룹(Aggregate Root)입니다.

    Attributes:
        cohort_date (date): 상한가 발생 기준일
        stocks (List[TrackedStock]): 해당 날짜에 포함된 추적 종목 리스트
    """
    cohort_date: date
    stocks: List[TrackedStock] = field(default_factory=list)

    def add_stock(self, name: str, code: str, initial_price: int, new_high_status: str = "") -> None:
        """새로운 상한가 종목을 코호트에 추가합니다.

        Args:
            name (str): 종목명
            code (str): 종목 코드
            initial_price (int): 상한가 당시 종가
            new_high_status (str): 신고가 상태
        """
        # 종목 중복 체크 (코드 기준, 코드가 없으면 이름 기준)
        if any(s.stock.code == code or s.stock.name == name for s in self.stocks if code or name):
            return
            
        stock = Stock(name=name, code=code, new_high_status=new_high_status)
        tracked = TrackedStock(stock=stock, initial_price=initial_price, cohort_date=self.cohort_date)
        self.stocks.append(tracked)

    def update_prices(self, date_key: date, price_map: Dict[str, int]) -> None:
        """코호트 내 종목들의 특정 날짜 주가를 업데이트합니다.

        Args:
            date_key (date): 업데이트할 날짜
            price_map (Dict[str, int]): {종목명: 가격} 형태의 매핑 데이터
        """
        for tracked in self.stocks:
            if tracked.stock.name in price_map:
                tracked.add_price(date_key, price_map[tracked.stock.name])

    def get_stocks_data(self) -> List[Dict[str, Any]]:
        """보고서 또는 저장소 전달을 위한 종목 데이터를 반환합니다.

        Returns:
            List[Dict[str, Any]]: 종목별 평탄화(Flattened)된 데이터 리스트
        """
        results = []
        for s in self.stocks:
            data = {
                'name': s.stock.name,
                'code': s.stock.code,
                'new_high_status': s.new_high_status,  # TrackedStock의 new_high_status 사용
                'initial_price': s.initial_price,
                'current_rate': s.current_fluctuation_rate,
                'history': s.price_history
            }
            results.append(data)
        return results
