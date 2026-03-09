"""일일 상한가 추적 업데이트를 담당하는 애플리케이션 서비스입니다.

이 모듈은 매일 새로운 상한가 종목을 수집하고 과거 코호트의 가격을 업데이트하는 유즈케이스를 처리합니다.
"""
from datetime import date
from typing import List

from src.domain.model import CeilingCohort
from src.domain.ports import StockDataProvider, CohortRepository
from src.domain.constants import TradingConstants


class DailyUpdateService:
    """일일 상한가 추적 업데이트를 담당하는 서비스입니다."""

    def __init__(self, stock_provider: StockDataProvider,
                 cohort_repo: CohortRepository):
        """서비스를 초기화합니다.

        Args:
            stock_provider: 주식 데이터 제공자.
            cohort_repo: 코호트 저장소.
        """
        self.provider = stock_provider
        self.repo = cohort_repo

    def execute_daily_update(self, target_date: date) -> None:
        """오늘 상한가 코호트 생성 + 최근 N일 코호트 가격 업데이트."""
        print(f"[Service] Starting daily update for {target_date}...")
        self._create_today_cohort(target_date)
        self._update_past_cohorts(target_date)
        print("[Service] Daily update finished.")

    def _create_today_cohort(self, target_date: date) -> None:
        ceiling_stocks = self._fetch_ceiling_stocks(target_date)
        if not ceiling_stocks:
            return
        cohort = self._build_cohort_from_stocks(target_date, ceiling_stocks)
        self.repo.save_cohort(cohort)

    def _fetch_ceiling_stocks(self, target_date: date) -> List:
        ceiling_stocks = self.provider.fetch_today_ceiling_stocks(target_date)
        if not ceiling_stocks:
            print(f"[Service] No ceiling stocks found for {target_date}.")
            return []
        print(f"[Service] Found {len(ceiling_stocks)} ceiling stocks. Creating new cohort.")
        return ceiling_stocks

    def _build_cohort_from_stocks(self, target_date: date,
                                  ceiling_stocks: List) -> CeilingCohort:
        cohort = CeilingCohort(cohort_date=target_date)
        for stock_info in ceiling_stocks:
            cohort.add_stock(
                name=stock_info['name'],
                code=stock_info['code'],
                initial_price=stock_info['close'],
                new_high_status=stock_info.get('new_high_status', "")
            )
        return cohort

    def _update_past_cohorts(self, target_date: date) -> None:
        recent_cohorts = self._load_recent_cohorts(target_date)
        if not recent_cohorts:
            return
        all_prices = self._fetch_prices_for_cohorts(recent_cohorts, target_date)
        if not all_prices:
            return
        self._update_and_save_cohorts(recent_cohorts, target_date, all_prices)

    def _load_recent_cohorts(self, target_date: date) -> List[CeilingCohort]:
        recent_cohorts = self.repo.load_recent_cohorts(
            limit_days=TradingConstants.TRACKING_DAYS,
            base_date=target_date
        )
        if not recent_cohorts:
            print("[Service] No recent cohorts to update.")
            return []
        print(f"[Service] Updating {len(recent_cohorts)} recent cohorts...")
        return recent_cohorts

    def _fetch_prices_for_cohorts(self, cohorts: List[CeilingCohort],
                                   target_date: date) -> dict:
        all_stock_names = self._collect_stock_names(cohorts, target_date)
        if not all_stock_names:
            print("[Service] No stocks to update.")
            return {}
        print(f"[Service] Fetching prices for {len(all_stock_names)} unique stocks...")
        all_prices = self.provider.fetch_current_prices(list(all_stock_names), target_date)
        if not all_prices:
            print("[Service] No price data available for today.")
            return {}
        return all_prices

    def _collect_stock_names(self, cohorts: List[CeilingCohort],
                             exclude_date: date) -> set:
        all_stock_names: set[str] = set()
        for cohort in cohorts:
            if cohort.cohort_date != exclude_date:
                all_stock_names.update(s.stock.name for s in cohort.stocks)
        return all_stock_names

    def _update_and_save_cohorts(self, cohorts: List[CeilingCohort],
                                  target_date: date, all_prices: dict) -> None:
        updated = []
        for cohort in cohorts:
            if cohort.cohort_date == target_date:
                continue
            price_map = self._build_price_map_for_cohort(cohort, all_prices)
            if not price_map:
                print(f"  -> No price data for cohort {cohort.cohort_date}")
                continue
            cohort.update_prices(target_date, price_map)
            print(f"  -> Updated cohort {cohort.cohort_date} ({len(price_map)} stocks)")
            updated.append(cohort)

        # parquet R+W 단 1회
        if updated:
            self.repo.save_cohorts_batch(updated)

    def _update_single_cohort(self, cohort: CeilingCohort,
                               target_date: date, all_prices: dict) -> None:
        """(단일 저장용 레거시 메서드 - 내부적으로는 미사용)."""
        price_map = self._build_price_map_for_cohort(cohort, all_prices)
        if not price_map:
            return
        cohort.update_prices(target_date, price_map)
        self.repo.save_cohort(cohort)

    def _build_price_map_for_cohort(self, cohort: CeilingCohort,
                                    all_prices: dict) -> dict:
        stock_names = [s.stock.name for s in cohort.stocks]
        if not stock_names:
            return {}
        return {name: all_prices[name] for name in stock_names if name in all_prices}
