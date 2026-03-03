from datetime import date
from typing import List
import pandas as pd

from src.domain.model import CeilingCohort
from src.domain.ports import StockDataProvider, CohortRepository
from src.domain.constants import TradingConstants


class DailyUpdateService:
    """일일 상한가 추적 업데이트를 담당하는 서비스입니다.

    Attributes:
        provider: 주식 데이터 제공자
        repo: 코호트 저장소 (ParquetCohortRepository)
    """

    def __init__(self, stock_provider: StockDataProvider,
                 cohort_repo: CohortRepository):
        """서비스를 초기화합니다.

        Args:
            stock_provider: 주식 데이터 제공자
            cohort_repo: 코호트 저장소
        """
        self.provider = stock_provider
        self.repo = cohort_repo

    def execute_daily_update(self, target_date: date) -> None:
        """일일 업데이트 작업을 수행합니다.

        Note:
            1. 오늘 상한가 종목 수집 및 새 코호트 생성
            2. 최근 N일간의 코호트 업데이트
        """
        print(f"[Service] Starting daily update for {target_date}...")

        self._create_today_cohort(target_date)
        self._update_past_cohorts(target_date)

        print(f"[Service] Daily update finished.")

    def _create_today_cohort(self, target_date: date) -> None:
        """오늘 상한가 종목을 찾아 새로운 코호트로 저장합니다."""
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
        """과거 코호트들의 오늘 시세를 업데이트합니다."""
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
        all_prices = self.provider.fetch_current_prices(
            list(all_stock_names), target_date
        )

        if not all_prices:
            print("[Service] No price data available for today.")
            return {}

        return all_prices

    def _collect_stock_names(self, cohorts: List[CeilingCohort],
                             exclude_date: date) -> set:
        all_stock_names = set()
        for cohort in cohorts:
            if cohort.cohort_date != exclude_date:
                all_stock_names.update(s.stock.name for s in cohort.stocks)
        return all_stock_names

    def _update_and_save_cohorts(self, cohorts: List[CeilingCohort],
                                  target_date: date, all_prices: dict) -> None:
        for cohort in cohorts:
            if cohort.cohort_date == target_date:
                continue
            self._update_single_cohort(cohort, target_date, all_prices)

    def _update_single_cohort(self, cohort: CeilingCohort,
                               target_date: date, all_prices: dict) -> None:
        price_map = self._build_price_map_for_cohort(cohort, all_prices)

        if not price_map:
            print(f"  -> No price data for cohort {cohort.cohort_date}")
            return

        cohort.update_prices(target_date, price_map)
        self.repo.save_cohort(cohort)

        print(f"  -> Updated cohort {cohort.cohort_date} ({len(price_map)} stocks)")

    def _build_price_map_for_cohort(self, cohort: CeilingCohort,
                                    all_prices: dict) -> dict:
        stock_names = [s.stock.name for s in cohort.stocks]
        if not stock_names:
            return {}
        return {name: all_prices[name] for name in stock_names if name in all_prices}


class RangeUpdateService:
    """기간 단위 대량 업데이트를 담당하는 서비스입니다 (성능 최적화)."""

    def __init__(self, stock_provider: StockDataProvider,
                 cohort_repo: CohortRepository):
        self.provider = stock_provider
        self.repo = cohort_repo

    def execute_range_update(self, start_date: date, end_date: date) -> None:
        print(f"\n[Service] Starting Range Update: {start_date} ~ {end_date}")

        # 1. 기간 내 상한가 후보군 수집
        daily_candidates = self.provider.fetch_candidates_in_range(
            start_date, end_date
        )
        if not daily_candidates:
            print("[Service] No ceiling candidates found in this range.")
            return

        print(f"[Service] Found candidates in {len(daily_candidates)} trading days.")

        # 2. 새 후보 티커 수집
        new_candidate_tickers = set()
        for d, candidates in daily_candidates.items():
            for c in candidates:
                new_candidate_tickers.add(c['code'])

        # 3. 기존 코호트 로드 (기간 내 + 추적 기간)
        days_in_range = (end_date - start_date).days
        recent_days = days_in_range + TradingConstants.TRACKING_DAYS + 10

        existing_cohorts = self.repo.load_recent_cohorts(
            limit_days=recent_days,
            base_date=end_date
        )
        cohort_map = {c.cohort_date: c for c in existing_cohorts}

        # 새 코호트 추가
        for d, candidates in daily_candidates.items():
            if d not in cohort_map:
                cohort_map[d] = CeilingCohort(cohort_date=d)

            current_cohort = cohort_map[d]
            for c in candidates:
                current_cohort.add_stock(c['name'], c['code'], c['close'])

        # 4. 전체 티커 OHLCV 일괄 수집
        all_tickers = set(new_candidate_tickers)
        for c in existing_cohorts:
            for s in c.stocks:
                if s.stock.code:
                    all_tickers.add(s.stock.code)

        if not all_tickers:
            print("[Service] No tickers to fetch.")
            return

        history_map = self.provider.fetch_ohlcv_bulk(
            list(all_tickers), date(1990, 1, 1), end_date
        )

        # 5A. 가격 업데이트
        for d, cohort in cohort_map.items():
            for s in cohort.stocks:
                if s.stock.code in history_map:
                    h_df = history_map[s.stock.code]

                    # 상한가 당일 종가를 수정주가로 갱신
                    if pd.Timestamp(cohort.cohort_date) in h_df.index:
                        s.initial_price = int(
                            h_df.loc[pd.Timestamp(cohort.cohort_date), '종가']
                        )

                    range_mask = (
                        (h_df.index >= pd.Timestamp(start_date)) &
                        (h_df.index <= pd.Timestamp(end_date))
                    )
                    range_prices = h_df[range_mask]

                    for row_date, row_data in range_prices.iterrows():
                        r_date = row_date.date()
                        if r_date > cohort.cohort_date:
                            s.add_price(r_date, int(row_data['종가']))

        # 5B. 신고가 분석
        print(f"[Service] Analyzing New High status for {len(cohort_map)} cohorts...")

        for d, cohort in cohort_map.items():
            for s in cohort.stocks:
                if s.stock.code in history_map:
                    hist_df = history_map[s.stock.code]
                    target_price = s.initial_price
                    target_date_obj = cohort.cohort_date

                    past_mask = hist_df.index < pd.Timestamp(target_date_obj)
                    past_df = hist_df[past_mask]

                    if past_df.empty:
                        s.new_high_status = "신규"
                        continue

                    max_price = past_df['고가'].max()

                    if target_price >= max_price:
                        s.new_high_status = "역·신"
                    elif target_price >= max_price * 0.9:
                        s.new_high_status = "역·근"
                    else:
                        oneyear_ago = (
                            pd.Timestamp(target_date_obj) - pd.Timedelta(days=365)
                        )
                        recent_mask = past_df.index >= oneyear_ago
                        recent_df = past_df[recent_mask]

                        if not recent_df.empty:
                            max_52 = recent_df['고가'].max()
                            if target_price >= max_52:
                                s.new_high_status = "52·신"
                            elif target_price >= max_52 * 0.9:
                                s.new_high_status = "52·근"

        # 6. 전체 코호트 Parquet에 저장
        print(f"[Service] Saving {len(cohort_map)} cohorts to Parquet...")
        for d in sorted(cohort_map.keys()):
            self.repo.save_cohort(cohort_map[d])

        print("[Service] Range update completed.")
