"""특정 기간 동안의 대량 업데이트를 처리하는 애플리케이션 서비스입니다.

이 모듈은 대량의 상한가 데이터를 한 번에 수집하고 분석하는 관리용 유즈케이스를 처리합니다.
"""
from datetime import date

import pandas as pd

from src.domain.model import CeilingCohort
from src.domain.ports import StockDataProvider, CohortRepository
from src.domain.constants import TradingConstants


class RangeUpdateService:
    """기간 단위 대량 업데이트를 담당하는 서비스입니다 (성능 최적화)."""

    def __init__(self, stock_provider: StockDataProvider,
                 cohort_repo: CohortRepository):
        """서비스를 초기화합니다.

        Args:
            stock_provider: 주식 데이터 제공자.
            cohort_repo: 코호트 저장소.
        """
        self.provider = stock_provider
        self.repo = cohort_repo

    def execute_range_update(self, start_date: date, end_date: date) -> None:
        """지정된 기간 동안의 상한가 데이터를 수집, 분석 및 저장합니다.

        Args:
            start_date: 시작 날짜.
            end_date: 종료 날짜.
        """
        print(f"\n[Service] Starting Range Update: {start_date} ~ {end_date}")

        # 1. 기간 내 상한가 후보군 수집
        daily_candidates = self.provider.fetch_candidates_in_range(start_date, end_date)
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
        for c_entity in existing_cohorts:
            for s in c_entity.stocks:
                if s.stock.code:
                    all_tickers.add(s.stock.code)

        if not all_tickers:
            print("[Service] No tickers to fetch.")
            return

        history_map = self.provider.fetch_ohlcv_bulk(
            list(all_tickers), date(1990, 1, 1), end_date
        )

        # 5A. 가격 업데이트
        for d, c_obj in cohort_map.items():
            for s in c_obj.stocks:
                if s.stock.code in history_map:
                    h_df = history_map[s.stock.code]

                    if pd.Timestamp(c_obj.cohort_date) in h_df.index:
                        s.initial_price = int(
                            h_df.loc[pd.Timestamp(c_obj.cohort_date), '종가']
                        )

                    range_mask = (
                        (h_df.index >= pd.Timestamp(start_date)) &
                        (h_df.index <= pd.Timestamp(end_date))
                    )
                    range_prices = h_df[range_mask]

                    for row_date, row_data in range_prices.iterrows():
                        r_date = row_date.date()
                        if r_date > c_obj.cohort_date:
                            s.add_price(r_date, int(row_data['종가']))

        # 5B. 신고가 분석
        print(f"[Service] Analyzing New High status for {len(cohort_map)} cohorts...")

        for d, c_obj in cohort_map.items():
            for s in c_obj.stocks:
                if s.stock.code in history_map:
                    hist_df = history_map[s.stock.code]
                    target_price = s.initial_price
                    target_date_obj = c_obj.cohort_date

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
