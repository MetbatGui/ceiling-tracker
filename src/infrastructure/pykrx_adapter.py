import time
from datetime import date
from typing import List, Dict, Any
import pandas as pd
from pykrx import stock
from tqdm import tqdm
from src.domain.ports import StockDataProvider

class PykrxStockInfoAdapter(StockDataProvider):
    """
    Pykrx 라이브러리를 사용하는 어댑터입니다.
    KRX(한국거래소) 공식 데이터를 스크래핑하므로 데이터 신뢰도가 높고,
    특정 날짜의 전 종목 시세를 한 번에 조회할 수 있어 백테스팅에 최적화되어 있습니다.
    """

    def _fetch_all_markets(self, date_str: str) -> pd.DataFrame:
        """
        KOSPI와 KOSDAQ 시장 데이터를 각각 조회하여 병합합니다. (KONEX 제외)
        """
        try:
            df_kospi = stock.get_market_price_change_by_ticker(date_str, date_str, market="KOSPI")
            df_kosdaq = stock.get_market_price_change_by_ticker(date_str, date_str, market="KOSDAQ")
            
            return pd.concat([df_kospi, df_kosdaq])
        except Exception:
            return pd.DataFrame()

    def fetch_today_ceiling_stocks(self, target_date: date) -> List[Dict[str, Any]]:
        # Pykrx는 날짜 포맷을 "YYYYMMDD" 문자열로 받습니다.
        target_date_str = target_date.strftime("%Y%m%d")
        display_date = target_date.strftime("%Y-%m-%d")
        
        print(f"[Pykrx] Fetching market data for {display_date}...")

        try:
            # 1. 시가총액 API를 사용하면 '종목명', '종가', '등락률', '거래량'을 한 번에 가져옵니다.
            # "ALL"은 코스피+코스닥 전체를 의미합니다.
            # 1. 등락률 데이터를 가져오기 위해 '일자별 등락률 상위' 조회 API를 사용하거나
            #    '기간별 등락률' 조회 API (get_market_price_change_by_ticker)를 사용합니다.
            #    단일 날짜 조회를 위해 시작일=종료일로 설정합니다.
            # KONEX 제외 요청으로 인해 KOSPI+KOSDAQ 병합 메서드 사용
            df = self._fetch_all_markets(target_date_str)
        except Exception as e:
            print(f"[Error] Failed to fetch data from KRX: {e}")
            return []

        # 데이터가 없는 경우 (휴장일 등)
        if df.empty:
            print(f"[Warning] No data found for {display_date}. Is it a holiday?")
            return []

        # 2. 상한가 필터링 (등락률 29.5% 이상)
        # Pykrx의 '등락률' 컬럼은 퍼센트 단위(float)입니다. (예: 29.87)
        # 거래량이 0이거나 종가가 0인 데이터는 제외
        cond = (df['등락률'] >= 29.5) & (df['등락률'] <= 30.5) & (df['종가'] > 0)
        candidates_df = df[cond].copy()

        results = []

        # 3. 필터링된 종목 상세 분석 (신고가 여부 확인)
        # index는 티커(종목코드)입니다.
        for ticker, row in tqdm(candidates_df.iterrows(), total=len(candidates_df), desc="Analyzing Candidates"):
            name = row['종목명']
            close = int(row['종가'])
            rate = float(row['등락률'])

            res = {
                'name': name,
                'code': f"{ticker:0>6}", # 005930 포맷 유지
                'close': close,
                'rate': round(rate / 100, 4), # 30.0 -> 0.3000 변환
                'new_high_status': ""
            }

            # 신고가 분석 (과거 데이터 조회)
            self._analyze_new_high(res, ticker, target_date_str)
            
            results.append(res)
            tqdm.write(f"  -> Found: {res['name']} ({res['rate']*100:.2f}%) Status: {res['new_high_status']}")

        return results

    def _analyze_new_high(self, res: dict, ticker: str, target_date_str: str):
        """
        해당 종목의 과거 데이터를 조회하여 신고가(52주/역대) 여부를 판단합니다.
        """
        try:
            # 1990년부터 타겟 날짜까지의 '일별 시세' 조회 (수정주가 자동 반영됨)
            # Pykrx는 시작일, 종료일, 티커 순으로 입력
            df_hist = stock.get_market_ohlcv_by_date("19900101", target_date_str, ticker)
            
            if df_hist.empty:
                return

            # 고가(High) 컬럼 사용
            high_prices = df_hist['고가']
            max_all_time = high_prices.max()
            current_close = res['close']

            # 52주 신고가 기준일 계산
            target_dt = pd.to_datetime(target_date_str, format="%Y%m%d")
            cutoff_52w = target_dt - pd.Timedelta(days=365)
            
            # 인덱스가 datetime 형식이므로 바로 비교 가능
            recent_52w = df_hist[df_hist.index >= cutoff_52w]
            max_52w = recent_52w['고가'].max() if not recent_52w.empty else max_all_time

            status = ""
            if current_close >= max_all_time:
                status = "역·신"
            elif current_close >= max_all_time * 0.9:
                status = "역·근"
            elif current_close >= max_52w:
                status = "52·신"
            elif current_close >= max_52w * 0.9:
                status = "52·근"
            
            res['new_high_status'] = status

        except Exception as e:
            # 개별 종목 분석 실패가 전체 로직을 멈추지 않도록 함
            pass

    def fetch_current_prices(self, identifiers: List[str], target_date: date) -> Dict[str, int]:
        """
        여러 종목의 현재가를 조회합니다. (Batch 방식)
        """
        target_date_str = target_date.strftime("%Y%m%d")
        print(f"[Pykrx] Batch fetching prices for {len(identifiers)} stocks...")
        
        try:
            # 시가총액 API를 호출하면 전 종목의 이름과 종가가 포함되어 있습니다.
            # 전 종목 시세를 가져오기 위해 get_market_price_change_by_ticker 사용
            df = self._fetch_all_markets(target_date_str)
        except Exception:
            return {}

        if df.empty:
            return {}

        # 검색 최적화를 위한 딕셔너리 생성
        # df의 인덱스는 '티커(Code)'입니다.
        code_to_price = df['종가'].to_dict()
        
        # 이름 -> 종가 매핑
        # 종목명이 인덱스가 아니므로 set_index나 zip을 사용
        name_to_price = dict(zip(df['종목명'], df['종가']))

        results = {}
        
        for ident in identifiers:
            price = None
            if ident in name_to_price:
                price = name_to_price[ident]
            elif ident in code_to_price: # ident가 종목코드인 경우
                price = code_to_price[ident]
            
            if price is not None:
                results[ident] = int(price)
                
        return results

    def fetch_ohlcv_bulk(self, tickers: List[str], start_date: date, end_date: date) -> Dict[str, Any]:
        """
        여러 종목의 기간별 OHLCV를 병렬로 수집합니다.
        Returns: {ticker: DataFrame}
        """
        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")
        results = {}
        
        print(f"[Pykrx] Fetching OHLCV for {len(tickers)} stocks ({start_str}~{end_str})...")
        for ticker in tqdm(tickers, desc="Fetching History"):
            try:
                df = stock.get_market_ohlcv_by_date(start_str, end_str, ticker, adjusted=True)
                if df is not None and not df.empty:
                    results[ticker] = df
                time.sleep(0.05)
            except Exception:
                pass

        return results

    def fetch_candidates_in_range(self, start_date: date, end_date: date) -> Dict[date, List[Dict[str, Any]]]:
        """
        기간 내 상한가 후보군을 병렬로 수집합니다.
        """
        # 1. Get Trading Days (using KOSPI)
        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")
        
        try:
            index_df = stock.get_index_ohlcv_by_date(start_str, end_str, "1001")
            trading_days = index_df.index
        except Exception:
            return {}
            
        results = {}
        
        def _scan_day(d):
            d_str = d.strftime("%Y%m%d")
            try:
                df = self._fetch_all_markets(d_str)
                if df.empty:
                    return d, []

                cond = (df['등락률'] >= 29.5) & (df['등락률'] <= 30.5) & (df['종가'] > 0)
                candidates = df[cond].copy()

                day_res = []
                for ticker, row in candidates.iterrows():
                    day_res.append({
                        'name': row['종목명'],
                        'code': f"{ticker:0>6}",
                        'close': int(row['종가']),
                        'rate': float(row['등락률']) / 100
                    })
                return d, day_res

            except Exception:
                return d, []

        print(f"[Pykrx] Scanning {len(trading_days)} trading days...")
        for d in tqdm(trading_days, desc="Scanning Market"):
            d_obj, candidates = _scan_day(d)
            if candidates:
                results[d_obj.date()] = candidates
            time.sleep(0.05)

        return results