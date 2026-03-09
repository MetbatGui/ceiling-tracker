"""KRX(한국거래소) 정보데이터시스템에서 주식 데이터를 수집하는 어댑터입니다.

이 모듈은 HTTP 요청을 통해 KRX 상한가 소스 및 가격 정보를 직접 스크래핑합니다.
"""
import os
import requests  # type: ignore
from typing import List, Dict, Any, Optional
from datetime import date
from src.domain.ports import StockDataProvider

class KrxDirectStockInfoAdapter(StockDataProvider):
    """KRX 정보데이터시스템(data.krx.co.kr)에서 직접 데이터를 스크래핑하는 어댑터."""
    
    BASE_URL = "https://data.krx.co.kr"
    LOGIN_URL = f"{BASE_URL}/contents/MDC/COMS/client/MDCCOMS001D1.cmd"
    
    def __init__(self, mbr_id: Optional[str] = None, pw: Optional[str] = None):
        """저장소를 초기화하고 KRX 세션을 획득합니다.

        Args:
            mbr_id: KRX 정보데이터시스템 사용자 ID.
            pw: KRX 정보데이터시스템 비밀번호.
        """
        self.mbr_id = mbr_id or os.getenv("KRX_USERNAME")
        self.pw = pw or os.getenv("KRX_PASSWORD")
        
        if not self.mbr_id or not self.pw:
            raise ValueError("KRX_USERNAME 또는 KRX_PASSWORD 환경변수가 설정되지 않았습니다.")
            
        self.session = requests.Session()
        self._login()

    def _login(self) -> None:
        """KRX 정보데이터시스템 로그인 후 세션 쿠키(JSESSIONID)를 갱신합니다.
        
        로그인 흐름:
          1. GET MDCCOMS001.cmd  → 초기 JSESSIONID 발급
          2. GET login.jsp       → iframe 세션 초기화
          3. POST MDCCOMS001D1.cmd → 실제 로그인
          4. CD011(중복 로그인) → skipDup=Y 추가 후 재전송
        """
        _LOGIN_PAGE = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001.cmd"
        _LOGIN_JSP  = "https://data.krx.co.kr/contents/MDC/COMS/client/view/login.jsp?site=mdc"
        _LOGIN_URL  = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001D1.cmd"
        _UA = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        )
        
        try:
            # 1 & 2. 초기 세션 발급
            self.session.get(_LOGIN_PAGE, headers={"User-Agent": _UA}, timeout=15)
            self.session.get(_LOGIN_JSP, headers={"User-Agent": _UA, "Referer": _LOGIN_PAGE}, timeout=15)
            
            payload = {
                "mbrNm": "", "telNo": "", "di": "", "certType": "",
                "mbrId": self.mbr_id, "pw": self.pw,
            }
            headers = {"User-Agent": _UA, "Referer": _LOGIN_PAGE}
            
            # 3. 로그인 POST
            resp = self.session.post(_LOGIN_URL, data=payload, headers=headers, timeout=15)
            data = resp.json()
            error_code = data.get("_error_code", "")
            
            # 4. CD011 중복 로그인 처리
            if error_code == "CD011":
                payload["skipDup"] = "Y"
                resp = self.session.post(_LOGIN_URL, data=payload, headers=headers, timeout=15)
                data = resp.json()
                error_code = data.get("_error_code", "")
                
            if error_code == "CD001":
                print(f"[KRX Adapter] 세션 획득 완료 (회원번호: {data.get('MBR_NO', '')})")
            else:
                print(f"[KRX Adapter] 로그인 에러: {data}")
                
            # 기본 쿠키 세팅
            self.session.cookies.set('mdc.client_session', 'true', domain='data.krx.co.kr')
            self.session.cookies.set('lang', 'ko_KR', domain='data.krx.co.kr')
            
        except Exception as e:
            print(f"[KRX Adapter] 로그인 요청 실패: {e}")
    def _fetch_all_markets(self, target_date_str: str) -> List[Dict[str, Any]]:
        """MDCSTAT01501을 호출하여 특정 날짜의 전종목 시세를 조회합니다."""
        url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': self.BASE_URL,
            'Referer': f'{self.BASE_URL}/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }
        payload = {
            'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',
            'locale': 'ko_KR',
            'mktId': 'ALL',
            'trdDd': target_date_str,
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false',
        }
        res = self.session.post(url, headers=headers, data=payload)
        if res.status_code != 200:
            return []
        data = res.json()
        output = data.get('OutBlock_1', [])
        if not output: 
            output = data.get('output', [])
        return output

    def _parse_num(self, val: str) -> float:
        try:
            return float(val.replace(',', ''))
        except (ValueError, TypeError):
            return 0.0

    def fetch_today_ceiling_stocks(self, target_date: date) -> List[Dict[str, Any]]:
        """해당 날짜의 전종목 시세를 조회하여 상한가 종목을 추출합니다.

        Args:
            target_date: 조회할 날짜.

        Returns:
            상한가 종목 정보 리스트.
        """
        target_date_str = target_date.strftime("%Y%m%d")
        print(f"[KRX Adapter] Fetching market data for {target_date}...")
        
        items = self._fetch_all_markets(target_date_str)
        if not items:
            print(f"[Warning] No data found for {target_date}. Is it a holiday?")
            return []
            
        results = []
        for row in items:
            fluc_rt = self._parse_num(row.get('FLUC_RT', '0'))
            close_prc = int(self._parse_num(row.get('TDD_CLSPRC', '0')))
            
            # 상한가 조건 (29.5 이상 30.5 이하), 종가 0 초과
            if 29.5 <= fluc_rt <= 30.5 and close_prc > 0:
                name = row.get('ISU_ABBRV', '')
                code = row.get('ISU_SRT_CD', '')
                isu_cd = row.get('ISU_CD', '') # for OHLCV history
                
                res = {
                    'name': name,
                    'code': code,
                    'close': close_prc,
                    'rate': round(fluc_rt / 100, 4),
                    'new_high_status': "",
                    '_isu_cd': isu_cd # Internal parsing usage
                }
                
                self._analyze_new_high(res, target_date_str)
                results.append(res)
                print(f"  -> Found: {name} ({res['rate']*100:.2f}%) Status: {res['new_high_status']}")
                
        return results

    def _analyze_new_high(self, res: dict, target_date_str: str):
        """특정 종목의 과거 시세를 네이버 금융 API(fchart)를 통해 호출해 신고가 여부를 판단합니다."""
        import datetime
        from datetime import timedelta
        import xml.etree.ElementTree as ET
        import requests
        
        target_dt = datetime.datetime.strptime(target_date_str, "%Y%m%d")
        cutoff_52w = target_dt - timedelta(days=365)
        
        # 네이버 금융 차트 API (수정주가 반영)
        # count=3650 (약 15년치 거래일)
        url = f"https://fchart.stock.naver.com/sise.nhn?symbol={res['code']}&timeframe=day&count=3650&requestType=0"
        
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            root = ET.fromstring(resp.text)
            items = root.findall('.//item')
            
            if not items:
                return
                
            all_time_highs = []
            recent_52w_highs = []
            
            for item in items:
                data = item.attrib.get('data')
                if not data:
                    continue
                # data format: YYYYMMDD|Open|High|Low|Close|Vol
                parts = data.split('|')
                if len(parts) >= 6:
                    trd_dt = datetime.datetime.strptime(parts[0], "%Y%m%d")
                    
                    # 목표일(target_dt) 이후의 미래 데이터는 무시 (과거 복기 시 중요)
                    if trd_dt > target_dt:
                        continue
                        
                    high_prc = float(parts[2])
                    all_time_highs.append(high_prc)
                    
                    if trd_dt >= cutoff_52w:
                        recent_52w_highs.append(high_prc)
                        
            if not all_time_highs:
                return
                
            max_all_time = max(all_time_highs)
            max_52w = max(recent_52w_highs) if recent_52w_highs else max_all_time
            current_close = res['close']
            
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
            print(f"[Warning] Failed to fetch Naver data for {res['name']}({res['code']}): {e}")

    def fetch_current_prices(self, identifiers: List[str], target_date: date) -> Dict[str, int]:
        """주어진 종목들의 특정 날짜 종가를 일괄 조회합니다.

        Args:
            identifiers: 종목명 또는 종목코드 리스트.
            target_date: 조회할 날짜.

        Returns:
            {종목명: 종가} 형태의 매핑 데이터.
        """
        target_date_str = target_date.strftime("%Y%m%d")
        print(f"[KRX Adapter] Batch fetching prices for {len(identifiers)} stocks...")
        
        items = self._fetch_all_markets(target_date_str)
        if not items:
            return {}
            
        code_to_price = {}
        name_to_price = {}
        
        for row in items:
            close_prc = int(self._parse_num(row.get('TDD_CLSPRC', '0')))
            code_to_price[row.get('ISU_SRT_CD', '')] = close_prc
            name_to_price[row.get('ISU_ABBRV', '')] = close_prc
            
        results = {}
        for ident in identifiers:
            if ident in name_to_price:
                results[ident] = name_to_price[ident]
            elif ident in code_to_price:
                results[ident] = code_to_price[ident]
                
        return results

    def fetch_ohlcv_bulk(self, tickers: List[str], start_date: date, end_date: date) -> Dict[str, Any]:
        """여러 종목의 기간별 OHLCV를 네이버 금융 API 연동을 통해 수집합니다."""
        import pandas as pd
        import xml.etree.ElementTree as ET
        import requests
        
        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")
        results = {}
        
        print(f"[KRX Adapter (Naver)] Fetching OHLCV for {len(tickers)} stocks ({start_str}~{end_str})...")
        
        # 시작과 끝 기간 대략적 산정 (넉넉히 15년치 = 3650 영업일 호출)
        # 네이버 API는 count 단위이므로 전체기간을 가져온 뒤 DataFrame 수준에서 필터링
        for ticker in tickers:
            url = f"https://fchart.stock.naver.com/sise.nhn?symbol={ticker}&timeframe=day&count=3650&requestType=0"
            try:
                resp = requests.get(url, timeout=10)
                resp.raise_for_status()
                root = ET.fromstring(resp.text)
                items = root.findall('.//item')
                
                if not items:
                    continue
                    
                all_records = []
                for item in items:
                    data = item.attrib.get('data')
                    if not data:
                        continue
                    parts = data.split('|')
                    if len(parts) >= 6:
                        # YYYYMMDD|Open|High|Low|Close|Vol
                        dt_str = parts[0]
                        all_records.append({
                            '날짜': pd.to_datetime(dt_str, format="%Y%m%d"),
                            '시가': int(parts[1]),
                            '고가': int(parts[2]),
                            '저가': int(parts[3]),
                            '종가': int(parts[4]),
                            '거래량': int(parts[5]),
                            '거래대금': 0, # fchart는 거래대금을 주지 않으므로 0으로 처리 (일반적으로 사용되지 않음)
                            '등락률': 0.0 # Pandas에서 추후 계산 (또는 기존 로직 호환 위해 임의의 0)
                        })
                
                if all_records:
                    df = pd.DataFrame(all_records)
                    df.set_index('날짜', inplace=True)
                    df.sort_index(inplace=True)
                    
                    # 기간 필터링
                    # DataFrame 인덱스가 DatetimeIndex이므로 start_date, end_date(문자열 캐싱 후 슬라이스 가능)
                    # date 객체를 pd.Timestamp로 변환하여 로케이션 비교
                    mask = (df.index >= pd.Timestamp(start_date)) & (df.index <= pd.Timestamp(end_date))
                    df_filtered = df.loc[mask].copy()
                    
                    # 등락률 계산
                    df_filtered['등락률'] = df_filtered['종가'].pct_change() * 100
                    
                    if not df_filtered.empty:
                        results[ticker] = df_filtered
                        
            except Exception as e:
                print(f"[Warning] Failed to fetch bulk OHLCV for {ticker}: {e}")
                
        return results

    def get_trading_days(self, start_date: date, end_date: date) -> List[date]:
        """두 날짜 사이의 실제 거래일 목록을 반환합니다. (삼성전자 시세 기준)."""
        start_str = start_date.strftime("%Y%m%d") if isinstance(start_date, date) else start_date
        end_str   = end_date.strftime("%Y%m%d")   if isinstance(end_date, date) else end_date
        return self._get_trading_days(start_str, end_str)

    def _get_trading_days(self, start_str: str, end_str: str) -> List[date]:
        """삼성전자(005930) 시세 추이를 이용해 거래일 목록을 추출합니다."""
        import datetime
        url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': self.BASE_URL,
            'Referer': f'{self.BASE_URL}/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201',
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            'X-Requested-With': 'XMLHttpRequest'
        }
        payload = {
            'bld': 'dbms/MDC/STAT/standard/MDCSTAT01701',
            'locale': 'ko_KR',
            'isuCd': 'KR7005930003', # 삼성전자
            'isuSrtCd': '005930',
            'strtDd': start_str,
            'endDd': end_str,
            'adjStkPrc_isNo': 'Y',
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false',
        }
        try:
            resp = self.session.post(url, headers=headers, data=payload)
            data = resp.json().get('output', [])
            
            days = []
            for row in data:
                trd_str = row.get('TRD_DD', '').replace('/', '')
                if trd_str:
                    days.append(datetime.datetime.strptime(trd_str, "%Y%m%d").date())
            # API 반환이 최신순일 수 있으므로 오름차순 정렬
            return sorted(days)
        except Exception:
            return []

    def fetch_candidates_in_range(self, start_date: date, end_date: date) -> Dict[date, List[Dict[str, Any]]]:
        """특정 기간 동안의 모든 상한가 종목 후보를 날짜별로 수집합니다.

        Args:
            start_date: 시작 날짜.
            end_date: 종료 날짜.

        Returns:
            {날짜: [상한가_종목_정보]} 형태의 매핑 데이터.
        """
        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")
        
        trading_days = self._get_trading_days(start_str, end_str)
        print(f"[KRX Adapter] Found {len(trading_days)} trading days in range.")
        
        results = {}
        for d in trading_days:
            d_str = d.strftime("%Y%m%d")
            items = self._fetch_all_markets(d_str)
            if not items:
                continue
                
            day_res = []
            for row in items:
                fluc_rt = self._parse_num(row.get('FLUC_RT', '0'))
                close_prc = int(self._parse_num(row.get('TDD_CLSPRC', '0')))
                
                if 29.5 <= fluc_rt <= 30.5 and close_prc > 0:
                    day_res.append({
                        'name': row.get('ISU_ABBRV', ''),
                        'code': row.get('ISU_SRT_CD', ''),
                        'close': close_prc,
                        'rate': round(fluc_rt / 100, 4)
                    })
            
            if day_res:
                results[d] = day_res
                
        return results
