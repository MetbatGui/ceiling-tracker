"""KRX 거래소 휴장일을 판단하는 서비스입니다.

이 모듈은 daily-update 등 실행 시점의 날짜가 거래일인지 판단하는 게이트로 사용됩니다.
weekly_gainers 프로젝트의 CalendarService를 이식했습니다 (근로자의 날 하드코딩은 제외 —
실행 시점마다 KRX 고시를 조회하므로 당일 게이트 용도에서는 불필요).
"""
import time
from datetime import date, timedelta
from typing import Set, Tuple, Optional
import requests
from src.logger import logger


class CalendarService:
    """KRX 고시 휴장일을 조회해 거래일 여부를 판단합니다."""

    def __init__(self):
        self._holidays_cache: dict[str, Set[date]] = {}

    def _fetch_krx_holidays(self, year: str) -> Set[date]:
        """KRX OPN99000001.jspx API를 호출하여 휴장일 집합을 반환합니다."""
        if year in self._holidays_cache:
            return self._holidays_cache[year]

        url_otp = "https://open.krx.co.kr/contents/COM/GenerateOTP.jspx"
        url_data = "https://open.krx.co.kr/contents/OPN/99/OPN99000001.jspx"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36',
            'Referer': 'https://open.krx.co.kr/contents/MKD/01/0110/01100305/MKD01100305.jsp',
        }

        try:
            params = {
                'bld': 'MKD/01/0110/01100305/mkd01100305_01',
                'name': 'form',
                '_': int(time.time() * 1000)
            }
            resp_otp = requests.get(url_otp, params=params, headers=headers)
            if resp_otp.status_code != 200:
                return set()
            otp = resp_otp.text.strip()

            payload = {
                'search_bas_yy': year,
                'gridTp': 'KRX',
                'pagePath': '/contents/MKD/01/0110/01100305/MKD01100305.jsp',
                'code': otp
            }
            resp_data = requests.post(url_data, data=payload, headers=headers)
            if resp_data.status_code != 200:
                return set()

            holidays_set = set()
            result = resp_data.json()
            rows = result.get('block1', [])
            for row in rows:
                date_str = row.get('calnd_dd')
                if date_str:
                    y, m, d = map(int, date_str.split('-'))
                    holidays_set.add(date(y, m, d))

            self._holidays_cache[year] = holidays_set
            return holidays_set
        except Exception as e:
            logger.warning(f"[CalendarService] {year}년 KRX 휴장일 조회 중 예외 발생: {e}")
            return set()

    def is_holiday(self, target_date: date) -> bool:
        """주말이거나 KRX 고시 휴장일이면 True를 반환합니다."""
        if target_date.weekday() >= 5:
            return True
        year_str = str(target_date.year)
        holidays_set = self._fetch_krx_holidays(year_str)
        return target_date in holidays_set

    def get_last_trading_day(self, target_date: date) -> date:
        """주어진 날짜 이전(포함)의 가장 최근 영업일을 반환합니다."""
        curr = target_date
        while self.is_holiday(curr):
            curr -= timedelta(days=1)
        return curr

    def get_first_trading_day(self, target_date: date) -> date:
        """주어진 날짜 이후(포함)의 가장 첫 영업일을 반환합니다."""
        curr = target_date
        while self.is_holiday(curr):
            curr += timedelta(days=1)
        return curr

    def get_trading_range_in_period(self, start_date: date, end_date: date) -> Tuple[Optional[date], Optional[date]]:
        """지정된 기간 내 휴장일을 제외한 첫 거래일과 마지막 거래일을 반환합니다."""
        trading_days = self.get_trading_days(start_date, end_date)
        if not trading_days:
            return None, None
        return trading_days[0], trading_days[-1]

    def get_trading_days(self, start_date: date, end_date: date) -> list[date]:
        """지정된 기간 내 휴장일을 제외한 거래일 목록을 오름차순으로 반환합니다."""
        trading_days = []
        curr = start_date
        while curr <= end_date:
            if not self.is_holiday(curr):
                trading_days.append(curr)
            curr += timedelta(days=1)
        return trading_days


if __name__ == '__main__':
    # ponytail: assert 기반 self-check (근로자의 날 하드코딩 제거가 실제 KRX 고시로 커버되는지 확인)
    cal = CalendarService()
    assert cal.is_holiday(date(2026, 5, 2)) is True   # 토요일
    assert cal.is_holiday(date(2026, 5, 1)) is True   # 근로자의 날, KRX 고시에 포함됨
    assert cal.is_holiday(date(2026, 8, 18)) is False  # 평일, 휴장일 아님 (화요일)
    logger.info("[calendar_service] self-check OK")
