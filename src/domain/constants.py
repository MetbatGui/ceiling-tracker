"""
도메인 상수 정의

비즈니스 규칙과 관련된 상수들을 중앙 집중식으로 관리합니다.
"""

class TradingConstants:
    """주식 거래 관련 상수"""
    
    # 상한가 기준
    CEILING_RATE_MIN = 0.295  # 29.5%
    CEILING_RATE_MAX = 0.305  # 30.5%
    
    # 연속 상한가 기준
    CONSECUTIVE_CEILING_MIN = 2  # 연속 상한가 최소 일수 (색상 표시 기준)
    
    # 추적 기간
    TRACKING_DAYS = 20  # 과거 코호트 추적 일수 (달력 기준, 약 3주)
    FIXED_DATE_SLOTS = 10  # Excel에 표시할 고정 날짜 슬롯 (D+0 ~ D+9)
    
    # 신고가 기준
    NEW_HIGH_52W_THRESHOLD = 0.9  # 52주 신고가 근접 기준 (90%)
    NEW_HIGH_ALL_TIME_THRESHOLD = 0.9  # 역대 신고가 근접 기준 (90%)
    
    # 신고가 상태 레이블
    STATUS_ALL_TIME_NEW = "역·신"  # 역대 신고가
    STATUS_ALL_TIME_NEAR = "역·근"  # 역대 신고가 근접
    STATUS_52W_NEW = "52·신"  # 52주 신고가
    STATUS_52W_NEAR = "52·근"  # 52주 신고가 근접


class ExcelConstants:
    """Excel 출력 관련 상수"""
    
    # 컬럼 너비
    COLUMN_WIDTH_NAME = 12  # 종목명 컬럼
    COLUMN_WIDTH_STATUS = 8  # 신고가 상태 컬럼
    COLUMN_WIDTH_RATE = 10  # 등락률 컬럼
    COLUMN_WIDTH_DATE = 10  # 날짜 컬럼
    
    # 색상 (RGB)
    COLOR_HEADER = "4472C4"  # 헤더 배경색 (파란색)
    COLOR_RED = "FF0000"  # 연속 2일
    COLOR_ORANGE = "FFA500"  # 연속 3일
    COLOR_YELLOW = "FFFF00"  # 연속 4일
    COLOR_GREEN = "00FF00"  # 연속 5일
    COLOR_BLUE = "0000FF"  # 연속 6일
    COLOR_NAVY = "000080"  # 연속 7일
    COLOR_PURPLE = "800080"  # 연속 8일 이상
    
    # 연속 상한가 색상 매핑
    CONSECUTIVE_COLORS = {
        2: COLOR_RED,
        3: COLOR_ORANGE,
        4: COLOR_YELLOW,
        5: COLOR_GREEN,
        6: COLOR_BLUE,
        7: COLOR_NAVY,
    }
    # 8일 이상은 보라색
    CONSECUTIVE_COLOR_DEFAULT = COLOR_PURPLE
