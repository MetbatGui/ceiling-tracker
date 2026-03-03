from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import date
import pandas as pd
import openpyxl

class StockDataProvider(ABC):
    """주식 데이터 제공을 위한 추상 기본 클래스(Port)입니다."""

    @abstractmethod
    def fetch_today_ceiling_stocks(self, target_date: date) -> List[Dict[str, Any]]:
        """해당 날짜의 상한가 종목 리스트를 가져옵니다.

        Args:
            target_date (date): 조회할 날짜

        Returns:
            List[Dict[str, Any]]: 상한가 종목 정보 리스트
            (예: [{'name': '삼성전자', 'code': '005930', 'close': 80000, 'rate': 30.0}, ...]
        """
        pass

    @abstractmethod
    def fetch_current_prices(self, identifiers: List[str], target_date: date) -> Dict[str, int]:
        """특정 종목들의 해당 날짜 종가를 조회합니다."""
        pass

    @abstractmethod
    def fetch_ohlcv_bulk(self, tickers: List[str], start_date: date, end_date: date) -> Dict[str, Any]:
        """여러 종목의 기간별 OHLCV 데이터를 병렬로 수집합니다."""
        pass

    @abstractmethod
    def fetch_candidates_in_range(self, start_date: date, end_date: date) -> Dict[date, List[Dict[str, Any]]]:
        """기간 내 모든 거래일의 상한가 후보군을 병렬로 수집합니다."""
        pass


class CohortRepository(ABC):
    """코호트 데이터를 저장하고 불러오는 추상 클래스(Port)입니다.
    
    Parquet 기반의 순수 데이터 저장소 역할을 합니다.
    엑셀(Excel) 출력은 ExcelExporter가 담당합니다.
    """

    @abstractmethod
    def save_cohort(self, cohort: 'CeilingCohort') -> None:
        """코호트를 저장합니다. 기존 데이터와 병합하여 덮어쓰지 않습니다.

        Args:
            cohort (CeilingCohort): 저장할 코호트 객체
        """
        pass

    @abstractmethod
    def load_recent_cohorts(self, limit_days: int, base_date: Optional[date] = None) -> List['CeilingCohort']:
        """최근 N일 이내의 코호트들을 불러옵니다.

        Args:
            limit_days (int): 기준일로부터 며칠 전 데이터까지 불러올지
            base_date (Optional[date]): 기준 날짜 (기본값: 오늘)

        Returns:
            List[CeilingCohort]: 코호트 객체 리스트
        """
        pass

    @abstractmethod
    def load_cohorts_in_range(self, start_date: date, end_date: date) -> List['CeilingCohort']:
        """지정된 날짜 범위의 코호트들을 불러옵니다.

        Args:
            start_date (date): 시작 날짜
            end_date (date): 종료 날짜

        Returns:
            List[CeilingCohort]: 코호트 객체 리스트
        """
        pass


class StoragePort(ABC):
    """저장소 추상 인터페이스(Port)입니다.
    
    로컬 파일 시스템, Google Drive 등 다양한 저장소 구현을 위한 인터페이스를 정의합니다.
    """

    @abstractmethod
    def save_dataframe_excel(self, df: pd.DataFrame, path: str, **kwargs) -> bool:
        """DataFrame을 Excel 파일로 저장합니다."""
        pass

    @abstractmethod
    def save_dataframe_csv(self, df: pd.DataFrame, path: str, **kwargs) -> bool:
        """DataFrame을 CSV 파일로 저장합니다."""
        pass

    @abstractmethod
    def save_workbook(self, book: openpyxl.Workbook, path: str) -> bool:
        """openpyxl Workbook을 저장합니다."""
        pass

    @abstractmethod
    def load_workbook(self, path: str) -> Optional[openpyxl.Workbook]:
        """Excel Workbook을 로드합니다."""
        pass

    @abstractmethod
    def path_exists(self, path: str) -> bool:
        """경로가 존재하는지 확인합니다."""
        pass

    @abstractmethod
    def ensure_directory(self, path: str) -> bool:
        """디렉토리가 없으면 생성합니다."""
        pass

    @abstractmethod
    def load_dataframe(self, path: str, sheet_name: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """Excel 파일에서 DataFrame을 로드합니다."""
        pass

    @abstractmethod
    def get_file(self, path: str) -> Optional[bytes]:
        """파일의 내용을 바이트로 읽어옵니다."""
        pass

    @abstractmethod
    def put_file(self, path: str, data: bytes) -> bool:
        """바이트 데이터를 파일로 저장합니다."""
        pass

    @abstractmethod
    def save_parquet(self, df: pd.DataFrame, path: str) -> bool:
        """DataFrame을 Parquet 파일로 저장합니다.

        Args:
            df (pd.DataFrame): 저장할 DataFrame.
            path (str): 저장 경로.

        Returns:
            bool: 성공 여부.
        """
        pass

    @abstractmethod
    def load_parquet(self, path: str) -> pd.DataFrame:
        """Parquet 파일에서 DataFrame을 로드합니다.

        Args:
            path (str): 로드할 파일 경로.

        Returns:
            pd.DataFrame: 로드된 DataFrame. 파일이 없으면 빈 DataFrame 반환.
        """
        pass
