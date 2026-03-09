"""코호트 데이터를 영속화하기 위한 저장소 어댑터들을 정의합니다.

이 모듈은 Parquet 기반의 메뉴 저장소와 마이그레이션을 위한 레거시 엑셀 저장소를 포함합니다.
"""
import pandas as pd
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict
from src.domain.ports import CohortRepository, StoragePort
from src.domain.model import CeilingCohort


# ---------------------------------------------------------------------------
# Parquet 기반 코호트 저장소 (메인 DB)
# ---------------------------------------------------------------------------

class ParquetCohortRepository(CohortRepository):
    """Parquet 파일 기반 상한가 코호트 저장소 구현체.

    코호트 데이터를 tidy(long) 포맷의 Parquet 파일로 저장하고 불러옵니다.

    스키마:
        cohort_date  : 코호트 날짜 (상한가 발생일)
        stock_name   : 종목명
        stock_code   : 종목코드
        new_high_status : 신고가 상태
        initial_price: 상한가 당일 종가 (기준가)
        price_date   : 추적 날짜
        price        : 해당 날 종가
    """

    PARQUET_PATH = "cohorts.parquet"

    def __init__(self, storage: StoragePort, parquet_path: str = PARQUET_PATH):
        """저장소를 초기화합니다.

        Args:
            storage: 파일 I/O를 담당하는 StoragePort 구현체
            parquet_path: Parquet 파일 경로 (storage 기준 상대 경로)
        """
        self.storage = storage
        self.parquet_path = parquet_path

    # ------------------------------------------------------------------
    # CohortRepository 인터페이스 구현
    # ------------------------------------------------------------------

    def save_cohort(self, cohort: CeilingCohort) -> None:
        """코호트를 Parquet에 저장합니다. 기존 데이터와 병합합니다."""
        new_df = self._cohort_to_dataframe(cohort)
        if new_df.empty:
            return

        existing_df = self.storage.load_parquet(self.parquet_path)

        if existing_df.empty:
            merged_df = new_df
        else:
            merged_df = self._merge_dataframes(existing_df, new_df)

        self.storage.save_parquet(merged_df, self.parquet_path)
        print(f"[ParquetRepo] 저장 완료: cohort_date={cohort.cohort_date}, "
              f"종목수={len(cohort.stocks)}")

    def save_cohorts_batch(self, cohorts: List[CeilingCohort]) -> None:
        """여러 코호트를 한 번의 parquet I/O로 배치 저장합니다."""
        if not cohorts:
            return

        # 모든 코호트를 하나의 DataFrame으로 변환
        new_dfs = [self._cohort_to_dataframe(c) for c in cohorts]
        new_dfs = [df for df in new_dfs if not df.empty]
        if not new_dfs:
            return

        import pandas as pd
        batch_df = pd.concat(new_dfs, ignore_index=True)

        # parquet 단 한 번 READ
        existing_df = self.storage.load_parquet(self.parquet_path)

        if existing_df.empty:
            merged_df = batch_df
        else:
            merged_df = self._merge_dataframes(existing_df, batch_df)

        # parquet 단 한 번 WRITE
        self.storage.save_parquet(merged_df, self.parquet_path)
        for cohort in cohorts:
            print(f"[ParquetRepo] 저장 완료: cohort_date={cohort.cohort_date}, "
                  f"종목수={len(cohort.stocks)}")

    def load_recent_cohorts(self, limit_days: int,
                            base_date: Optional[date] = None) -> List[CeilingCohort]:
        """최근 N일 이내 코호트를 불러옵니다.

        Args:
            limit_days: 기준일로부터 며칠 전까지 로드할지
            base_date: 기준 날짜 (기본값: 오늘)

        Returns:
            CeilingCohort 리스트
        """
        if base_date is None:
            base_date = date.today()

        cutoff = base_date - timedelta(days=limit_days)
        return self._load_cohorts_where(
            lambda df: df['cohort_date'] >= pd.Timestamp(cutoff)
        )

    def load_cohorts_in_range(self, start_date: date,
                              end_date: date) -> List[CeilingCohort]:
        """날짜 범위의 코호트를 불러옵니다.

        Args:
            start_date: 시작 날짜 (포함)
            end_date: 종료 날짜 (포함)

        Returns:
            CeilingCohort 리스트
        """
        return self._load_cohorts_where(
            lambda df: (df['cohort_date'] >= pd.Timestamp(start_date)) &
                       (df['cohort_date'] <= pd.Timestamp(end_date))
        )

    # ------------------------------------------------------------------
    # 내부 헬퍼
    # ------------------------------------------------------------------

    def _cohort_to_dataframe(self, cohort: CeilingCohort) -> pd.DataFrame:
        """CeilingCohort를 tidy DataFrame으로 변환합니다."""
        rows = []
        for tracked in cohort.stocks:
            # 당일 가격 (initial_price)
            rows.append({
                'cohort_date': cohort.cohort_date,
                'stock_name': tracked.stock.name,
                'stock_code': tracked.stock.code,
                'new_high_status': tracked.new_high_status or '',
                'initial_price': tracked.initial_price,
                'price_date': cohort.cohort_date,
                'price': tracked.initial_price,
            })
            # 추적 가격 히스토리
            for price_date, price in tracked.price_history.items():
                if price_date == cohort.cohort_date:
                    continue  # 당일은 위에서 이미 추가
                rows.append({
                    'cohort_date': cohort.cohort_date,
                    'stock_name': tracked.stock.name,
                    'stock_code': tracked.stock.code,
                    'new_high_status': tracked.new_high_status or '',
                    'initial_price': tracked.initial_price,
                    'price_date': price_date,
                    'price': price,
                })

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df['cohort_date'] = pd.to_datetime(df['cohort_date'])
        df['price_date'] = pd.to_datetime(df['price_date'])
        return df

    def _merge_dataframes(self, existing: pd.DataFrame,
                          new: pd.DataFrame) -> pd.DataFrame:
        """기존 DataFrame과 신규 DataFrame을 병합합니다.

        동일한 (cohort_date, stock_code, price_date) 조합은 새 데이터로 교체합니다.
        """
        key_cols = ['cohort_date', 'stock_code', 'price_date']

        # 기존 데이터에서 새 데이터와 겹치는 행 제거
        new_keys = new[key_cols]
        mask = existing.set_index(key_cols).index.isin(
            new_keys.set_index(key_cols).index
        )
        existing_filtered = existing[~mask]

        merged = pd.concat([existing_filtered, new], ignore_index=True)
        merged.sort_values(['cohort_date', 'stock_code', 'price_date'], inplace=True)
        merged.reset_index(drop=True, inplace=True)
        return merged

    def _load_cohorts_where(self, condition_fn) -> List[CeilingCohort]:
        """Parquet을 로드하고 조건 함수로 필터링한 뒤 CeilingCohort 리스트로 변환합니다."""
        df = self.storage.load_parquet(self.parquet_path)
        if df.empty:
            return []

        # 날짜 타입 보장
        df['cohort_date'] = pd.to_datetime(df['cohort_date'])
        df['price_date'] = pd.to_datetime(df['price_date'])

        filtered = df[condition_fn(df)]
        if filtered.empty:
            return []

        return self._dataframe_to_cohorts(filtered)

    def _dataframe_to_cohorts(self, df: pd.DataFrame) -> List[CeilingCohort]:
        """Tidy DataFrame을 CeilingCohort 리스트로 복원합니다."""
        cohorts: Dict[date, CeilingCohort] = {}

        for cohort_date_val, cohort_df in df.groupby('cohort_date'):
            cohort_date = pd.to_datetime(str(cohort_date_val)).date()
            cohort = CeilingCohort(cohort_date=cohort_date)

            for stock_code_val, stock_df in cohort_df.groupby('stock_code'):
                first_row = stock_df.iloc[0]
                stock_name = str(first_row['stock_name'])
                stock_code = str(stock_code_val)
                new_high_status = str(first_row['new_high_status'])
                initial_price = int(first_row['initial_price'])

                cohort.add_stock(stock_name, stock_code, initial_price, new_high_status)
                tracked = cohort.stocks[-1]

                # 가격 히스토리 복원
                for _, row in stock_df.iterrows():
                    price_date = row['price_date'].date()
                    price = int(row['price'])
                    if price_date != cohort_date:
                        tracked.add_price(price_date, price)

            cohorts[cohort_date] = cohort

        return sorted(cohorts.values(), key=lambda c: c.cohort_date)


# ---------------------------------------------------------------------------
# 레거시: 엑셀 기반 코호트 저장소 (마이그레이션 스크립트 전용)
# ---------------------------------------------------------------------------

class ExcelCohortRepository:
    """엑셀 파일 기반 상한가 코호트 저장소 구현체 (레거시, 마이그레이션 전용).

    기존 엑셀 파일에서 코호트 데이터를 읽어서 Parquet으로 이전하는 용도입니다.
    신규 데이터 수집에는 사용하지 않습니다.
    """

    def __init__(self, file_path: str):
        """저장소를 초기화합니다.

        Args:
            file_path: 레거시 엑셀 파일 경로.
        """
        self.file_path = file_path

    def load_all_cohorts(self, storage: StoragePort) -> List[CeilingCohort]:
        """엑셀 파일의 모든 코호트를 읽어옵니다."""
        wb = self._load_workbook_safely(storage, self.file_path)
        if wb is None:
            return []

        cohorts = []
        for sheet_name in wb.sheetnames:
            cohort = self._load_cohort_from_sheet(wb, sheet_name)
            if cohort:
                cohorts.append(cohort)
        return cohorts

    def _load_workbook_safely(self, storage, file_path: str):
        if not storage.path_exists(file_path):
            return None
        try:
            return storage.load_workbook(file_path)
        except Exception as e:
            print(f"[ExcelRepo] 엑셀 읽기 실패: {e}")
            return None

    def _load_cohort_from_sheet(self, wb, sheet_name: str) -> Optional[CeilingCohort]:
        cohort_date = self._parse_sheet_date(sheet_name)
        if cohort_date is None:
            return None

        cohort = CeilingCohort(cohort_date=cohort_date)
        ws = wb[sheet_name]
        
        # 헤더 트리밍 (양끝 공백 제거)
        headers = [str(cell.value).strip() if cell.value is not None else None for cell in ws[1]]

        for row in ws.iter_rows(min_row=2, values_only=True):
            if not row or not any(row):
                continue
            try:
                self._add_stock_from_row(cohort, headers, row, sheet_name)
            except Exception as e:
                print(f"[ExcelRepo] [Warning] 행 로드 실패 ({sheet_name}): {e}")
                continue

        return cohort

    def _parse_sheet_date(self, sheet_name: str) -> Optional[date]:
        try:
            return datetime.strptime(sheet_name, "%y%m%d").date()
        except ValueError:
            return None

    def _add_stock_from_row(self, cohort: CeilingCohort, headers: List,
                            row: tuple, sheet_name: str) -> None:
        import pandas as pd
        row_dict = dict(zip(headers, row))
        name = str(row_dict.get('종목명', '')).strip()
        if not name or pd.isna(name) or name == 'None':
            return

        new_high_status = ''
        if '신고가' in row_dict and row_dict['신고가'] and not pd.isna(row_dict['신고가']):
            new_high_status = str(row_dict['신고가']).strip()

        # 시트 이름과 일치하는 컬럼(당일 가격) 찾기 (공백 제거 후 비교)
        initial_price = 0
        target_col = sheet_name
        
        # row_dict의 키들도 트리밍해서 비교
        trimmed_row_dict = {str(k).strip(): v for k, v in row_dict.items()}
        
        if target_col in trimmed_row_dict and trimmed_row_dict[target_col] is not None:
            try:
                val = str(trimmed_row_dict[target_col]).replace(',', '').strip()
                initial_price = int(float(val))
            except (ValueError, TypeError):
                initial_price = 0

        cohort.add_stock(name, '', initial_price, new_high_status)
        tracked = cohort.stocks[-1]

        for col_name, col_value in trimmed_row_dict.items():
            if col_value is None or pd.isna(col_value):
                continue
            try:
                # '260102' 형식의 날짜 컬럼인지 확인
                col_date = datetime.strptime(str(col_name), "%y%m%d").date()
                val = str(col_value).replace(',', '').strip()
                price = int(float(val))
                if col_date != cohort.cohort_date:
                    tracked.add_price(col_date, price)
            except (ValueError, TypeError):
                continue
