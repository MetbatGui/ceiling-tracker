"""코호트 데이터를 Excel 리포트로 내보내는 애플리케이션 서비스입니다.

이 모듈은 데이터 저장소에서 정보를 읽어와 시각화 가능한 엑셀 파일로 변환하는 유즈케이스를 처리합니다.
"""
from datetime import date

from src.domain.ports import CohortRepository, StoragePort
from src.infrastructure.calendar_service import CalendarService


class ExcelExportService:
    """Parquet 데이터를 읽어 Excel 리포트를 생성하고 저장하는 Application 서비스.

    수집(DailyUpdateService)과 분리된 전담 리포트 생성 서비스입니다.
    렌더러와 저장소는 생성자에서 주입받습니다.
    """

    def __init__(self,
                 repo: CohortRepository,
                 calendar: CalendarService,
                 renderer,
                 storage: StoragePort) -> None:
        """서비스 초기화.

        Args:
            repo: 코호트 저장소 (Parquet)
            calendar: 거래일 조회용 (KRX 로그인 불필요 — 휴장일 API 역산)
            renderer: Excel 렌더러 (ExcelRenderer)
            storage: 파일 저장소 (local or Drive)
        """
        self.repo = repo
        self.calendar = calendar
        self.renderer = renderer
        self.storage = storage

    def generate_report(self,
                        start_date: date,
                        end_date: date,
                        output_file: str) -> bool:
        """Excel 리포트를 생성하고 저장합니다.

        Args:
            start_date: 조회 시작 날짜
            end_date: 조회 종료 날짜
            output_file: 저장할 파일명

        Returns:
            성공 여부
        """
        cohorts = self.repo.load_cohorts_in_range(start_date, end_date)
        if not cohorts:
            print(f"[ExcelExportService] 데이터 없음: {start_date} ~ {end_date}")
            return False

        print(f"[ExcelExportService] {len(cohorts)}개 코호트 로드 완료.")
        trading_days = self.calendar.get_trading_days(start_date, end_date)
        wb = self.renderer.render(cohorts, trading_days=trading_days, end_date=end_date)

        ok = self.storage.save_workbook(wb, output_file)
        if ok:
            print(f"[ExcelExportService] 저장 완료: {output_file}")
        else:
            print(f"[ExcelExportService] 저장 실패: {output_file}")
        return ok
