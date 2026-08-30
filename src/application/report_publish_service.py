"""export(엑셀 렌더링) + 산출물/DB 업로드를 담당하는 오케스트레이션 하위 서비스.

daily 백필이 끝난 뒤 "export도 오케스트레이션 흐름 안에 있어야 한다"는
orchestration_guide.md §1 요구를 만족시키기 위해 DailyRoutineService가 쓴다.
업로드 순서는 §3: 산출물(사람이 바로 보는 결과) 먼저, DB는 나중, 각각
독립적으로 시도하고 실패는 결과 값 객체에 담는다.
"""
from dataclasses import dataclass, field
from datetime import date
from typing import List, Optional

from src.application.excel_export_service import ExcelExportService
from src.domain.ports import StoragePort
from src.infrastructure.calendar_service import CalendarService
from src.infrastructure.db_sync import list_db_files_present, upload_db_files
from src.infrastructure.storage_adapters import LocalStorageAdapter
from src.logger import logger


@dataclass
class PublishResult:
    """ReportPublishService.publish()의 실행 결과."""
    export_ok: bool = False
    artifact_upload_failed: bool = False
    db_upload_failed: bool = False
    uploaded_db_files: List[str] = field(default_factory=list)


class ReportPublishService:
    """엑셀 리포트 렌더링 + (Drive 사용 시) 산출물 백업/업로드 + DB 업로드를
    한 흐름으로 실행한다.

    drive_storage가 None이면(로컬 전용 모드) 로컬에만 저장하고 업로드는
    하지 않는다.
    """

    def __init__(
        self,
        repo,
        renderer,
        drive_storage: Optional[StoragePort],
        db_dir: str,
        local_base_path: str = "data",
        calendar: Optional[CalendarService] = None,
    ):
        self.repo = repo
        self.calendar = calendar or CalendarService()
        self.renderer = renderer
        self.drive_storage = drive_storage
        self.db_dir = db_dir
        self.local_base_path = local_base_path

    def publish(self, start_date: date, end_date: date, output_file: str) -> PublishResult:
        result = PublishResult()

        primary_storage = self.drive_storage or LocalStorageAdapter(base_path=self.local_base_path)
        service = ExcelExportService(self.repo, self.calendar, self.renderer, primary_storage)
        result.export_ok = service.generate_report(start_date, end_date, output_file)

        if self.drive_storage is None:
            return result

        if result.export_ok:
            local_service = ExcelExportService(
                self.repo, self.calendar, self.renderer,
                LocalStorageAdapter(base_path=self.local_base_path),
            )
            if not local_service.generate_report(start_date, end_date, output_file):
                logger.error("[ReportPublishService] 로컬 백업 저장 실패")
                result.artifact_upload_failed = True
        else:
            result.artifact_upload_failed = True

        db_files_present = set(list_db_files_present(self.db_dir))
        uploaded = upload_db_files(self.drive_storage, self.db_dir)
        result.uploaded_db_files = uploaded
        if db_files_present - set(uploaded):
            result.db_upload_failed = True

        return result
