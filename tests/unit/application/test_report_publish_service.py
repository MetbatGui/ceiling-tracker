"""ReportPublishService 유닛 테스트

ExcelExportService/저장소/DB 업로드를 mock/monkeypatch로 격리해 오케스트레이션
로직(로컬 전용 모드, 산출물→DB 업로드 순서, 실패 전파)만 검증합니다.
"""
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from src.application.report_publish_service import PublishResult, ReportPublishService


@pytest.fixture
def repo():
    return MagicMock()


@pytest.fixture
def renderer():
    return MagicMock()


def test_local_only_mode_skips_upload_when_drive_storage_none(repo, renderer, tmp_path):
    """drive_storage가 None이면 로컬에만 저장하고 업로드는 시도하지 않는다."""
    service = ReportPublishService(
        repo=repo, renderer=renderer, drive_storage=None,
        db_dir=str(tmp_path), local_base_path=str(tmp_path),
    )

    with patch("src.application.report_publish_service.ExcelExportService") as mock_export_cls:
        mock_export_cls.return_value.generate_report.return_value = True

        result = service.publish(date(2026, 1, 1), date(2026, 12, 31), "report.xlsx")

    assert result.export_ok is True
    assert result.artifact_upload_failed is False
    assert result.db_upload_failed is False
    assert result.uploaded_db_files == []
    # 로컬 전용 모드에서는 generate_report가 한 번만 호출된다 (드라이브 백업 없음)
    assert mock_export_cls.return_value.generate_report.call_count == 1


def test_drive_mode_uploads_artifact_backup_and_db_files(repo, renderer, tmp_path):
    """§3: 산출물(엑셀) 먼저, DB는 나중 - 둘 다 성공하면 실패 플래그가 안 선다."""
    (tmp_path / "2026.db").write_bytes(b"data")
    drive_storage = MagicMock()
    drive_storage.put_file.return_value = True

    service = ReportPublishService(
        repo=repo, renderer=renderer, drive_storage=drive_storage,
        db_dir=str(tmp_path), local_base_path=str(tmp_path),
    )

    with patch("src.application.report_publish_service.ExcelExportService") as mock_export_cls:
        mock_export_cls.return_value.generate_report.return_value = True

        result = service.publish(date(2026, 1, 1), date(2026, 12, 31), "report.xlsx")

    assert result.export_ok is True
    assert result.artifact_upload_failed is False
    assert result.uploaded_db_files == ["2026.db"]
    assert result.db_upload_failed is False
    # 드라이브 저장 1회 + 로컬 백업 1회 = 2회
    assert mock_export_cls.return_value.generate_report.call_count == 2


def test_export_failure_marks_artifact_upload_failed_and_skips_local_backup(repo, renderer, tmp_path):
    drive_storage = MagicMock()
    service = ReportPublishService(
        repo=repo, renderer=renderer, drive_storage=drive_storage,
        db_dir=str(tmp_path), local_base_path=str(tmp_path),
    )

    with patch("src.application.report_publish_service.ExcelExportService") as mock_export_cls:
        mock_export_cls.return_value.generate_report.return_value = False

        result = service.publish(date(2026, 1, 1), date(2026, 12, 31), "report.xlsx")

    assert result.export_ok is False
    assert result.artifact_upload_failed is True
    # 엑셀 생성 자체가 실패하면 로컬 백업 시도할 의미가 없다 - 드라이브 저장 1회만
    assert mock_export_cls.return_value.generate_report.call_count == 1


def test_db_upload_failure_is_independent_of_artifact_success(repo, renderer, tmp_path):
    """§3: 원격 산출물 업로드가 성공해도 DB 업로드는 독립적으로 시도·판정된다."""
    (tmp_path / "2026.db").write_bytes(b"data")
    drive_storage = MagicMock()
    drive_storage.put_file.return_value = False  # DB 업로드만 실패

    service = ReportPublishService(
        repo=repo, renderer=renderer, drive_storage=drive_storage,
        db_dir=str(tmp_path), local_base_path=str(tmp_path),
    )

    with patch("src.application.report_publish_service.ExcelExportService") as mock_export_cls:
        mock_export_cls.return_value.generate_report.return_value = True

        result = service.publish(date(2026, 1, 1), date(2026, 12, 31), "report.xlsx")

    assert result.export_ok is True
    assert result.artifact_upload_failed is False
    assert result.uploaded_db_files == []
    assert result.db_upload_failed is True
