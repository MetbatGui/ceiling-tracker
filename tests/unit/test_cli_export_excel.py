"""cli.py export-excel 커맨드의 실패 시 종료 코드 유닛 테스트.

run-daily.sh(Docker cron)가 `set -e`로 리포트 생성 실패를 감지하려면,
export-excel이 실패(ok=False)했을 때 exit code 0으로 끝나면 안 됩니다.
"""
import os
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from src.cli import cli
from src.infrastructure.storage_adapters import GoogleDriveAdapter


def test_export_excel_exits_nonzero_when_generate_report_fails():
    with patch("src.cli._build_storage", return_value=MagicMock()), \
         patch("src.cli._build_repo", return_value=MagicMock()), \
         patch("src.cli.CalendarService"), \
         patch("src.cli.ExcelExportService") as mock_service_cls:
        mock_service_cls.return_value.generate_report.return_value = False

        runner = CliRunner()
        result = runner.invoke(cli, ["export-excel", "--year", "2026"])

        assert result.exit_code != 0


def test_export_excel_exits_nonzero_when_db_upload_fails(tmp_path):
    """db_ssot_guide.md §6.2: 업로드 실패를 조용히 넘기면 다음 실행의 무조건
    다운로드가 최신 로컬 데이터를 낡은 원격 사본으로 덮어써 지울 수 있다."""
    (tmp_path / "2026.db").write_bytes(b"data")

    drive_storage = MagicMock(spec=GoogleDriveAdapter)
    drive_storage.put_file.return_value = False

    with patch("src.cli._build_storage", return_value=drive_storage), \
         patch("src.cli._build_repo", return_value=MagicMock()), \
         patch("src.cli.CalendarService"), \
         patch("src.cli.LocalStorageAdapter", return_value=MagicMock()), \
         patch("src.cli.ExcelExportService") as mock_service_cls, \
         patch.dict(os.environ, {"SQLITE_DB_DIR": str(tmp_path)}):
        mock_service_cls.return_value.generate_report.return_value = True

        runner = CliRunner()
        result = runner.invoke(cli, ["export-excel", "--year", "2026", "--drive"])

        assert result.exit_code != 0
