"""cli.py export-excel 커맨드의 실패 시 종료 코드 유닛 테스트.

run-daily.sh(Docker cron)가 `set -e`로 리포트 생성 실패를 감지하려면,
export-excel이 실패(ok=False)했을 때 exit code 0으로 끝나면 안 됩니다.
"""
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from src.cli import cli


def test_export_excel_exits_nonzero_when_generate_report_fails():
    with patch("src.cli._build_storage", return_value=MagicMock()), \
         patch("src.cli._build_repo", return_value=MagicMock()), \
         patch("src.cli.CalendarService"), \
         patch("src.cli.ExcelExportService") as mock_service_cls:
        mock_service_cls.return_value.generate_report.return_value = False

        runner = CliRunner()
        result = runner.invoke(cli, ["export-excel", "--year", "2026"])

        assert result.exit_code != 0
