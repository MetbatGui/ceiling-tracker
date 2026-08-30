"""cli.py daily-update 커맨드의 실패 시 종료 코드 유닛 테스트.

run-daily.sh(Docker cron)가 `set -e`로 실패를 감지하려면, daily-update가
내부 예외를 삼키고 exit code 0으로 끝나면 안 됩니다. DB 다운로드/export/업로드는
이제 DailyRoutineService가 소유하므로(orchestration_guide.md §1/§3), CLI 테스트는
그 서비스가 반환하는 결과 값 객체를 exit code로 올바르게 변환하는지만 검증한다 -
다운로드/업로드 자체의 세부 동작은 test_daily_routine_service.py에서 검증한다.
"""
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from src.application.daily_routine_service import DailyRunResult
from src.application.report_publish_service import PublishResult
from src.cli import cli


def test_daily_update_exits_nonzero_on_failure():
    with patch("src.cli._build_repo", return_value=MagicMock()), \
         patch("src.cli.CalendarService"), \
         patch("src.cli.KrxDirectStockInfoAdapter"), \
         patch("src.cli.DailyRoutineService") as mock_routine_cls:
        mock_routine_cls.return_value.run.side_effect = RuntimeError("boom")

        runner = CliRunner()
        result = runner.invoke(cli, ["daily-update"])

        assert result.exit_code != 0


def test_daily_update_exits_nonzero_when_db_download_fails():
    """db_ssot_guide.md §6.1: 서비스가 download_failed=True를 반환하면 CLI는
    0이 아닌 exit code로 끝나야 한다."""
    with patch("src.cli._build_repo", return_value=MagicMock()), \
         patch("src.cli.CalendarService"), \
         patch("src.cli.KrxDirectStockInfoAdapter"), \
         patch("src.cli.DailyRoutineService") as mock_routine_cls:
        mock_routine_cls.return_value.run.return_value = DailyRunResult(
            skipped=False, download_failed=True
        )

        runner = CliRunner()
        result = runner.invoke(cli, ["daily-update"])

        assert result.exit_code != 0


def test_daily_update_exits_nonzero_when_db_upload_fails():
    with patch("src.cli._build_repo", return_value=MagicMock()), \
         patch("src.cli.CalendarService"), \
         patch("src.cli.KrxDirectStockInfoAdapter"), \
         patch("src.cli.DailyRoutineService") as mock_routine_cls:
        mock_routine_cls.return_value.run.return_value = DailyRunResult(
            skipped=False,
            publish=PublishResult(export_ok=True, db_upload_failed=True),
        )

        runner = CliRunner()
        result = runner.invoke(cli, ["daily-update"])

        assert result.exit_code != 0


def test_daily_update_exits_zero_on_full_success():
    with patch("src.cli._build_repo", return_value=MagicMock()), \
         patch("src.cli.CalendarService"), \
         patch("src.cli.KrxDirectStockInfoAdapter"), \
         patch("src.cli.DailyRoutineService") as mock_routine_cls:
        mock_routine_cls.return_value.run.return_value = DailyRunResult(
            skipped=False,
            publish=PublishResult(export_ok=True, uploaded_db_files=["2026.db"]),
        )

        runner = CliRunner()
        result = runner.invoke(cli, ["daily-update"])

        assert result.exit_code == 0


def test_daily_update_skipped_on_holiday_exits_zero():
    with patch("src.cli._build_repo", return_value=MagicMock()), \
         patch("src.cli.CalendarService"), \
         patch("src.cli.KrxDirectStockInfoAdapter"), \
         patch("src.cli.DailyRoutineService") as mock_routine_cls:
        mock_routine_cls.return_value.run.return_value = DailyRunResult(skipped=True, reason="holiday")

        runner = CliRunner()
        result = runner.invoke(cli, ["daily-update"])

        assert result.exit_code == 0
