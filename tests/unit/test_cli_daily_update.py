"""cli.py daily-update 커맨드의 실패 시 종료 코드 유닛 테스트.

run-daily.sh(Docker cron)가 `set -e`로 실패를 감지하려면, daily-update가
내부 예외를 삼키고 exit code 0으로 끝나면 안 됩니다.
"""
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from src.cli import cli


def test_daily_update_exits_nonzero_on_failure():
    with patch("src.cli._try_build_drive_storage", return_value=None), \
         patch("src.cli._build_repo", return_value=MagicMock()), \
         patch("src.cli.CalendarService"), \
         patch("src.cli.KrxDirectStockInfoAdapter"), \
         patch("src.cli.DailyRoutineService") as mock_routine_cls:
        mock_routine_cls.return_value.run.side_effect = RuntimeError("boom")

        runner = CliRunner()
        result = runner.invoke(cli, ["daily-update"])

        assert result.exit_code != 0


def test_daily_update_exits_nonzero_when_db_download_fails():
    """db_ssot_guide.md §6.1: 원격에 파일은 있는데 다운로드가 실패하면 로컬 상태를
    신뢰할 수 없으므로 수집을 아예 시작하지 말고 중단해야 한다."""
    with patch("src.cli._try_build_drive_storage", return_value=MagicMock()), \
         patch("src.cli._sync_db_down", return_value=False), \
         patch("src.cli._build_repo", return_value=MagicMock()), \
         patch("src.cli.CalendarService"), \
         patch("src.cli.KrxDirectStockInfoAdapter"), \
         patch("src.cli.DailyRoutineService") as mock_routine_cls:
        runner = CliRunner()
        result = runner.invoke(cli, ["daily-update"])

        assert result.exit_code != 0
        mock_routine_cls.return_value.run.assert_not_called()
