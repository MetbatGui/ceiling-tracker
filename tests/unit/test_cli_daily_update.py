"""cli.py daily-update 커맨드의 실패 시 종료 코드 유닛 테스트.

run-daily.sh(Docker cron)가 `set -e`로 실패를 감지하려면, daily-update가
내부 예외를 삼키고 exit code 0으로 끝나면 안 됩니다.
"""
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

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
