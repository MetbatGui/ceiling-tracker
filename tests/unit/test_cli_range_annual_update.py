"""cli.py range-update/annual-update 커맨드의 실패 시 종료 코드 유닛 테스트.

daily-update/export-excel과 달리 이 두 커맨드는 실패해도 예외를 삼키거나
그냥 return해서 exit code 0으로 끝나던 문제가 있었다 (docker_guide.md §10).
"""
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from src.cli import cli


def test_range_update_exits_nonzero_on_failure():
    with patch("src.cli._try_build_drive_storage", return_value=None), \
         patch("src.cli._build_repo", return_value=MagicMock()), \
         patch("src.cli.KrxDirectStockInfoAdapter"), \
         patch("src.cli.RangeRoutineService") as mock_routine_cls:
        mock_routine_cls.return_value.run_range.side_effect = RuntimeError("boom")

        runner = CliRunner()
        result = runner.invoke(cli, ["range-update", "--start", "2026-01-01", "--end", "2026-01-05"])

        assert result.exit_code != 0


def test_range_update_exits_nonzero_when_db_download_fails():
    with patch("src.cli._try_build_drive_storage", return_value=MagicMock()), \
         patch("src.cli._sync_db_down", return_value=False), \
         patch("src.cli._build_repo", return_value=MagicMock()), \
         patch("src.cli.KrxDirectStockInfoAdapter"), \
         patch("src.cli.RangeRoutineService") as mock_routine_cls:
        runner = CliRunner()
        result = runner.invoke(cli, ["range-update", "--start", "2026-01-01", "--end", "2026-01-05"])

        assert result.exit_code != 0
        mock_routine_cls.return_value.run_range.assert_not_called()


def test_annual_update_exits_nonzero_when_a_year_fails():
    with patch("src.cli._try_build_drive_storage", return_value=None), \
         patch("src.cli._build_repo", return_value=MagicMock()), \
         patch("src.cli.KrxDirectStockInfoAdapter"), \
         patch("src.cli.RangeRoutineService") as mock_routine_cls:
        result_obj = MagicMock()
        result_obj.completed_years = [2024]
        result_obj.failed_year = 2025
        result_obj.error = "boom"
        mock_routine_cls.return_value.run_annual.return_value = result_obj

        runner = CliRunner()
        result = runner.invoke(cli, ["annual-update", "--start-year", "2024", "--end-year", "2025"])

        assert result.exit_code != 0


def test_annual_update_exits_nonzero_when_db_download_fails():
    with patch("src.cli._try_build_drive_storage", return_value=MagicMock()), \
         patch("src.cli._sync_db_down", return_value=False), \
         patch("src.cli._build_repo", return_value=MagicMock()), \
         patch("src.cli.KrxDirectStockInfoAdapter"), \
         patch("src.cli.RangeRoutineService") as mock_routine_cls:
        runner = CliRunner()
        result = runner.invoke(cli, ["annual-update", "--start-year", "2024", "--end-year", "2025"])

        assert result.exit_code != 0
        mock_routine_cls.return_value.run_annual.assert_not_called()
