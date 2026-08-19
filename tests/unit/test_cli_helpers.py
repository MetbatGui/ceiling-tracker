"""cli.py의 헬퍼 함수 유닛 테스트"""
from unittest.mock import MagicMock

from src.cli import _upload_db_files


def test_upload_db_files_uploads_only_existing_years(tmp_path):
    (tmp_path / "2025.db").write_bytes(b"dummy-2025")
    (tmp_path / "2026.db").write_bytes(b"dummy-2026")
    # 2027.db는 존재하지 않음

    storage = MagicMock()
    storage.put_file.return_value = True

    uploaded = _upload_db_files(storage, str(tmp_path), start_year=2025, end_year=2027)

    assert uploaded == ["2025.db", "2026.db"]
    storage.put_file.assert_any_call("db/2025.db", b"dummy-2025")
    storage.put_file.assert_any_call("db/2026.db", b"dummy-2026")
    assert storage.put_file.call_count == 2


def test_upload_db_files_returns_empty_when_none_exist(tmp_path):
    storage = MagicMock()

    uploaded = _upload_db_files(storage, str(tmp_path), start_year=2030, end_year=2031)

    assert uploaded == []
    storage.put_file.assert_not_called()
