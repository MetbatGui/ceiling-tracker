"""cli.py의 헬퍼 함수 유닛 테스트"""
from unittest.mock import MagicMock

from src.cli import _upload_db_files


def test_upload_db_files_uploads_all_existing_db_files(tmp_path):
    """리포트 조회 범위가 아니라, db_dir에 실제로 있는 모든 *.db 파일을 업로드합니다.

    연도 경계에서는 지난 연도 코호트도 여전히 업데이트될 수 있어서
    (예: 12월 코호트가 1월에도 미완결 추적 중), 리포트 날짜 범위만 보고
    업로드 대상을 정하면 실제로 바뀐 지난 연도 db가 누락될 수 있음.
    """
    (tmp_path / "2020.db").write_bytes(b"dummy-2020")
    (tmp_path / "2025.db").write_bytes(b"dummy-2025")
    (tmp_path / "2026.db").write_bytes(b"dummy-2026")
    (tmp_path / "not-a-year.db").write_bytes(b"ignored")

    storage = MagicMock()
    storage.put_file.return_value = True

    uploaded = _upload_db_files(storage, str(tmp_path))

    assert sorted(uploaded) == ["2020.db", "2025.db", "2026.db"]
    storage.put_file.assert_any_call("db/2020.db", b"dummy-2020")
    storage.put_file.assert_any_call("db/2025.db", b"dummy-2025")
    storage.put_file.assert_any_call("db/2026.db", b"dummy-2026")
    assert storage.put_file.call_count == 3


def test_upload_db_files_returns_empty_when_none_exist(tmp_path):
    storage = MagicMock()

    uploaded = _upload_db_files(storage, str(tmp_path))

    assert uploaded == []
    storage.put_file.assert_not_called()
