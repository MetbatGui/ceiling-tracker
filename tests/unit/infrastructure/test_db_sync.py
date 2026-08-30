"""src/infrastructure/db_sync.py 유닛 테스트.

GDrive DB 세션 동기화(다운로드/업로드) 로직 자체를 검증한다. 다운로드/업로드
전체 흐름의 "누가 언제 부르는가"는 DailyRoutineService/RangeRoutineService
테스트에서 검증한다.
"""
import os
from unittest.mock import MagicMock, patch

from src.infrastructure.db_sync import (
    list_db_files_present,
    sync_db_down,
    try_build_drive_storage,
    upload_db_files,
)


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

    uploaded = upload_db_files(storage, str(tmp_path))

    assert sorted(uploaded) == ["2020.db", "2025.db", "2026.db"]
    storage.put_file.assert_any_call("db/2020.db", b"dummy-2020")
    storage.put_file.assert_any_call("db/2025.db", b"dummy-2025")
    storage.put_file.assert_any_call("db/2026.db", b"dummy-2026")
    assert storage.put_file.call_count == 3


def test_upload_db_files_returns_empty_when_none_exist(tmp_path):
    storage = MagicMock()

    uploaded = upload_db_files(storage, str(tmp_path))

    assert uploaded == []
    storage.put_file.assert_not_called()


def test_upload_db_files_swallows_exception_and_skips_that_file(tmp_path):
    """StoragePort 계약(§6): 실패는 삼켜서 bool/list로만 알린다 - 예외가 그대로
    새면 호출부(오케스트레이션 서비스)가 트레이스백과 함께 죽는다."""
    (tmp_path / "2026.db").write_bytes(b"data")
    storage = MagicMock()
    storage.put_file.side_effect = RuntimeError("network down")

    uploaded = upload_db_files(storage, str(tmp_path))

    assert uploaded == []


def test_list_db_files_present_ignores_non_year_files(tmp_path):
    (tmp_path / "2026.db").write_bytes(b"x")
    (tmp_path / "backup.db").write_bytes(b"x")

    assert list_db_files_present(str(tmp_path)) == ["2026.db"]


def test_sync_db_down_skips_years_not_yet_on_remote(tmp_path):
    """원격에 아직 없는 연도는 실패가 아니라 정상(최초 수집 전)이다 (db_ssot_guide.md §6.1)."""
    storage = MagicMock()
    storage.path_exists.return_value = False

    ok = sync_db_down(storage, str(tmp_path), [2026])

    assert ok is True
    storage.get_file.assert_not_called()
    assert not (tmp_path / "2026.db").exists()


def test_sync_db_down_overwrites_local_even_if_already_present(tmp_path):
    """로컬에 이미 파일이 있어도 항상 원격에서 다시 받아 덮어쓴다 (§6.2 "로컬 불신")."""
    (tmp_path / "2026.db").write_bytes(b"stale-local")
    storage = MagicMock()
    storage.path_exists.return_value = True
    storage.get_file.return_value = b"fresh-remote"

    ok = sync_db_down(storage, str(tmp_path), [2026])

    assert ok is True
    assert (tmp_path / "2026.db").read_bytes() == b"fresh-remote"


def test_sync_db_down_writes_atomically_via_tmp_file(tmp_path):
    """§7: 임시파일 + os.replace()로 원자적 교체 - 다운로드 도중 죽어도 최종
    경로의 기존 파일이 손상되지 않아야 한다."""
    storage = MagicMock()
    storage.path_exists.return_value = True
    storage.get_file.return_value = b"fresh-remote"

    sync_db_down(storage, str(tmp_path), [2026])

    assert not (tmp_path / "2026.db.tmp").exists()
    assert (tmp_path / "2026.db").read_bytes() == b"fresh-remote"


def test_sync_db_down_fails_closed_when_remote_exists_but_download_fails(tmp_path):
    """원격에 파일이 있는 게 확인됐는데 다운로드가 실패하면 False - "없음"과 구분한다."""
    storage = MagicMock()
    storage.path_exists.return_value = True
    storage.get_file.return_value = None

    ok = sync_db_down(storage, str(tmp_path), [2020, 2026])

    assert ok is False


def test_sync_db_down_swallows_storage_exceptions_as_failure(tmp_path):
    """StoragePort 계약을 어기고 예외를 던지는 어댑터가 있어도 트레이스백 없이
    False로 변환해야 호출부가 일관되게 fail-closed로 처리할 수 있다."""
    storage = MagicMock()
    storage.path_exists.side_effect = RuntimeError("network down")

    ok = sync_db_down(storage, str(tmp_path), [2026])

    assert ok is False


def test_try_build_drive_storage_returns_none_when_not_configured():
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("GOOGLE_DRIVE_ROOT_FOLDER_ID", None)
        assert try_build_drive_storage() is None


def test_try_build_drive_storage_builds_when_configured():
    with patch.dict(os.environ, {"GOOGLE_DRIVE_ROOT_FOLDER_ID": "some-folder-id"}), \
         patch("src.infrastructure.db_sync.build_storage", return_value=MagicMock()) as mock_build:
        storage = try_build_drive_storage()

        assert storage is not None
        mock_build.assert_called_once_with(True)
