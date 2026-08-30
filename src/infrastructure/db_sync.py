"""GDrive DB 세션 동기화 + 저장소 조립 헬퍼 (db_ssot_guide.md §6/§6.1/§6.2).

오케스트레이션 서비스(DailyRoutineService, RangeRoutineService)가 다운로드/업로드
전체 흐름을 소유하도록, CLI가 아니라 이 모듈의 순수 함수를 서비스가 직접 호출한다
(orchestration_guide.md §1/§4).
"""
import os
from pathlib import Path
from typing import Iterable, List, Optional

from src.domain.ports import StoragePort
from src.infrastructure.storage_adapters import GoogleDriveAdapter, LocalStorageAdapter
from src.logger import logger


def build_storage(use_drive: bool) -> StoragePort:
    """Storage 인스턴스를 생성합니다."""
    if use_drive:
        token_file = os.getenv("GOOGLE_DRIVE_TOKEN_FILE", "secrets/token.json")
        client_secret = os.getenv("GOOGLE_DRIVE_CLIENT_SECRET_FILE", "secrets/client_secret.json")
        folder_id = os.getenv("GOOGLE_DRIVE_ROOT_FOLDER_ID")

        if not folder_id:
            raise ValueError("GOOGLE_DRIVE_ROOT_FOLDER_ID 환경변수가 설정되지 않았습니다.")

        return GoogleDriveAdapter(
            token_file=token_file,
            root_folder_id=folder_id,
            client_secret_file=client_secret,
        )
    base_path = os.getenv("LOCAL_STORAGE_BASE_PATH", "data")
    return LocalStorageAdapter(base_path=base_path)


def try_build_drive_storage() -> Optional[StoragePort]:
    """GOOGLE_DRIVE_ROOT_FOLDER_ID가 설정돼 있을 때만 Drive storage를 만든다.

    설정 자체가 없으면(로컬 전용 개발 환경 등) None을 반환해 다운로드 단계를
    건너뛴다 - 이건 "다운로드 실패"가 아니라 "Drive 미사용"이라 구분해야 한다
    (db_ssot_guide.md §6.1).
    """
    if not os.getenv("GOOGLE_DRIVE_ROOT_FOLDER_ID"):
        return None
    return build_storage(True)


def sync_db_down(storage: StoragePort, db_dir: str, years: Iterable[int]) -> bool:
    """GDrive에 있는 {year}.db를 로컬로 받아 덮어쓴다 (db_ssot_guide.md §6.1/§6.2).

    로컬에 이미 파일이 있어도 항상 다시 받는다("로컬은 항상 불신의 대상"). 원격에
    파일이 있는 게 확인됐는데 다운로드가 실패하면 "없음"과 구분해 실패로 보고한다 -
    빈/낡은 로컬로 계속 진행하다 그대로 재업로드하면 기존 데이터가 사라진다.

    StoragePort 계약(§6)은 실패를 삼켜 Optional/bool로 반환해야 하지만, 이를
    지키지 않는 어댑터 메서드가 예외를 던져도 여기서 잡아 False로 변환한다 -
    호출부(오케스트레이션 서비스)가 트레이스백 없이 일관되게 bool만 보고
    fail-closed로 세션을 중단할 수 있게 하기 위함.

    쓰기는 임시 파일 + os.replace()로 원자적으로 교체한다(§7) - 다운로드 도중
    프로세스가 죽어도 최종 경로의 기존 DB 파일이 손상되지 않는다.

    Returns:
        bool: 대상 연도 전부가 정상(원격에 없거나 다운로드 성공)이면 True, 원격에
            파일이 있는데 다운로드가 실패한 연도가 하나라도 있으면 False.
    """
    db_path = Path(db_dir)
    db_path.mkdir(parents=True, exist_ok=True)
    ok = True
    for year in sorted(set(years)):
        remote_path = f"db/{year}.db"
        try:
            exists = storage.path_exists(remote_path)
        except Exception as e:
            logger.error(f"[DbSync] {year}.db 원격 존재 확인 중 오류: {e}")
            ok = False
            continue
        if not exists:
            continue  # 원격에 아직 없음 - 최초 수집 전이라 정상
        try:
            data = storage.get_file(remote_path)
        except Exception as e:
            logger.error(f"[DbSync] {year}.db 다운로드 중 오류: {e}")
            ok = False
            continue
        if data is None:
            logger.error(f"[DbSync] {year}.db 다운로드 실패 - 원격에 파일은 있는데 읽지 못했습니다.")
            ok = False
            continue
        final_path = db_path / f"{year}.db"
        tmp_path = final_path.with_suffix(".db.tmp")
        tmp_path.write_bytes(data)
        tmp_path.replace(final_path)
    return ok


def upload_db_files(storage: StoragePort, db_dir: str) -> List[str]:
    """db_dir에 있는 모든 {year}.db 파일을 storage에 업로드한다.

    리포트 조회 날짜 범위가 아니라 로컬에 실제 존재하는 파일 전체를 대상으로
    한다 - 연도 경계에서는 지난 연도 코호트도 여전히 업데이트될 수 있어서
    (예: 12월 코호트가 1월에도 미완결 추적 중), 날짜 범위만 보고 업로드
    대상을 정하면 실제로 바뀐 지난 연도 db가 누락될 수 있다.

    Returns:
        업로드된 파일명(예: "2026.db") 리스트.
    """
    uploaded = []
    for local_path in sorted(Path(db_dir).glob("*.db")):
        if not local_path.stem.isdigit():
            continue
        try:
            with open(local_path, "rb") as f:
                data = f.read()
            if storage.put_file(f"db/{local_path.name}", data):
                uploaded.append(local_path.name)
        except Exception as e:
            logger.error(f"[DbSync] {local_path.name} 업로드 중 오류: {e}")
    return uploaded


def list_db_files_present(db_dir: str) -> List[str]:
    """db_dir에 실제 존재하는 {year}.db 파일명 목록을 반환한다 (업로드 성공/실패 대조용)."""
    return [p.name for p in sorted(Path(db_dir).glob("*.db")) if p.stem.isdigit()]
