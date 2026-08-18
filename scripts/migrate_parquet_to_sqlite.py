"""cohorts.parquet의 모든 코호트를 db/{year}.db로 1회성 이관하는 스크립트.

기존 Parquet 파일은 건드리지 않습니다 (읽기 전용). 이미 SQLite에 있는 데이터와
겹치면 upsert되므로 여러 번 실행해도 안전합니다.

실행:
    uv run python scripts/migrate_parquet_to_sqlite.py
"""
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.infrastructure.storage_adapters import LocalStorageAdapter
from src.infrastructure.repository import ParquetCohortRepository
from src.infrastructure.sqlite_repository import SqliteCohortRepository


def main():
    if sys.platform.startswith('win'):
        sys.stdout.reconfigure(encoding='utf-8')

    local_storage = LocalStorageAdapter(base_path=os.getenv("LOCAL_STORAGE_BASE_PATH", "data"))
    parquet_repo = ParquetCohortRepository(storage=local_storage)
    sqlite_repo = SqliteCohortRepository(db_dir=os.getenv("SQLITE_DB_DIR", "db"))

    cohorts = parquet_repo.load_cohorts_in_range(date(2000, 1, 1), date(2100, 12, 31))
    print(f"[Migrate] Parquet에서 코호트 {len(cohorts)}개 로드")

    sqlite_repo.save_cohorts_batch(cohorts)
    print(f"[Migrate] SQLite로 이관 완료 ({len(cohorts)}개 코호트)")


if __name__ == '__main__':
    main()
