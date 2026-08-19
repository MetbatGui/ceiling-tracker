"""구글 드라이브에 남아있는 2020~2025년 레거시 엑셀 리포트를 db/{year}.db로
1회성 이관하는 스크립트.

레거시 엑셀에는 종목코드가 없어 ExcelCohortRepository가 종목명을 대체
식별자로 저장합니다. Drive 파일은 건드리지 않습니다 (읽기 전용). 이미
SQLite에 있는 데이터와 겹치면 upsert되므로 여러 번 실행해도 안전합니다.

실행:
    uv run python scripts/migrate_excel_to_sqlite.py --years 2020-2025
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

from src.infrastructure.repository import ExcelCohortRepository
from src.infrastructure.storage_adapters import GoogleDriveAdapter
from src.infrastructure.sqlite_repository import SqliteCohortRepository


def parse_years(spec: str) -> range:
    start, end = spec.split('-')
    return range(int(start), int(end) + 1)


def main():
    if sys.platform.startswith('win'):
        sys.stdout.reconfigure(encoding='utf-8')

    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument('--years', default='2020-2025', help='예: 2020-2025')
    args = parser.parse_args()

    drive = GoogleDriveAdapter(
        token_file=os.getenv("GOOGLE_DRIVE_TOKEN_FILE", "secrets/token.json"),
        root_folder_id=os.getenv("GOOGLE_DRIVE_ROOT_FOLDER_ID"),
        client_secret_file=os.getenv("GOOGLE_DRIVE_CLIENT_SECRET_FILE"),
    )
    sqlite_repo = SqliteCohortRepository(db_dir=os.getenv("SQLITE_DB_DIR", "db"))

    for year in parse_years(args.years):
        filename = f"상한가분석({year}년).xlsx"
        legacy_repo = ExcelCohortRepository(file_path=filename)
        cohorts = legacy_repo.load_all_cohorts(drive)
        if not cohorts:
            print(f"[Migrate] {year}: '{filename}' 없음 또는 코호트 0개 - 스킵")
            continue

        sqlite_repo.save_cohorts_batch(cohorts)
        stock_count = sum(len(c.stocks) for c in cohorts)
        print(f"[Migrate] {year}: 코호트 {len(cohorts)}개, 종목수 {stock_count} -> db/{year}.db")


if __name__ == '__main__':
    main()
