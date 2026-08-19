"""SQLite(연도별 분리 파일) 기반 코호트 저장소 구현체입니다.

Parquet 단일 파일 대신 db/{year}.db로 연도별 분리하고, cohort_stocks/
price_history 두 테이블로 정규화해 종목당 정적 값(이름/신고가상태/기준가)이
가격 이력 개수만큼 반복 저장되던 문제를 없앱니다.

파일 분리 기준은 cohort_date의 연도입니다 — 코호트와 그 가격 이력 전체가
항상 같은 파일에 있어서(추적 중 연도가 넘어가도) cross-db 조인이 필요 없습니다.
"""
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from src.domain.model import CeilingCohort
from src.domain.ports import CohortRepository
from src.logger import logger

_SCHEMA = """
CREATE TABLE IF NOT EXISTS cohort_stocks (
    cohort_date TEXT NOT NULL,
    stock_code TEXT NOT NULL,
    stock_name TEXT NOT NULL,
    new_high_status TEXT NOT NULL DEFAULT '',
    initial_price INTEGER NOT NULL,
    PRIMARY KEY (cohort_date, stock_code)
);
CREATE TABLE IF NOT EXISTS price_history (
    cohort_date TEXT NOT NULL,
    stock_code TEXT NOT NULL,
    price_date TEXT NOT NULL,
    price INTEGER NOT NULL,
    PRIMARY KEY (cohort_date, stock_code, price_date)
);
CREATE TABLE IF NOT EXISTS collection_log (
    run_date TEXT PRIMARY KEY,
    ceiling_count INTEGER NOT NULL,
    collected_at TEXT NOT NULL
);
"""


class SqliteCohortRepository(CohortRepository):
    """SQLite(연도별 분리 파일) 기반 상한가 코호트 저장소 구현체."""

    def __init__(self, db_dir: str = "db"):
        self.db_dir = Path(db_dir)
        self.db_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # 연결 헬퍼
    # ------------------------------------------------------------------

    def _db_path(self, year: int) -> Path:
        return self.db_dir / f"{year}.db"

    def _write_connect(self, year: int) -> sqlite3.Connection:
        """쓰기용 연결. 파일/스키마가 없으면 생성합니다."""
        conn = sqlite3.connect(self._db_path(year))
        conn.executescript(_SCHEMA)
        return conn

    def _read_connect(self, year: int) -> Optional[sqlite3.Connection]:
        """읽기용 연결. 파일이 없으면 None (읽기 경로에서 빈 파일을 만들지 않기 위함)."""
        path = self._db_path(year)
        if not path.exists():
            return None
        return sqlite3.connect(path)

    def _existing_years(self) -> List[int]:
        years = []
        for p in self.db_dir.glob("*.db"):
            try:
                years.append(int(p.stem))
            except ValueError:
                continue
        return sorted(years)

    # ------------------------------------------------------------------
    # CohortRepository 인터페이스 구현
    # ------------------------------------------------------------------

    def save_cohort(self, cohort: CeilingCohort,
                     prune_range: Optional[Tuple[date, date]] = None) -> None:
        self.save_cohorts_batch([cohort], prune_range)

    def save_cohorts_batch(self, cohorts: List[CeilingCohort],
                            prune_range: Optional[Tuple[date, date]] = None) -> None:
        """여러 코호트를 저장합니다. 연도 경계를 넘나드는 배치 전체가 원자적입니다 —

        연도별로 파일이 분리되어 SQLite 자체의 단일 트랜잭션으로 묶을 수 없으므로,
        모든 연도의 쓰기를 커밋하지 않은 채 먼저 실행해두고, 전부 성공했을 때만
        순서대로 커밋합니다. 중간에 실패하면 이미 실행된 연도까지 전부 롤백합니다.
        (파일 간 진짜 2단계 커밋은 아니라 마지막 커밋 자체가 실패하는 극단적 경우까지
        막지는 못하지만, 그 경우도 재실행이 upsert라 안전합니다.)
        """
        by_year: Dict[int, List[CeilingCohort]] = {}
        for cohort in cohorts:
            by_year.setdefault(cohort.cohort_date.year, []).append(cohort)

        connections: List[sqlite3.Connection] = []
        try:
            for year, year_cohorts in by_year.items():
                conn = self._write_connect(year)
                connections.append(conn)
                conn.execute("BEGIN")
                for cohort in year_cohorts:
                    self._save_one(conn, cohort, prune_range)
                    logger.info(f"[SqliteRepo] 저장 완료: cohort_date={cohort.cohort_date}, "
                          f"종목수={len(cohort.stocks)}")
        except Exception:
            for conn in connections:
                conn.rollback()
            raise
        else:
            for conn in connections:
                conn.commit()
        finally:
            for conn in connections:
                conn.close()

    def _save_one(self, conn: sqlite3.Connection, cohort: CeilingCohort,
                   prune_range: Optional[Tuple[date, date]]) -> None:
        cd = cohort.cohort_date.isoformat()

        if prune_range:
            p_start, p_end = prune_range
            conn.execute(
                "DELETE FROM price_history WHERE cohort_date = ? AND price_date BETWEEN ? AND ?",
                (cd, p_start.isoformat(), p_end.isoformat()),
            )

        for s in cohort.stocks:
            conn.execute(
                "INSERT INTO cohort_stocks (cohort_date, stock_code, stock_name, new_high_status, initial_price) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(cohort_date, stock_code) DO UPDATE SET "
                "stock_name=excluded.stock_name, new_high_status=excluded.new_high_status, "
                "initial_price=excluded.initial_price",
                (cd, s.stock.code, s.stock.name, s.new_high_status, s.initial_price),
            )
            for price_date, price in s.price_history.items():
                conn.execute(
                    "INSERT INTO price_history (cohort_date, stock_code, price_date, price) "
                    "VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(cohort_date, stock_code, price_date) DO UPDATE SET price=excluded.price",
                    (cd, s.stock.code, price_date.isoformat(), price),
                )

    def load_recent_cohorts(self, limit_days: int,
                            base_date: Optional[date] = None) -> List[CeilingCohort]:
        if base_date is None:
            base_date = date.today()
        cutoff = base_date - timedelta(days=limit_days)
        return self._load_from_years(
            range(cutoff.year, base_date.year + 1),
            "WHERE cs.cohort_date >= ?", (cutoff.isoformat(),),
        )

    def load_cohorts_in_range(self, start_date: date, end_date: date) -> List[CeilingCohort]:
        return self._load_from_years(
            range(start_date.year, end_date.year + 1),
            "WHERE cs.cohort_date BETWEEN ? AND ?",
            (start_date.isoformat(), end_date.isoformat()),
        )

    def load_incomplete_cohorts(self) -> List[CeilingCohort]:
        """추적이 끝나지 않은 종목을 포함한 코호트를 전부 불러옵니다 (연도 무관 전체 조회)."""
        all_cohorts = self._load_from_years(self._existing_years(), "", ())
        return [c for c in all_cohorts if any(not s.is_tracking_complete() for s in c.stocks)]

    def mark_collected(self, run_date: date, ceiling_count: int) -> None:
        conn = self._write_connect(run_date.year)
        try:
            with conn:
                conn.execute(
                    "INSERT INTO collection_log (run_date, ceiling_count, collected_at) VALUES (?, ?, ?) "
                    "ON CONFLICT(run_date) DO UPDATE SET "
                    "ceiling_count=excluded.ceiling_count, collected_at=excluded.collected_at",
                    (run_date.isoformat(), ceiling_count, datetime.now().isoformat()),
                )
        finally:
            conn.close()

    def is_date_collected(self, run_date: date) -> bool:
        conn = self._read_connect(run_date.year)
        if conn is None:
            return False
        try:
            cur = conn.execute("SELECT 1 FROM collection_log WHERE run_date = ?", (run_date.isoformat(),))
            return cur.fetchone() is not None
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # 내부 헬퍼
    # ------------------------------------------------------------------

    def _load_from_years(self, years, where_sql: str, params: tuple) -> List[CeilingCohort]:
        rows = []
        for year in years:
            conn = self._read_connect(year)
            if conn is None:
                continue
            try:
                cur = conn.execute(
                    f"""
                    SELECT cs.cohort_date, cs.stock_code, cs.stock_name, cs.new_high_status, cs.initial_price,
                           ph.price_date, ph.price
                    FROM cohort_stocks cs
                    LEFT JOIN price_history ph
                      ON cs.cohort_date = ph.cohort_date AND cs.stock_code = ph.stock_code
                    {where_sql}
                    """,
                    params,
                )
                rows.extend(cur.fetchall())
            finally:
                conn.close()
        return self._rows_to_cohorts(rows)

    def _rows_to_cohorts(self, rows) -> List[CeilingCohort]:
        cohorts: Dict[date, CeilingCohort] = {}
        for cohort_date_str, stock_code, stock_name, new_high_status, initial_price, price_date_str, price in rows:
            cohort_date = date.fromisoformat(cohort_date_str)
            cohort = cohorts.setdefault(cohort_date, CeilingCohort(cohort_date=cohort_date))

            tracked = next((s for s in cohort.stocks if s.stock.code == stock_code), None)
            if tracked is None:
                cohort.add_stock(stock_name, stock_code, initial_price, new_high_status)
                tracked = cohort.stocks[-1]

            if price_date_str is not None:
                price_date = date.fromisoformat(price_date_str)
                if price_date != cohort_date:
                    tracked.add_price(price_date, price)

        return sorted(cohorts.values(), key=lambda c: c.cohort_date)
