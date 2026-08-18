"""SqliteCohortRepository 유닛 테스트 (실제 sqlite 파일, tmp_path 사용)"""
from datetime import date
from pathlib import Path

from src.infrastructure.sqlite_repository import SqliteCohortRepository
from src.domain.model import CeilingCohort


def _make_cohort(cohort_date: date, stocks: list) -> CeilingCohort:
    """stocks = [(name, code, price, status)]"""
    cohort = CeilingCohort(cohort_date=cohort_date)
    for name, code, price, status in stocks:
        cohort.add_stock(name, code, price, status)
    return cohort


# ---------------------------------------------------------------------------
# save_cohort / load_cohorts_in_range round-trip
# ---------------------------------------------------------------------------

def test_round_trip_basic(tmp_path):
    repo = SqliteCohortRepository(db_dir=str(tmp_path))
    cohort = _make_cohort(date(2026, 3, 1), [
        ("현대차", "005380", 200000, "역·신"),
        ("기아", "000270", 95000, "52·신"),
    ])
    cohort.stocks[0].add_price(date(2026, 3, 2), 210000)
    cohort.stocks[0].add_price(date(2026, 3, 3), 205000)

    repo.save_cohort(cohort)
    restored = repo.load_cohorts_in_range(date(2026, 3, 1), date(2026, 3, 31))

    assert len(restored) == 1
    assert restored[0].cohort_date == date(2026, 3, 1)
    assert len(restored[0].stocks) == 2
    hyundai = next(s for s in restored[0].stocks if s.stock.name == "현대차")
    assert hyundai.initial_price == 200000
    assert hyundai.new_high_status == "역·신"
    assert hyundai.price_history[date(2026, 3, 2)] == 210000
    assert hyundai.price_history[date(2026, 3, 3)] == 205000


def test_save_cohort_upserts_on_conflict(tmp_path):
    """같은 (cohort_date, stock_code, price_date)로 다시 저장하면 값이 교체됩니다."""
    repo = SqliteCohortRepository(db_dir=str(tmp_path))
    cohort = _make_cohort(date(2026, 1, 5), [("삼성전자", "005930", 80000, "")])
    repo.save_cohort(cohort)

    cohort2 = _make_cohort(date(2026, 1, 5), [("삼성전자", "005930", 81000, "역·신")])
    repo.save_cohort(cohort2)

    restored = repo.load_cohorts_in_range(date(2026, 1, 1), date(2026, 1, 31))
    assert len(restored) == 1
    assert len(restored[0].stocks) == 1
    assert restored[0].stocks[0].initial_price == 81000
    assert restored[0].stocks[0].new_high_status == "역·신"


# ---------------------------------------------------------------------------
# 연도별 파일 분리
# ---------------------------------------------------------------------------

def test_saves_into_year_named_db_files(tmp_path):
    repo = SqliteCohortRepository(db_dir=str(tmp_path))
    repo.save_cohort(_make_cohort(date(2025, 12, 20), [("A", "000001", 1000, "")]))
    repo.save_cohort(_make_cohort(date(2026, 1, 10), [("B", "000002", 2000, "")]))

    assert (Path(tmp_path) / "2025.db").exists()
    assert (Path(tmp_path) / "2026.db").exists()


def test_load_recent_cohorts_crosses_year_boundary(tmp_path):
    """직전 연도 말 코호트도 base_date가 새해 초일 때 조회되어야 합니다."""
    repo = SqliteCohortRepository(db_dir=str(tmp_path))
    cohort = _make_cohort(date(2025, 12, 28), [("A", "000001", 1000, "")])
    cohort.stocks[0].add_price(date(2026, 1, 5), 1100)
    repo.save_cohort(cohort)

    recent = repo.load_recent_cohorts(limit_days=30, base_date=date(2026, 1, 10))

    assert len(recent) == 1
    assert recent[0].cohort_date == date(2025, 12, 28)
    assert recent[0].stocks[0].price_history[date(2026, 1, 5)] == 1100


# ---------------------------------------------------------------------------
# load_incomplete_cohorts
# ---------------------------------------------------------------------------

def test_load_incomplete_cohorts_excludes_completed_and_includes_old(tmp_path):
    repo = SqliteCohortRepository(db_dir=str(tmp_path))

    complete = _make_cohort(date(2025, 1, 2), [("카카오", "035720", 50000, "")])
    for i in range(1, 10):
        complete.stocks[0].add_price(date(2025, 1, 2 + i), 50000 + i)
    repo.save_cohort(complete)

    incomplete = _make_cohort(date(2026, 1, 5), [("삼성전자", "005930", 80000, "")])
    incomplete.stocks[0].add_price(date(2026, 1, 6), 84000)
    repo.save_cohort(incomplete)

    result = repo.load_incomplete_cohorts()
    cohort_dates = {c.cohort_date for c in result}

    assert date(2025, 1, 2) not in cohort_dates
    assert date(2026, 1, 5) in cohort_dates


# ---------------------------------------------------------------------------
# mark_collected / is_date_collected
# ---------------------------------------------------------------------------

def test_mark_collected_then_is_date_collected(tmp_path):
    repo = SqliteCohortRepository(db_dir=str(tmp_path))

    assert repo.is_date_collected(date(2026, 3, 1)) is False
    repo.mark_collected(date(2026, 3, 1), ceiling_count=0)
    assert repo.is_date_collected(date(2026, 3, 1)) is True
    assert repo.is_date_collected(date(2026, 3, 2)) is False


def test_is_date_collected_false_when_no_db_file_yet(tmp_path):
    """해당 연도 db 파일이 아예 없어도 예외 없이 False를 반환해야 합니다."""
    repo = SqliteCohortRepository(db_dir=str(tmp_path))
    assert repo.is_date_collected(date(2030, 1, 1)) is False
    assert not (Path(tmp_path) / "2030.db").exists()  # 읽기 경로는 파일을 생성하지 않음


# ---------------------------------------------------------------------------
# prune_range
# ---------------------------------------------------------------------------

def test_prune_range_removes_stale_price_not_reconfirmed(tmp_path):
    repo = SqliteCohortRepository(db_dir=str(tmp_path))
    cohort = _make_cohort(date(2026, 1, 5), [("삼성전자", "005930", 80000, "")])
    cohort.stocks[0].add_price(date(2026, 5, 1), 0)  # 오염된 값
    repo.save_cohort(cohort)

    rebuilt = _make_cohort(date(2026, 1, 5), [("삼성전자", "005930", 80000, "")])
    rebuilt.stocks[0].add_price(date(2026, 1, 6), 84000)
    repo.save_cohort(rebuilt, prune_range=(date(2026, 1, 1), date(2026, 12, 31)))

    restored = repo.load_cohorts_in_range(date(2026, 1, 1), date(2026, 12, 31))
    assert date(2026, 5, 1) not in restored[0].stocks[0].price_history
    assert restored[0].stocks[0].price_history[date(2026, 1, 6)] == 84000
