"""ExcelCohortRepository(레거시 마이그레이션 전용) 유닛 테스트"""
from datetime import date

import openpyxl

from src.infrastructure.repository import ExcelCohortRepository
from src.infrastructure.storage_adapters import LocalStorageAdapter


def _make_legacy_workbook(tmp_path, filename="legacy.xlsx"):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "200102"
    ws.append(["종목명", "신고가", "200102", "200103", "등락률"])
    ws.append(["써니전자", "52·신", 5000, 5660, "13.2%"])
    ws.append(["다믈멀티미디어", None, 4705, 4975, "5.7%"])
    wb.save(tmp_path / filename)
    return filename


def test_load_all_cohorts_assigns_unique_stock_code_per_name(tmp_path):
    """실제 종목코드가 없는 레거시 엑셀에서, 같은 시트(cohort_date) 안의 서로 다른
    종목이 같은 stock_code로 저장되면 안 됩니다 (SqliteCohortRepository의 PK가
    (cohort_date, stock_code)라서 코드가 전부 ''로 비어있으면 한 종목만 남고
    나머지가 덮어써짐).
    """
    storage = LocalStorageAdapter(base_path=str(tmp_path))
    filename = _make_legacy_workbook(tmp_path)

    repo = ExcelCohortRepository(file_path=filename)
    cohorts = repo.load_all_cohorts(storage)

    assert len(cohorts) == 1
    cohort = cohorts[0]
    assert cohort.cohort_date == date(2020, 1, 2)
    assert len(cohort.stocks) == 2

    codes = {s.stock.code for s in cohort.stocks}
    assert len(codes) == 2  # 코드가 서로 달라야 함 (이름 기반 대체 식별자)
    assert "" not in codes
