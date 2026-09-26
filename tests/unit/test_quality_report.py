"""일별 품질 리포트 테스트. ClickHouse 없이 판정 로직과 저장 순서만 확인한다."""

import json
from datetime import date

from pipeline.quality.report import MAX_FLAGGED_RATIO, evaluate, run

HEALTHY = {
    "session_rows": 3000,
    "session_keys": 3000,
    "daily_rows": 1,
    "top_rows": 20,
    "top_ranks": 20,
    "abuse_rows": 3000,
    "abuse_keys": 3000,
    "flagged": 168,
}


def failed(metrics):
    return {c.name for c in evaluate(metrics) if not c.passed}


def test_a_healthy_day_passes_every_check():
    assert failed(HEALTHY) == set()


def test_rows_loaded_twice_are_caught():
    # 백필을 두 번 돌렸는데 지우지 않고 쌓였다면 행 수가 고유 세션 수의 두 배가 된다
    doubled = {**HEALTHY, "session_rows": 6000, "abuse_rows": 6000}
    assert failed(doubled) == {"session_rows_unique", "abuse_covers_sessions"}


def test_an_empty_day_fails():
    empty = dict.fromkeys(HEALTHY, 0)
    assert "sessions_present" in failed(empty)
    assert "daily_row_single" in failed(empty)


def test_sessions_left_unscored_fail_the_coverage_check():
    assert failed({**HEALTHY, "abuse_rows": 2990, "abuse_keys": 2990}) == {"abuse_covers_sessions"}


def test_duplicate_ranks_fail():
    assert failed({**HEALTHY, "top_rows": 21}) == {"top_query_ranks_unique"}


def test_flagging_too_many_sessions_fails():
    too_many = {**HEALTHY, "flagged": int(3000 * MAX_FLAGGED_RATIO) + 1}
    assert failed(too_many) == {"flagged_ratio_bounded"}


class FakeResult:
    def __init__(self, metrics):
        self.column_names = list(metrics)
        self.result_rows = [list(metrics.values())]


class FakeClient:
    def __init__(self, metrics):
        self.metrics = metrics
        self.calls = []

    def query(self, sql, parameters=None):
        self.calls.append(("query", parameters))
        return FakeResult(self.metrics)

    def command(self, sql, parameters=None):
        self.calls.append(("command", sql.strip().split("\n")[-1][:40], parameters))

    def insert(self, table, rows, column_names):
        self.calls.append(("insert", table, len(rows)))


def test_report_is_written_and_stored_even_when_a_check_fails(tmp_path):
    # 실패한 날일수록 기록이 남아야 원인을 볼 수 있다
    client = FakeClient({**HEALTHY, "session_rows": 6000, "abuse_rows": 6000})
    checks = run(date(2026, 9, 25), tmp_path, client=client)

    report = json.loads((tmp_path / "quality_2026-09-25.json").read_text(encoding="utf-8"))
    assert report["passed"] is False
    assert report["metrics"]["session_rows"] == 6000
    assert len(report["checks"]) == len(checks)

    kinds = [c[0] for c in client.calls]
    assert kinds == ["query", "command", "command", "insert"]  # 조회 → 테이블 보장 → 삭제 → 삽입
    assert client.calls[2][2] == {"d": date(2026, 9, 25)}
    assert client.calls[3] == ("insert", "mart_quality_report", len(checks))
