"""이상 탐지 운영 경로의 날짜 모드 테스트. ClickHouse 대신 호출 순서만 기록하는 가짜를 쓴다."""

from datetime import date

from pipeline.detect.run_detection import run


class FakeResult:
    column_names = []
    result_rows = []


class FakeClient:
    def __init__(self):
        self.calls = []

    def query(self, sql, parameters=None):
        self.calls.append(("query", sql, parameters))
        return FakeResult()

    def command(self, sql, parameters=None):
        self.calls.append(("command", sql, parameters))

    def insert(self, table, rows, column_names):
        self.calls.append(("insert", table))


def test_date_mode_reads_one_day_and_clears_its_old_verdicts():
    client = FakeClient()
    run(client=client, target=date(2026, 9, 25))

    (kind, sql, params), (kind2, sql2, params2) = client.calls
    assert kind == "query" and "WHERE event_date = {d:Date}" in sql
    assert params == {"d": date(2026, 9, 25)}
    # 그날 세션이 0개여도 예전 판정은 지운다
    assert kind2 == "command" and sql2.startswith("DELETE FROM mart_session_abuse")
    assert params2 == {"d": date(2026, 9, 25)}


def test_without_a_date_every_session_is_read_and_nothing_is_deleted():
    client = FakeClient()
    run(client=client)
    assert [c[0] for c in client.calls] == ["query"]
    assert "WHERE" not in client.calls[0][1]
