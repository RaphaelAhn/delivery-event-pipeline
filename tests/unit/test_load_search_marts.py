"""Spark 결과 → ClickHouse 적재기 테스트. Spark 도 ClickHouse 도 없이 변환만 확인한다."""

import json
from datetime import UTC, date, datetime

import pytest

from pipeline.marts.load_search_marts import MART_SPECS, coerce, load, read_jsonl_dir, to_rows

SESSION_ROW = {
    "session_id": "S-0000000000ab",
    "event_date": "2026-09-22",
    "queries": 3,
    "distinct_queries": 3,
    "clicks": 2,
    "click_rate": 0.6666666666666666,
    "distinct_query_ratio": 1.0,
    "avg_gap_seconds": 61.5,
    "min_gap_seconds": 40.0,
    "stddev_gap_seconds": 12.25,
    "avg_click_rank": 2.0,
    "span_seconds": 123,
    "queries_per_minute": 1.46,
    "client": "web",
    "first_query_at": "2026-09-22T10:00:00.000Z",
    "last_query_at": "2026-09-22T10:02:03.000Z",
}


class FakeClient:
    def __init__(self):
        self.inserts = []
        self.calls = []  # command·insert 가 호출된 순서

    def insert(self, table, rows, column_names):
        self.inserts.append((table, [list(r) for r in rows], list(column_names)))
        self.calls.append(("insert", table))

    def command(self, sql, parameters=None):
        self.calls.append(("command", sql, parameters))


def write_part(directory, name, records):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")


def test_dates_and_timestamps_are_converted():
    assert coerce("event_date", "2026-09-22") == date(2026, 9, 22)
    assert coerce("first_query_at", "2026-09-22T10:00:00.000Z") == datetime(
        2026, 9, 22, 10, 0, tzinfo=UTC
    )
    assert coerce("queries", 3) == 3
    assert coerce("avg_gap_seconds", None) is None


def test_row_follows_the_column_order_of_the_table():
    columns = MART_SPECS[0][2]
    row = to_rows([SESSION_ROW], columns)[0]
    assert row[columns.index("session_id")] == "S-0000000000ab"
    assert row[columns.index("event_date")] == date(2026, 9, 22)
    assert len(row) == len(columns)


def test_missing_optional_value_becomes_none():
    # 검색이 1회뿐인 세션에는 간격이 없어 Spark 가 그 칸을 아예 쓰지 않는다
    columns = MART_SPECS[0][2]
    sparse = {k: v for k, v in SESSION_ROW.items() if k != "avg_gap_seconds"}
    row = to_rows([sparse], columns)[0]
    assert row[columns.index("avg_gap_seconds")] is None


def test_reads_every_part_file_and_skips_blank_lines(tmp_path):
    directory = tmp_path / "mart_search_session"
    write_part(directory, "part-00000.json", [SESSION_ROW])
    write_part(directory, "part-00001.json", [{**SESSION_ROW, "session_id": "S-0000000000cd"}])
    (directory / "part-00002.json").write_text("\n", encoding="utf-8")  # 빈 part 파일
    (directory / "_SUCCESS").write_text("", encoding="utf-8")  # Spark 가 남기는 표식

    records = read_jsonl_dir(directory)
    assert [r["session_id"] for r in records] == ["S-0000000000ab", "S-0000000000cd"]


def test_missing_output_directory_is_an_error(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_jsonl_dir(tmp_path / "nope")


def test_load_sends_each_mart_to_its_table(tmp_path):
    write_part(tmp_path / "mart_search_session", "part-0.json", [SESSION_ROW])
    write_part(
        tmp_path / "mart_search_daily",
        "part-0.json",
        [
            {
                "event_date": "2026-09-22",
                "sessions": 1,
                "queries": 3,
                "clicks": 2,
                "distinct_queries": 3,
                "ctr": 0.67,
            }
        ],
    )
    write_part(tmp_path / "mart_search_top_query", "part-0.json", [])

    client = FakeClient()
    loaded = load(tmp_path, client=client)

    assert loaded == {
        "mart_search_session": 1,
        "mart_search_daily": 1,
        "mart_search_top_query": 0,
    }
    # 빈 마트는 insert 를 호출하지 않는다
    assert [table for table, _, _ in client.inserts] == [
        "mart_search_session",
        "mart_search_daily",
    ]


def write_day(tmp_path, day="2026-09-22"):
    write_part(
        tmp_path / "mart_search_session", "part-0.json", [{**SESSION_ROW, "event_date": day}]
    )
    write_part(
        tmp_path / "mart_search_daily",
        "part-0.json",
        [{"event_date": day, "sessions": 1, "queries": 3, "clicks": 2, "distinct_queries": 3}],
    )
    write_part(tmp_path / "mart_search_top_query", "part-0.json", [])


def test_date_mode_deletes_the_day_before_inserting(tmp_path):
    # 백필을 몇 번 돌려도 그 날짜의 행이 한 벌만 남으려면, 넣기 전에 반드시 지워야 한다
    write_day(tmp_path)
    client = FakeClient()
    load(tmp_path, client=client, target=date(2026, 9, 22))

    session_calls = [c for c in client.calls if "mart_search_session" in str(c)]
    assert session_calls[0][0] == "command"
    assert session_calls[0][1].startswith("DELETE FROM mart_search_session")
    assert session_calls[0][2] == {"d": date(2026, 9, 22)}
    assert session_calls[1] == ("insert", "mart_search_session")


def test_date_mode_clears_a_day_even_when_the_result_is_empty(tmp_path):
    # 인기 검색어가 0행이어도 예전 행이 남아 있으면 안 된다
    write_day(tmp_path)
    client = FakeClient()
    load(tmp_path, client=client, target=date(2026, 9, 22))

    deleted = [c[1].split()[2] for c in client.calls if c[0] == "command"]
    assert deleted == ["mart_search_session", "mart_search_daily", "mart_search_top_query"]
    assert ("insert", "mart_search_top_query") not in client.calls


def test_date_mode_refuses_rows_from_another_day(tmp_path):
    write_day(tmp_path, day="2026-09-23")
    client = FakeClient()
    with pytest.raises(ValueError, match="rows outside 2026-09-22"):
        load(tmp_path, client=client, target=date(2026, 9, 22))
    # 지우기 전에 멈춰야 기존 데이터가 사라지지 않는다
    assert client.calls == []


def test_without_a_date_nothing_is_deleted(tmp_path):
    write_day(tmp_path)
    client = FakeClient()
    load(tmp_path, client=client)
    assert all(c[0] == "insert" for c in client.calls)
