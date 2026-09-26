"""Spark 가 쓴 JSONL 결과를 ClickHouse 마트 테이블에 적재한다.

Spark 는 결과를 여러 개의 part 파일로 나눠 쓴다. 그 폴더를 통째로 읽어 한 번에 넣는다.
마트 테이블은 ReplacingMergeTree 라서, 같은 배치를 다시 적재해도 행이 늘지 않는다 (멱등성).

    python -m pipeline.marts.load_search_marts --input data/marts
    python -m pipeline.marts.load_search_marts --input data/marts/dt=2026-09-25 --date 2026-09-25

`--date` 를 주면 그 날짜의 기존 행을 지운 뒤 넣는다 (날짜 단위 덮어쓰기).
ReplacingMergeTree 의 중복 정리는 백그라운드 병합 때 일어나서, 병합 전에는 `FINAL` 없이 세면
행이 두 배로 보인다. 백필은 "몇 번을 돌려도 그 날짜의 행이 정확히 한 벌"이어야 하므로
병합에 기대지 않고 지우고 다시 쓴다. 늦게 온 이벤트로 세션이 줄거나 바뀌어도 옛 행이 남지 않는다.
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, date, datetime
from pathlib import Path

import clickhouse_connect

from pipeline.config import settings

# (Spark 출력 폴더, 대상 테이블, 넣을 컬럼 순서)
MART_SPECS = (
    (
        "mart_search_session",
        "mart_search_session",
        [
            "session_id",
            "event_date",
            "queries",
            "distinct_queries",
            "clicks",
            "click_rate",
            "distinct_query_ratio",
            "avg_gap_seconds",
            "min_gap_seconds",
            "stddev_gap_seconds",
            "avg_click_rank",
            "span_seconds",
            "queries_per_minute",
            "client",
            "first_query_at",
            "last_query_at",
        ],
    ),
    (
        "mart_search_daily",
        "mart_search_daily",
        ["event_date", "sessions", "queries", "clicks", "distinct_queries", "ctr"],
    ),
    (
        "mart_search_top_query",
        "mart_search_top_query",
        ["event_date", "rank", "query", "queries", "sessions", "clicks", "ctr"],
    ),
)

DATE_COLUMNS = {"event_date"}
DATETIME_COLUMNS = {"first_query_at", "last_query_at"}


def read_jsonl_dir(directory: Path) -> list[dict]:
    """Spark 가 나눠 쓴 part-*.json 파일을 모두 읽는다. 빈 part 파일도 있을 수 있다."""
    if not directory.is_dir():
        raise FileNotFoundError(f"missing Spark output: {directory}")
    rows: list[dict] = []
    for part in sorted(directory.glob("part-*")):
        with part.open(encoding="utf-8") as handle:
            rows.extend(json.loads(line) for line in handle if line.strip())
    return rows


def coerce(column: str, value):
    """JSON 문자열을 ClickHouse 가 기대하는 날짜·시각 타입으로 바꾼다."""
    if value is None:
        return None
    if column in DATE_COLUMNS:
        return date.fromisoformat(value) if isinstance(value, str) else value
    if column in DATETIME_COLUMNS and isinstance(value, str):
        # Spark 는 UTC 로 쓰지만 시간대 표기를 붙이지 않는다
        return datetime.fromisoformat(value.replace("Z", "")).replace(tzinfo=UTC)
    return value


def to_rows(records: list[dict], columns: list[str]) -> list[list]:
    return [[coerce(column, record.get(column)) for column in columns] for record in records]


def check_single_date(table: str, records: list[dict], target: date) -> None:
    """다른 날짜 행이 섞여 있으면 멈춘다.

    지운 날짜와 넣는 날짜가 다르면 데이터가 사라지거나 겹친다.
    """
    stray = {r.get("event_date") for r in records} - {target.isoformat()}
    if stray:
        raise ValueError(f"{table}: rows outside {target}: {sorted(map(str, stray))}")


def load(input_dir: Path, client=None, target: date | None = None) -> dict[str, int]:
    client = client or clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_db,
    )
    loaded: dict[str, int] = {}
    for folder, table, columns in MART_SPECS:
        records = read_jsonl_dir(input_dir / folder)
        if target is not None:
            check_single_date(table, records, target)
            # 결과가 0행이어도 지운다. "그날 데이터 없음"도 덮어써야 할 결과다
            client.command(
                f"DELETE FROM {table} WHERE event_date = {{d:Date}}",
                parameters={"d": target},
            )
        if not records:
            loaded[table] = 0
            continue
        client.insert(table, to_rows(records, columns), column_names=columns)
        loaded[table] = len(records)
    return loaded


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Spark search marts into ClickHouse")
    parser.add_argument("--input", type=Path, default=Path("data/marts"))
    parser.add_argument(
        "--date", type=date.fromisoformat, help="이 날짜의 기존 행을 지우고 다시 넣는다"
    )
    args = parser.parse_args()

    for table, count in load(args.input, target=args.date).items():
        print(f"{table:<24} {count}")


if __name__ == "__main__":
    main()
