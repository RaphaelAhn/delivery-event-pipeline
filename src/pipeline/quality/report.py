"""일별 품질 리포트: 그날 배치가 만든 결과를 믿어도 되는지 점검한다.

Airflow DAG 의 마지막 단계다. 앞 단계가 "에러 없이 끝났다"는 것만으로는 부족하다.
세션이 0개여도, 같은 세션이 두 번 들어가도 작업 자체는 성공으로 끝나기 때문이다.

    python -m pipeline.quality.report --date 2026-09-25

점검 결과는 두 곳에 남긴다.
  - data/reports/quality_<날짜>.json   사람이 바로 열어 보는 용도
  - mart_quality_report 테이블         날짜별 추이를 대시보드로 보는 용도

하나라도 실패하면 리포트를 남긴 뒤 종료 코드 1 로 끝나 Airflow 작업이 실패 처리된다.

행 수는 일부러 `FINAL` 없이 센다. 백필을 두 번 돌렸을 때 실제로 저장된 행이 한 벌인지를
보려는 것이라, 병합 후 결과를 보여 주는 `FINAL` 을 쓰면 중복이 가려진다.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import clickhouse_connect

from pipeline.config import settings

DDL_PATH = Path(__file__).resolve().parents[3] / "clickhouse" / "init" / "004_quality_tables.sql"

# 생성기는 봇을 5% 섞는다. 20% 넘게 막았다면 탐지나 입력 중 하나가 고장 난 것으로 본다.
MAX_FLAGGED_RATIO = 0.20
# Spark 작업의 top_queries(top_n=20) 와 같은 값
TOP_N = 20

METRICS_SQL = """
SELECT
    (SELECT count() FROM mart_search_session WHERE event_date = {d:Date}) AS session_rows,
    (SELECT uniqExact(session_id) FROM mart_search_session WHERE event_date = {d:Date})
        AS session_keys,
    (SELECT count() FROM mart_search_daily WHERE event_date = {d:Date}) AS daily_rows,
    (SELECT count() FROM mart_search_top_query WHERE event_date = {d:Date}) AS top_rows,
    (SELECT uniqExact(rank) FROM mart_search_top_query WHERE event_date = {d:Date}) AS top_ranks,
    (SELECT count() FROM mart_session_abuse WHERE event_date = {d:Date}) AS abuse_rows,
    (SELECT uniqExact(session_id) FROM mart_session_abuse WHERE event_date = {d:Date})
        AS abuse_keys,
    (SELECT countIf(is_abusive = 1) FROM mart_session_abuse WHERE event_date = {d:Date})
        AS flagged
"""


@dataclass(frozen=True)
class Check:
    name: str
    passed: bool
    observed: float
    expected: str


def evaluate(m: dict[str, int]) -> list[Check]:
    """집계값으로 점검 항목을 판정한다. ClickHouse 없이 테스트할 수 있게 계산만 분리했다."""
    flagged_ratio = m["flagged"] / m["abuse_rows"] if m["abuse_rows"] else 0.0
    return [
        # 세션이 0개면 착지 파일이 비었거나 날짜를 잘못 넘긴 것이다
        Check("sessions_present", m["session_rows"] > 0, m["session_rows"], "> 0"),
        # 백필을 두 번 돌려도 세션 1개 = 1행 (멱등성)
        Check(
            "session_rows_unique",
            m["session_rows"] == m["session_keys"],
            m["session_rows"],
            f"= {m['session_keys']} (distinct session_id)",
        ),
        Check("daily_row_single", m["daily_rows"] == 1, m["daily_rows"], "= 1"),
        Check(
            "top_query_ranks_unique",
            m["top_rows"] == m["top_ranks"] and m["top_rows"] <= TOP_N,
            m["top_rows"],
            f"= {m['top_ranks']} (distinct rank), <= {TOP_N}",
        ),
        # 모든 세션이 정확히 한 번 판정됐는가
        Check(
            "abuse_covers_sessions",
            m["abuse_rows"] == m["session_rows"] == m["abuse_keys"],
            m["abuse_rows"],
            f"= {m['session_rows']} (session_rows)",
        ),
        Check(
            "flagged_ratio_bounded",
            flagged_ratio <= MAX_FLAGGED_RATIO,
            round(flagged_ratio, 4),
            f"<= {MAX_FLAGGED_RATIO}",
        ),
    ]


def build_client():
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_db,
    )


def collect(client, target: date) -> dict[str, int]:
    result = client.query(METRICS_SQL, parameters={"d": target})
    return dict(zip(result.column_names, result.result_rows[0], strict=True))


def save(client, target: date, checks: list[Check]) -> None:
    client.command(DDL_PATH.read_text(encoding="utf-8"))
    client.command(
        "DELETE FROM mart_quality_report WHERE run_date = {d:Date}", parameters={"d": target}
    )
    client.insert(
        "mart_quality_report",
        [[target, c.name, int(c.passed), float(c.observed), c.expected] for c in checks],
        column_names=["run_date", "check_name", "passed", "observed", "expected"],
    )


def write_json(path: Path, target: date, metrics: dict[str, int], checks: list[Check]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "run_date": target.isoformat(),
        "passed": all(c.passed for c in checks),
        "metrics": metrics,
        "checks": [asdict(c) for c in checks],
    }
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run(target: date, out_dir: Path, client=None) -> list[Check]:
    client = client or build_client()
    metrics = collect(client, target)
    checks = evaluate(metrics)
    write_json(out_dir / f"quality_{target.isoformat()}.json", target, metrics, checks)
    save(client, target, checks)
    return checks


def main() -> None:
    parser = argparse.ArgumentParser(description="Check one day of batch output")
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--out-dir", type=Path, default=Path("data/reports"))
    args = parser.parse_args()

    checks = run(args.date, args.out_dir)
    for c in checks:
        print(f"{'PASS' if c.passed else 'FAIL'}  {c.name:<24} {c.observed:<10g} {c.expected}")
    if not all(c.passed for c in checks):
        sys.exit(1)


if __name__ == "__main__":
    main()
