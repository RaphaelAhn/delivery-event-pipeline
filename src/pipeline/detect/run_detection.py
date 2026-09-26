"""운영 경로: ClickHouse 의 세션 마트를 읽어 점수를 매기고 결과를 다시 저장한다.

    python -m pipeline.detect.run_detection --threshold 0.5
    python -m pipeline.detect.run_detection --date 2026-09-25

`--date` 를 주면 그날 세션만 점수를 매기고, 그날 기존 판정을 지운 뒤 다시 쓴다 (백필 멱등성).
속도 신호의 z-score 도 그날 세션 집단 기준으로 계산된다 (abuse.py 의 "그날 집단의 분포").

채점(scripts/score_detector.py)은 정답지가 있는 합성 데이터로 성능을 재고,
이 스크립트는 정답지 없이 실제 적재된 데이터에 같은 로직을 적용한다.
둘 다 pipeline.detect.abuse 의 같은 함수를 쓴다.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import date

import clickhouse_connect

from pipeline.config import settings
from pipeline.detect.abuse import DEFAULT_THRESHOLD, score_sessions
from pipeline.detect.features import from_mart_row

SELECT_SESSIONS = """
SELECT session_id, event_date, queries, distinct_queries, clicks,
       span_seconds, avg_gap_seconds, stddev_gap_seconds
FROM mart_search_session FINAL
"""

INSERT_COLUMNS = [
    "session_id",
    "event_date",
    "score",
    "is_abusive",
    "speed",
    "regularity",
    "monotony",
    "disinterest",
    "reasons",
    "threshold",
]


def build_client():
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_db,
    )


def run(threshold: float = DEFAULT_THRESHOLD, client=None, target: date | None = None) -> Counter:
    client = client or build_client()
    if target is None:
        result = client.query(SELECT_SESSIONS)
    else:
        result = client.query(
            SELECT_SESSIONS + "WHERE event_date = {d:Date}\n", parameters={"d": target}
        )
        client.command(
            "DELETE FROM mart_session_abuse WHERE event_date = {d:Date}", parameters={"d": target}
        )
    rows = [dict(zip(result.column_names, row, strict=True)) for row in result.result_rows]
    if not rows:
        return Counter()

    dates = {row["session_id"]: row["event_date"] for row in rows}
    scores = score_sessions([from_mart_row(row) for row in rows])

    payload = [
        [
            score.session_id,
            dates[score.session_id],
            score.score,
            1 if score.is_abusive(threshold) else 0,
            score.signals.get("speed", 0.0),
            score.signals.get("regularity", 0.0),
            score.signals.get("monotony", 0.0),
            score.signals.get("disinterest", 0.0),
            list(score.reasons),
            threshold,
        ]
        for score in scores
    ]
    client.insert("mart_session_abuse", payload, column_names=INSERT_COLUMNS)

    stats = Counter(scored=len(scores))
    stats["flagged"] = sum(1 for s in scores if s.is_abusive(threshold))
    for score in scores:
        if score.is_abusive(threshold):
            for reason in score.reasons:
                stats[f"reason:{reason.split('=')[0]}"] += 1
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Score sessions for abuse and store the result")
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    parser.add_argument("--date", type=date.fromisoformat, help="이 날짜 세션만 다시 판정한다")
    args = parser.parse_args()

    stats = run(args.threshold, target=args.date)
    print(f"scored  : {stats['scored']}")
    print(f"flagged : {stats['flagged']} (threshold {args.threshold})")
    for key in sorted(k for k in stats if k.startswith("reason:")):
        print(f"  {key[7:]:<20} {stats[key]}")


if __name__ == "__main__":
    main()
