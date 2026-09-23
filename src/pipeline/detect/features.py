"""세션 특징. 탐지가 보는 값은 여기 정의된 것뿐이다.

특징은 두 곳에서 만들어진다.
  - 운영 경로: Spark 가 계산해 `mart_search_session` 에 넣은 값 (7일차)
  - 평가 경로: 이벤트 목록에서 직접 계산 (채점·테스트용, Spark 없이 돌리기 위해)

두 경로가 같은 값을 뜻하도록 이 파일에 정의를 한 번만 둔다.
"""

from __future__ import annotations

import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class SessionFeatures:
    session_id: str
    queries: int
    distinct_queries: int
    clicks: int
    span_seconds: float
    avg_gap_seconds: float | None  # 검색이 1회면 간격이 없다
    stddev_gap_seconds: float | None

    @property
    def click_rate(self) -> float:
        return self.clicks / self.queries if self.queries else 0.0

    @property
    def distinct_query_ratio(self) -> float:
        return self.distinct_queries / self.queries if self.queries else 1.0

    @property
    def queries_per_minute(self) -> float | None:
        if self.span_seconds <= 0:
            return None
        return self.queries / (self.span_seconds / 60.0)


def _parse(value: str | datetime) -> datetime:
    return value if isinstance(value, datetime) else datetime.fromisoformat(value)


def from_events(events: list[dict]) -> list[SessionFeatures]:
    """이벤트 목록에서 세션 특징을 만든다 (채점·테스트용).

    계약을 어긴 이벤트는 실제 파이프라인에서 DLQ 로 빠지므로 여기서도 제외한다.
    같은 event_id 는 한 번만 센다 (재전송 중복이 검색 횟수를 부풀리지 않도록).
    """
    by_session: dict[str, list[dict]] = defaultdict(list)
    seen: set[str] = set()
    for event in events:
        session_id = event.get("session_id")
        event_id = event.get("event_id")
        if not session_id or not event_id or event_id in seen:
            continue
        try:
            _parse(event["event_time"])
        except (KeyError, ValueError):
            continue  # 시각이 깨진 이벤트
        seen.add(event_id)
        by_session[session_id].append(event)

    features = []
    for session_id, session_events in by_session.items():
        queries = sorted(
            (e for e in session_events if e["event_type"] == "SearchQuery"),
            key=lambda e: _parse(e["event_time"]),
        )
        if not queries:
            continue  # 클릭만 있는 세션은 평가 대상이 아니다
        clicks = sum(1 for e in session_events if e["event_type"] == "SearchResultClick")
        times = [_parse(e["event_time"]) for e in queries]
        gaps = [(b - a).total_seconds() for a, b in zip(times, times[1:], strict=False)]

        features.append(
            SessionFeatures(
                session_id=session_id,
                queries=len(queries),
                distinct_queries=len({e["query"] for e in queries}),
                clicks=clicks,
                span_seconds=(times[-1] - times[0]).total_seconds(),
                avg_gap_seconds=statistics.fmean(gaps) if gaps else None,
                stddev_gap_seconds=statistics.pstdev(gaps) if len(gaps) > 1 else None,
            )
        )
    return features


def from_mart_row(row: dict) -> SessionFeatures:
    """`mart_search_session` 한 행을 특징으로 바꾼다 (운영 경로)."""
    return SessionFeatures(
        session_id=row["session_id"],
        queries=int(row["queries"]),
        distinct_queries=int(row["distinct_queries"]),
        clicks=int(row["clicks"]),
        span_seconds=float(row["span_seconds"]),
        avg_gap_seconds=row.get("avg_gap_seconds"),
        stddev_gap_seconds=row.get("stddev_gap_seconds"),
    )
