"""합성 검색 로그 생성기.

세션 하나 = 한 사람이 이어서 한 검색 행동. 세션마다 검색어와 클릭이 번갈아 나온다.
정상 사용자 사이에 어뷰징(봇) 세션을 섞고, **어느 세션이 봇인지 정답지에 남긴다.**
8일차 이상 탐지가 그 정답지로 채점된다.

정상 세션과 봇 세션의 차이 (탐지가 보게 될 신호)
    검색 간격   정상 20~120초  /  봇 0.5~3초
    검색 횟수   정상 1~6회     /  봇 40~200회
    검색어 다양성 정상 대부분 다름 / 봇 같은 검색어 반복
    클릭률      정상 약 60%    /  봇 5% 미만, 클릭해도 항상 1위

배달 이벤트 생성기와 달리 **최근 시각**을 기준으로 만든다.
수집 지연(받은 시각 − 사건 시각)을 의미 있게 재려면 과거 고정 날짜는 쓸 수 없다.
"""

from __future__ import annotations

import argparse
import random
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pipeline.producer.generator import AnomalyRates, GeneratedBatch, _Pending, _ts, write_batch

CLIENTS = ("web", "app")
POPULAR_QUERIES = (
    "날씨",
    "환율",
    "로또 당첨번호",
    "지하철 노선도",
    "맛집 추천",
    "영화 순위",
    "주식 시세",
    "배송 조회",
    "부동산 시세",
    "채용 공고",
)
LONG_TAIL_QUERIES = (
    "판교 점심 맛집",
    "데이터 엔지니어 로드맵",
    "kafka 파티션 개수",
    "clickhouse 파티션 설계",
    "전세 대출 조건",
    "제주도 3박 4일",
    "고양이 사료 추천",
    "무선 이어폰 비교",
)
BOT_QUERIES = ("특가 쿠폰", "무료 상품권", "최저가", "쿠폰 코드")

NORMAL_CLICK_PROBABILITY = 0.6
BOT_CLICK_PROBABILITY = 0.04


@dataclass(frozen=True)
class SearchMix:
    """세션 구성 비율.

    봇을 두 종류로 나눈다.
      aggressive : 숨길 생각이 없는 봇. 빠르고 많이 두드린다. 규칙만으로도 잡힌다.
      stealth    : 사람인 척하는 봇. 느리게 돌고 가끔 클릭하며 검색어도 몇 개 섞는다.
                   사람과 신호가 겹치므로 임계값을 어디에 둘지에 따라 놓치거나 오탐이 생긴다.

    stealth 를 넣는 이유: 모든 봇이 뚜렷하면 탐지 점수가 항상 만점이라
    "임계값을 왜 그렇게 정했나"를 설명할 거리가 없다.
    """

    bot_session_ratio: float = 0.05
    stealth_share: float = 0.4  # 봇 중 사람인 척하는 비율

    def __post_init__(self) -> None:
        if not 0 <= self.bot_session_ratio <= 1:
            raise ValueError(
                f"bot_session_ratio must be between 0 and 1, got {self.bot_session_ratio}"
            )
        if not 0 <= self.stealth_share <= 1:
            raise ValueError(f"stealth_share must be between 0 and 1, got {self.stealth_share}")


class _SearchGenerator:
    def __init__(self, seed: int, start: datetime) -> None:
        self.rng = random.Random(seed)
        self.start = start

    def event_id(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def session_id(self) -> str:
        return f"S-{self.rng.getrandbits(48):012x}"

    def ingest_latency(self) -> timedelta:
        return timedelta(seconds=self.rng.uniform(0.2, 4))

    def _query_event(self, session_id: str, query: str, at: datetime, client: str) -> dict:
        return {
            "schema_version": 1,
            "event_id": self.event_id(),
            "event_type": "SearchQuery",
            "session_id": session_id,
            "query": query,
            "result_count": self.rng.randint(0, 200),
            "client": client,
            "event_time": at,
        }

    def _click_event(self, session_id: str, query: str, at: datetime, rank: int) -> dict:
        return {
            "schema_version": 1,
            "event_id": self.event_id(),
            "event_type": "SearchResultClick",
            "session_id": session_id,
            "query": query,
            "rank": rank,
            "doc_id": f"doc-{self.rng.getrandbits(32):08x}",
            "event_time": at,
        }

    def normal_session(self, session_id: str) -> list[dict]:
        """사람이 검색하는 모습. 간격이 길고, 검색어가 바뀌고, 절반 이상 클릭한다."""
        rng = self.rng
        client = rng.choice(CLIENTS)
        now = self.start + timedelta(seconds=rng.uniform(0, 24 * 3600))
        events: list[dict] = []

        for _ in range(rng.randint(1, 6)):
            query = rng.choice(POPULAR_QUERIES if rng.random() < 0.6 else LONG_TAIL_QUERIES)
            events.append(self._query_event(session_id, query, now, client))
            if rng.random() < NORMAL_CLICK_PROBABILITY:
                # 사람은 결과를 읽고 누르므로 몇 초 걸리고, 위쪽 결과를 더 자주 누른다
                clicked_at = now + timedelta(seconds=rng.uniform(2, 20))
                rank = min(10, max(1, int(rng.expovariate(0.5)) + 1))
                events.append(self._click_event(session_id, query, clicked_at, rank))
            now += timedelta(seconds=rng.uniform(20, 120))
        return events

    def bot_session(self, session_id: str) -> list[dict]:
        """자동화된 수집. 간격이 일정하고 짧으며, 같은 검색어를 반복하고 거의 클릭하지 않는다."""
        rng = self.rng
        client = rng.choice(CLIENTS)
        now = self.start + timedelta(seconds=rng.uniform(0, 24 * 3600))
        query_pool = [rng.choice(BOT_QUERIES)] * 3 + [rng.choice(POPULAR_QUERIES)]
        interval = rng.uniform(0.5, 3)
        events: list[dict] = []

        for _ in range(rng.randint(40, 200)):
            query = rng.choice(query_pool)
            events.append(self._query_event(session_id, query, now, client))
            if rng.random() < BOT_CLICK_PROBABILITY:
                events.append(self._click_event(session_id, query, now + timedelta(seconds=0.3), 1))
            # 사람이라면 나올 수 없는 규칙적인 간격 (흔들림이 아주 작다)
            now += timedelta(seconds=interval * rng.uniform(0.95, 1.05))
        return events

    def stealth_bot_session(self, session_id: str) -> list[dict]:
        """사람인 척하는 봇. 사람보다 조금 빠르고 조금 더 많이 검색할 뿐이다.

        의도적으로 사람과 겹치게 만든다.
          검색 횟수 8~25회   (사람 1~6회, 공격적 봇 40~200회)
          간격 5~25초        (사람 20~120초 → 느린 사람과 겹친다)
          클릭률 약 25%      (사람 60% 와 공격적 봇 4% 사이)
          검색어 3~6개 반복  (사람보다는 단조롭지만 0.019 처럼 극단적이지 않다)
        """
        rng = self.rng
        client = rng.choice(CLIENTS)
        now = self.start + timedelta(seconds=rng.uniform(0, 24 * 3600))
        pool = [rng.choice(BOT_QUERIES)] + rng.sample(POPULAR_QUERIES, rng.randint(2, 5))
        base_interval = rng.uniform(5, 25)
        events: list[dict] = []

        for _ in range(rng.randint(8, 25)):
            query = rng.choice(pool)
            events.append(self._query_event(session_id, query, now, client))
            if rng.random() < 0.25:
                events.append(
                    self._click_event(
                        session_id,
                        query,
                        now + timedelta(seconds=rng.uniform(1, 6)),
                        rng.randint(1, 5),
                    )
                )
            # 사람처럼 보이도록 간격을 넓게 흔든다 (표준편차가 0 이 아니다)
            now += timedelta(seconds=base_interval * rng.uniform(0.5, 1.8))
        return events

    def corrupt(self, event: dict) -> tuple[dict, str]:
        broken = dict(event)
        options = ["missing_session_id", "empty_query", "bad_event_time"]
        if "rank" in broken:
            options.append("non_positive_rank")
        kind = self.rng.choice(options)
        if kind == "missing_session_id":
            del broken["session_id"]
        elif kind == "empty_query":
            broken["query"] = ""
        elif kind == "non_positive_rank":
            broken["rank"] = 0
        else:
            broken["event_time"] = "2026-13-45T25:61:00Z"
        return broken, kind


def generate_search(
    sessions: int,
    seed: int = 42,
    start: datetime | None = None,
    rates: AnomalyRates | None = None,
    mix: SearchMix | None = None,
) -> GeneratedBatch:
    """검색 로그를 만든다. 반환 형식은 배달 생성기와 같다 (도착 순서 + 정답지)."""
    if sessions < 1:
        raise ValueError("sessions must be >= 1")
    rates = rates or AnomalyRates()
    mix = mix or SearchMix()
    # 기본은 "어제 이 시각부터 24시간". 수집 지연을 의미 있게 재기 위해 최근 시각을 쓴다.
    start = start or (datetime.now(UTC) - timedelta(days=1)).replace(microsecond=0)

    gen = _SearchGenerator(seed, start)
    rng = gen.rng
    pending: list[_Pending] = []
    injected: dict[str, list] = {"duplicate": [], "late": [], "out_of_order": [], "invalid": []}
    bot_sessions: list[str] = []
    bot_profiles: dict[str, str] = {}  # 세션 → aggressive / stealth
    clean_event_count = 0

    for _ in range(sessions):
        session_id = gen.session_id()
        if rng.random() < mix.bot_session_ratio:
            stealth = rng.random() < mix.stealth_share
            events = gen.stealth_bot_session(session_id) if stealth else gen.bot_session(session_id)
            bot_sessions.append(session_id)
            bot_profiles[session_id] = "stealth" if stealth else "aggressive"
        else:
            events = gen.normal_session(session_id)
        clean_event_count += len(events)

        first_arrival = events[0]["event_time"] + gen.ingest_latency()
        pending.append(_Pending(first_arrival, events[0]))

        for event in events[1:]:
            arrival = event["event_time"] + gen.ingest_latency()
            draw = rng.random()
            if draw < rates.late:
                arrival += timedelta(minutes=rng.uniform(10, 120))
                injected["late"].append(event["event_id"])
            elif draw < rates.late + rates.out_of_order:
                arrival = first_arrival - timedelta(seconds=rng.uniform(1, 30))
                injected["out_of_order"].append(event["event_id"])
            elif draw < rates.late + rates.out_of_order + rates.invalid:
                event, kind = gen.corrupt(event)
                injected["invalid"].append({"event_id": event["event_id"], "kind": kind})
            pending.append(_Pending(arrival, event))

    for item in list(pending):
        if rng.random() < rates.duplicate:
            resend = item.arrival + timedelta(seconds=rng.uniform(1, 600))
            pending.append(_Pending(resend, dict(item.event)))
            injected["duplicate"].append(item.event["event_id"])

    pending.sort(key=lambda p: p.arrival)
    events_out = [
        {k: _ts(v) if isinstance(v, datetime) else v for k, v in p.event.items()} for p in pending
    ]

    manifest = {
        "seed": seed,
        "sessions": sessions,
        "start": _ts(start),
        "rates": asdict(rates),
        "mix": asdict(mix),
        "counts": {
            "clean_events": clean_event_count,
            "emitted_events": len(events_out),
            "bot_sessions": len(bot_sessions),
            "aggressive_bots": sum(1 for kind in bot_profiles.values() if kind == "aggressive"),
            "stealth_bots": sum(1 for kind in bot_profiles.values() if kind == "stealth"),
            "normal_sessions": sessions - len(bot_sessions),
            **{name: len(ids) for name, ids in injected.items()},
        },
        "injected": injected,
        # 8일차 이상 탐지의 정답지
        "bot_sessions": bot_sessions,
        "bot_profiles": bot_profiles,
    }
    return GeneratedBatch(events=events_out, manifest=manifest)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sessions", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("data/search_events.jsonl"))
    parser.add_argument("--bot-ratio", type=float, default=SearchMix.bot_session_ratio)
    parser.add_argument("--invalid-rate", type=float, default=AnomalyRates.invalid)
    args = parser.parse_args()

    batch = generate_search(
        args.sessions,
        seed=args.seed,
        rates=AnomalyRates(invalid=args.invalid_rate),
        mix=SearchMix(bot_session_ratio=args.bot_ratio),
    )
    manifest_path = write_batch(batch, args.output)
    counts = batch.manifest["counts"]
    print(f"Wrote {counts['emitted_events']} events for {args.sessions} sessions to {args.output}")
    print(
        f"Sessions: normal={counts['normal_sessions']} bot={counts['bot_sessions']} | "
        f"injected duplicate={counts['duplicate']} late={counts['late']} "
        f"out_of_order={counts['out_of_order']} invalid={counts['invalid']}"
    )
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
