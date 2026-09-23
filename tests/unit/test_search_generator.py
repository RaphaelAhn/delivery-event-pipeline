"""검색 로그 생성기 테스트.

여기서 지키려는 것
- 봇 세션과 정상 세션이 **구분 가능한 신호**를 갖는다 (8일차 탐지가 잡을 수 있어야 한다)
- 정답지(manifest)에 봇 세션 목록이 정확히 남는다
- 생성된 이벤트가 계약(search_event.schema.json)을 지킨다
"""

import json
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from statistics import mean

import pytest

from pipeline.common.topics import SEARCH_TOPIC, topic_for
from pipeline.consumer.validate import validate
from pipeline.producer.generator import AnomalyRates
from pipeline.producer.search_generator import SearchMix, generate_search

NO_ANOMALIES = AnomalyRates(duplicate=0, late=0, out_of_order=0, invalid=0)


def parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def by_session(events):
    grouped = defaultdict(list)
    for event in events:
        grouped[event["session_id"]].append(event)
    return grouped


def intervals(events):
    times = sorted(parse(e["event_time"]) for e in events if e["event_type"] == "SearchQuery")
    return [(b - a).total_seconds() for a, b in zip(times, times[1:], strict=False)]


def test_same_seed_produces_identical_batches():
    assert generate_search(200, seed=7).events == generate_search(200, seed=7).events


def test_every_clean_event_passes_the_contract():
    batch = generate_search(300, seed=3, rates=NO_ANOMALIES)
    for event in batch.events:
        result = validate(json.dumps(event).encode())
        assert result.ok, (event, result.violations)


def test_search_events_go_to_the_search_topic():
    for event in generate_search(100, seed=5, rates=NO_ANOMALIES).events:
        assert topic_for(event["event_type"]) == SEARCH_TOPIC


def test_events_are_recent_not_a_fixed_past_date():
    # 수집 지연을 의미 있게 재려면 최근 시각이어야 한다 (4일차에 발견한 한계)
    batch = generate_search(50, seed=11, rates=NO_ANOMALIES)
    latest = max(parse(e["event_time"]) for e in batch.events)
    assert datetime.now(UTC) - latest < timedelta(days=2)


def test_manifest_lists_every_bot_session():
    batch = generate_search(500, seed=13, rates=NO_ANOMALIES, mix=SearchMix(bot_session_ratio=0.1))
    bots = set(batch.manifest["bot_sessions"])
    sessions = set(by_session(batch.events))

    assert bots
    assert bots <= sessions
    assert batch.manifest["counts"]["bot_sessions"] == len(bots)
    assert batch.manifest["counts"]["normal_sessions"] == 500 - len(bots)
    # 봇은 두 종류로 나뉘고, 둘을 합치면 전체 봇 수와 같다
    counts = batch.manifest["counts"]
    assert counts["aggressive_bots"] + counts["stealth_bots"] == counts["bot_sessions"]
    assert set(batch.manifest["bot_profiles"]) == bots


def test_bot_ratio_is_close_to_requested():
    batch = generate_search(
        3000, seed=17, rates=NO_ANOMALIES, mix=SearchMix(bot_session_ratio=0.08)
    )
    assert batch.manifest["counts"]["bot_sessions"] / 3000 == pytest.approx(0.08, abs=0.02)


def aggressive_ids(batch):
    return {sid for sid, kind in batch.manifest["bot_profiles"].items() if kind == "aggressive"}


def stealth_ids(batch):
    return {sid for sid, kind in batch.manifest["bot_profiles"].items() if kind == "stealth"}


def test_aggressive_bots_search_far_more_often_than_people():
    batch = generate_search(800, seed=19, rates=NO_ANOMALIES, mix=SearchMix(bot_session_ratio=0.1))
    bots = set(batch.manifest["bot_sessions"])
    aggressive = aggressive_ids(batch)
    grouped = by_session(batch.events)

    bot_counts = [len(events) for sid, events in grouped.items() if sid in aggressive]
    human_counts = [len(events) for sid, events in grouped.items() if sid not in bots]

    assert min(bot_counts) > max(human_counts), "공격적인 봇은 검색 횟수만으로도 구분돼야 한다"


def test_stealth_bots_overlap_with_people_on_purpose():
    """사람인 척하는 봇은 신호가 겹쳐야 한다. 겹치지 않으면 임계값 선택이 의미를 잃는다."""
    batch = generate_search(
        1500, seed=23, rates=NO_ANOMALIES, mix=SearchMix(bot_session_ratio=0.1, stealth_share=1.0)
    )
    bots = set(batch.manifest["bot_sessions"])
    grouped = by_session(batch.events)

    stealth_gaps = [
        mean(gaps) for sid, events in grouped.items() if sid in bots and (gaps := intervals(events))
    ]
    human_gaps = [
        mean(gaps)
        for sid, events in grouped.items()
        if sid not in bots and (gaps := intervals(events))
    ]
    assert min(human_gaps) < max(stealth_gaps), "느린 사람과 빠른 봇의 간격이 겹쳐야 한다"


def test_aggressive_bots_have_short_and_regular_intervals():
    batch = generate_search(800, seed=23, rates=NO_ANOMALIES, mix=SearchMix(bot_session_ratio=0.1))
    bots = set(batch.manifest["bot_sessions"])
    aggressive = aggressive_ids(batch)
    grouped = by_session(batch.events)

    bot_gaps = [mean(intervals(events)) for sid, events in grouped.items() if sid in aggressive]
    human_gaps = [
        mean(gaps)
        for sid, events in grouped.items()
        if sid not in bots and (gaps := intervals(events))
    ]

    assert max(bot_gaps) < 5, "봇은 몇 초 간격으로 검색한다"
    assert min(human_gaps) > 15, "사람은 훨씬 느리게 검색한다"


def test_aggressive_bots_barely_click():
    batch = generate_search(800, seed=29, rates=NO_ANOMALIES, mix=SearchMix(bot_session_ratio=0.1))
    bots = set(batch.manifest["bot_sessions"])
    aggressive = aggressive_ids(batch)
    grouped = by_session(batch.events)

    def click_rate(events):
        queries = sum(1 for e in events if e["event_type"] == "SearchQuery")
        clicks = sum(1 for e in events if e["event_type"] == "SearchResultClick")
        return clicks / queries if queries else 0

    bot_rates = [click_rate(events) for sid, events in grouped.items() if sid in aggressive]
    human_rates = [click_rate(events) for sid, events in grouped.items() if sid not in bots]

    assert mean(bot_rates) < 0.1
    assert mean(human_rates) > 0.4


def test_aggressive_bots_repeat_the_same_query():
    batch = generate_search(800, seed=31, rates=NO_ANOMALIES, mix=SearchMix(bot_session_ratio=0.1))
    aggressive = aggressive_ids(batch)
    grouped = by_session(batch.events)

    def distinct_ratio(events):
        queries = [e["query"] for e in events if e["event_type"] == "SearchQuery"]
        return len(set(queries)) / len(queries)

    bot_ratios = [distinct_ratio(events) for sid, events in grouped.items() if sid in aggressive]
    assert max(bot_ratios) < 0.2, "봇은 같은 검색어를 반복한다"


def test_clicks_follow_their_query_in_the_same_session():
    batch = generate_search(300, seed=37, rates=NO_ANOMALIES)
    for events in by_session(batch.events).values():
        ordered = sorted(events, key=lambda e: parse(e["event_time"]))
        for click in (e for e in ordered if e["event_type"] == "SearchResultClick"):
            earlier_queries = [
                e
                for e in ordered
                if e["event_type"] == "SearchQuery"
                and parse(e["event_time"]) <= parse(click["event_time"])
                and e["query"] == click["query"]
            ]
            assert earlier_queries, click


def test_injected_invalid_events_break_the_contract():
    batch = generate_search(800, seed=41, rates=AnomalyRates(invalid=0.05))
    injected = {item["event_id"] for item in batch.manifest["injected"]["invalid"]}
    assert injected

    flagged = {
        validate(json.dumps(e).encode()).event_id
        for e in batch.events
        if not validate(json.dumps(e).encode()).ok
    }
    assert flagged == injected


def test_invalid_bot_ratio_is_rejected():
    with pytest.raises(ValueError):
        SearchMix(bot_session_ratio=1.5)
