"""어뷰징 탐지 테스트.

가장 중요한 두 가지
- 극단적인 봇이 기준을 끌어올려 중간 강도의 봇을 가리지 않을 것 (회귀 테스트)
- 정답지 기준 성능이 떨어지지 않을 것 (F1 하한선)
"""

from datetime import UTC, datetime, timedelta

import pytest

from pipeline.detect.abuse import (
    DEFAULT_THRESHOLD,
    MIN_QUERIES,
    WEIGHTS,
    Evaluation,
    evaluate,
    score_sessions,
)
from pipeline.detect.features import from_events, from_mart_row
from pipeline.producer.search_generator import SearchMix, generate_search

START = datetime(2026, 9, 22, 10, 0, tzinfo=UTC)


def events_for(session_id, gaps_seconds, queries, clicks=0):
    """검색 사이 간격을 직접 정해 세션을 만든다."""
    events = []
    now = START
    for i, query in enumerate(queries):
        events.append(
            {
                "event_id": f"{session_id}-q{i}",
                "event_type": "SearchQuery",
                "session_id": session_id,
                "query": query,
                "event_time": now.isoformat(),
            }
        )
        if i < len(gaps_seconds):
            now += timedelta(seconds=gaps_seconds[i])
    for i in range(clicks):
        events.append(
            {
                "event_id": f"{session_id}-c{i}",
                "event_type": "SearchResultClick",
                "session_id": session_id,
                "query": queries[0],
                "event_time": (START + timedelta(seconds=1)).isoformat(),
            }
        )
    return events


def features_for(session_id, gaps_seconds, queries, clicks=0):
    return from_events(events_for(session_id, gaps_seconds, queries, clicks))[0]


HUMAN_GAP_SHAPE = (0.4, 1.6, 0.7, 2.0, 0.5, 1.3, 0.9, 1.8, 0.6)


def human_features(session_id, base=60):
    """사람다운 세션: 간격이 들쭉날쭉하고, 검색어가 매번 다르며, 절반 이상 클릭한다."""
    gaps = [base * shape for shape in HUMAN_GAP_SHAPE]
    return features_for(session_id, gaps, [f"q{j}" for j in range(10)], clicks=6)


# ---------------------------------------------------------------- 특징 계산


def test_gap_average_and_spread_are_computed():
    f = features_for("S-1", [10, 20, 30], ["a", "b", "c", "d"])
    assert f.queries == 4
    assert f.avg_gap_seconds == pytest.approx(20.0)
    assert f.stddev_gap_seconds == pytest.approx(8.165, abs=0.01)
    assert f.span_seconds == pytest.approx(60.0)


def test_repeated_queries_lower_the_diversity_ratio():
    f = features_for("S-2", [5] * 5, ["같은말"] * 6)
    assert f.distinct_query_ratio == pytest.approx(1 / 6)


def test_duplicate_events_are_counted_once():
    events = events_for("S-3", [10, 10], ["a", "b", "c"])
    duplicated = events + [dict(events[0])]  # 재전송 중복
    (f,) = from_events(duplicated)
    assert f.queries == 3


def test_events_with_broken_time_or_missing_session_are_skipped():
    events = events_for("S-4", [10, 10], ["a", "b", "c"])
    events.append({**events[0], "event_id": "x", "event_time": "2026-13-45T25:61:00Z"})
    events.append({**events[0], "event_id": "y", "session_id": None})
    (f,) = from_events(events)
    assert f.queries == 3


def test_mart_row_and_events_describe_the_same_thing():
    f = features_for("S-5", [10, 10], ["a", "b", "c"], clicks=1)
    row = {
        "session_id": "S-5",
        "queries": 3,
        "distinct_queries": 3,
        "clicks": 1,
        "span_seconds": 20,
        "avg_gap_seconds": 10.0,
        "stddev_gap_seconds": 0.0,
    }
    assert from_mart_row(row).click_rate == f.click_rate
    assert from_mart_row(row).queries_per_minute == pytest.approx(f.queries_per_minute)


# ---------------------------------------------------------------- 점수 계산


def test_score_stays_between_zero_and_one():
    features = [
        features_for("bot", [1] * 60, ["쿠폰"] * 61),
        features_for("human", [60, 90, 45], ["날씨", "환율", "맛집", "영화"], clicks=3),
    ]
    for score in score_sessions(features):
        assert 0.0 <= score.score <= 1.0


def test_sessions_with_too_few_queries_are_not_judged():
    features = [features_for(f"s{i}", [30] * 10, [f"q{j}" for j in range(11)]) for i in range(5)]
    features.append(features_for("short", [1], ["쿠폰", "쿠폰"]))
    scores = {s.session_id: s for s in score_sessions(features)}
    assert scores["short"].score == 0.0
    assert scores["short"].reasons == ("too_few_queries",)
    assert MIN_QUERIES > 2


def test_extreme_bots_do_not_hide_moderate_ones():
    """회귀 테스트: 평균·표준편차를 쓰면 이 경우 중간 봇의 속도 신호가 0 이 된다."""
    # 사람은 간격이 들쭉날쭉하고, 사람마다 속도도 다르다.
    # 모두 똑같은 간격으로 만들면 "규칙적"이라는 신호가 켜져 사람이 봇처럼 보인다.
    people = [human_features(f"human-{i}", base=40 + 5 * i) for i in range(30)]
    extreme = [features_for(f"fast-{i}", [1] * 99, ["쿠폰"] * 100) for i in range(5)]
    moderate = features_for("moderate", [12] * 14, ["쿠폰", "최저가"] * 7 + ["쿠폰"])

    scores = {s.session_id: s for s in score_sessions([*people, *extreme, moderate])}
    assert scores["moderate"].signals["speed"] > 0.5
    assert scores["moderate"].is_abusive(DEFAULT_THRESHOLD)
    assert not scores["human-0"].is_abusive(DEFAULT_THRESHOLD)


def test_reasons_explain_the_decision():
    people = [human_features(f"human-{i}", base=40 + 5 * i) for i in range(20)]
    bot = features_for("bot", [2] * 49, ["쿠폰"] * 50)
    scores = {s.session_id: s for s in score_sessions([*people, bot])}
    assert scores["bot"].reasons, "판정 사유가 없으면 운영에서 설명할 수 없다"
    assert set(scores["bot"].signals) == set(WEIGHTS)


# ---------------------------------------------------------------- 채점


def test_precision_recall_and_f1_are_computed():
    e = Evaluation(threshold=0.5, true_positives=8, false_positives=2, false_negatives=4)
    assert e.precision == pytest.approx(0.8)
    assert e.recall == pytest.approx(2 / 3)
    assert e.f1 == pytest.approx(0.7272, abs=0.001)


def test_raising_the_threshold_trades_recall_for_precision():
    batch = generate_search(1500, seed=5, mix=SearchMix(bot_session_ratio=0.06, stealth_share=0.4))
    scores = score_sessions(from_events(batch.events))
    truth = set(batch.manifest["bot_sessions"])

    low = evaluate(scores, truth, 0.40)
    high = evaluate(scores, truth, 0.70)
    assert low.recall >= high.recall
    assert low.precision <= high.precision


def test_detection_quality_does_not_regress():
    batch = generate_search(2000, seed=21, mix=SearchMix(bot_session_ratio=0.05, stealth_share=0.4))
    scores = score_sessions(from_events(batch.events))
    result = evaluate(scores, set(batch.manifest["bot_sessions"]), DEFAULT_THRESHOLD)

    assert result.f1 >= 0.90, f"F1 이 떨어졌다: {result.f1:.3f}"
    assert result.precision >= 0.90
    assert result.recall >= 0.85


def test_both_bot_kinds_are_caught():
    batch = generate_search(2000, seed=13, mix=SearchMix(bot_session_ratio=0.06, stealth_share=0.5))
    scores = {s.session_id: s for s in score_sessions(from_events(batch.events))}
    profiles = batch.manifest["bot_profiles"]

    for kind, minimum in (("aggressive", 0.95), ("stealth", 0.70)):
        ids = [sid for sid, k in profiles.items() if k == kind and sid in scores]
        caught = sum(1 for sid in ids if scores[sid].is_abusive(DEFAULT_THRESHOLD))
        assert caught / len(ids) >= minimum, f"{kind} 탐지율 {caught / len(ids):.2f}"


def test_normal_sessions_are_rarely_flagged():
    batch = generate_search(2000, seed=7, mix=SearchMix(bot_session_ratio=0.05, stealth_share=0.4))
    bots = set(batch.manifest["bot_sessions"])
    scores = score_sessions(from_events(batch.events))

    humans = [s for s in scores if s.session_id not in bots]
    flagged = sum(1 for s in humans if s.is_abusive(DEFAULT_THRESHOLD))
    assert flagged / len(humans) < 0.01, "정상 사용자 오탐이 1%를 넘으면 쓸 수 없다"


def test_features_are_deterministic_across_runs():
    a = score_sessions(from_events(generate_search(300, seed=3).events))
    b = score_sessions(from_events(generate_search(300, seed=3).events))
    assert sorted((s.session_id, s.score) for s in a) == sorted((s.session_id, s.score) for s in b)
