from collections import Counter, defaultdict
from datetime import datetime

import pytest

from pipeline.common.topics import topic_for
from pipeline.producer.generator import AnomalyRates, generate, write_batch

NO_ANOMALIES = AnomalyRates(duplicate=0, late=0, out_of_order=0, invalid=0)


def parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def test_same_seed_produces_identical_batches():
    assert generate(200, seed=7).events == generate(200, seed=7).events


def test_different_seed_produces_different_batches():
    assert generate(200, seed=7).events != generate(200, seed=8).events


def test_clean_batch_follows_order_lifecycle():
    events = generate(500, seed=1, rates=NO_ANOMALIES).events
    by_order = defaultdict(list)
    for event in events:
        by_order[event["order_id"]].append(event)

    assert len(by_order) == 500
    for order_id, order_events in by_order.items():
        types = Counter(e["event_type"] for e in order_events)
        assert types["OrderCreated"] == 1, order_id
        # 배달 완료와 취소 중 정확히 하나로 끝난다
        assert types["DeliveryCompleted"] + types["OrderCancelled"] == 1, order_id

        created = next(e for e in order_events if e["event_type"] == "OrderCreated")
        for event in order_events:
            assert parse(event["event_time"]) >= parse(created["event_time"]), event

        accepted = [e for e in order_events if e.get("result") == "accepted"]
        assert len(accepted) <= 1
        for delivery in (e for e in order_events if e["event_type"] == "DeliveryCompleted"):
            assert accepted and delivery["dispatch_id"] == accepted[0]["dispatch_id"]


def test_clean_batch_has_unique_event_ids():
    events = generate(500, seed=1, rates=NO_ANOMALIES).events
    assert len({e["event_id"] for e in events}) == len(events)


def test_every_event_maps_to_a_topic():
    for event in generate(300, seed=3).events:
        topic_for(event["event_type"])


def test_duplicates_reuse_event_id_and_match_manifest():
    batch = generate(
        1000, seed=5, rates=AnomalyRates(duplicate=0.1, late=0, out_of_order=0, invalid=0)
    )
    counts = Counter(e["event_id"] for e in batch.events)
    duplicated = {event_id for event_id, n in counts.items() if n > 1}

    assert duplicated == set(batch.manifest["injected"]["duplicate"])
    manifest_counts = batch.manifest["counts"]
    assert manifest_counts["emitted_events"] == manifest_counts["clean_events"] + len(duplicated)


def test_out_of_order_events_arrive_before_their_order():
    batch = generate(
        1000, seed=9, rates=AnomalyRates(duplicate=0, late=0, out_of_order=0.1, invalid=0)
    )
    position = {e["event_id"]: i for i, e in enumerate(batch.events)}
    created_position = {
        e["order_id"]: position[e["event_id"]]
        for e in batch.events
        if e["event_type"] == "OrderCreated"
    }
    by_id = {e["event_id"]: e for e in batch.events}

    injected = batch.manifest["injected"]["out_of_order"]
    assert injected
    for event_id in injected:
        event = by_id[event_id]
        assert position[event_id] < created_position[event["order_id"]]
        # 도착만 빠를 뿐 event_time 은 여전히 주문 생성 이후다
        created = batch.events[created_position[event["order_id"]]]
        assert parse(event["event_time"]) >= parse(created["event_time"])


def test_invalid_events_break_the_contract():
    batch = generate(
        2000, seed=11, rates=AnomalyRates(duplicate=0, late=0, out_of_order=0, invalid=0.1)
    )
    by_id = {e["event_id"]: e for e in batch.events}
    injected = batch.manifest["injected"]["invalid"]
    assert injected
    for item in injected:
        event = by_id[item["event_id"]]
        kind = item["kind"]
        if kind == "missing_order_id":
            assert "order_id" not in event
        elif kind == "missing_dispatch_id":
            assert "dispatch_id" not in event
        elif kind == "non_positive_eta":
            assert event["eta_minutes"] <= 0
        else:
            with pytest.raises(ValueError):
                parse(event["event_time"])


def test_injected_rates_are_close_to_requested():
    rates = AnomalyRates(duplicate=0.05, late=0.05, out_of_order=0.05, invalid=0.05)
    batch = generate(5000, seed=13, rates=rates)
    counts = batch.manifest["counts"]
    # 자식 이벤트(주문 생성 제외) 기준으로 late/out_of_order/invalid 가 적용된다
    children = counts["clean_events"] - 5000
    for name in ("late", "out_of_order", "invalid"):
        assert counts[name] / children == pytest.approx(0.05, abs=0.01), name
    assert counts["duplicate"] / counts["clean_events"] == pytest.approx(0.05, abs=0.01)


@pytest.mark.parametrize(
    "rates",
    [
        {"duplicate": -0.1},
        {"late": 1.5},
        {"late": 0.5, "out_of_order": 0.4, "invalid": 0.2},
    ],
)
def test_invalid_rates_are_rejected(rates):
    with pytest.raises(ValueError):
        AnomalyRates(**rates)


def test_write_batch_creates_events_and_manifest(tmp_path):
    batch = generate(50, seed=2)
    output = tmp_path / "events.jsonl"
    manifest_path = write_batch(batch, output)

    assert len(output.read_text(encoding="utf-8").splitlines()) == len(batch.events)
    assert manifest_path.name == "events.manifest.json"
    assert manifest_path.exists()
