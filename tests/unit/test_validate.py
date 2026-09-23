"""검사원(validate.py) 테스트.

규칙 순서대로 묶여 있다. 구현할 때 R1 → R2 → R3 → R4 → 채점 순으로 하나씩 통과시키면 된다.

    pytest tests/unit/test_validate.py -k R1      # R1 테스트만 실행
    pytest tests/unit/test_validate.py -x         # 처음 실패한 곳에서 멈춤
"""

import json

import pytest

from pipeline.consumer.validate import (
    INVALID_EVENT_TIME,
    INVALID_JSON,
    MISSING_EVENT_TYPE,
    NOT_AN_OBJECT,
    SCHEMA_VIOLATION,
    UNKNOWN_EVENT_TYPE,
    validate,
)
from pipeline.producer.generator import AnomalyRates, generate

# 계약을 지키는 쪽지 4종류. 테스트마다 복사해서 한 군데만 망가뜨린다.
ORDER_CREATED = {
    "schema_version": 1,
    "event_id": "evt-order-created",
    "event_type": "OrderCreated",
    "order_id": "O-100000",
    "event_time": "2026-01-01T00:36:00Z",
    "region_id": "R-SEOUL-01",
    "experiment_variant": "control",
}
ORDER_CANCELLED = {
    "schema_version": 1,
    "event_id": "evt-order-cancelled",
    "event_type": "OrderCancelled",
    "order_id": "O-100000",
    "event_time": "2026-01-01T00:50:00Z",
    "cancel_reason": "no_rider",
}
DISPATCH_ACCEPTED = {
    "schema_version": 1,
    "event_id": "evt-dispatch",
    "event_type": "DispatchResult",
    "order_id": "O-100000",
    "dispatch_id": "D-100000-2",
    "rider_id": "rider-184",
    "result": "accepted",
    "eta_minutes": 35,
    "event_time": "2026-01-01T00:40:19Z",
}
DELIVERY_COMPLETED = {
    "schema_version": 1,
    "event_id": "evt-delivery",
    "event_type": "DeliveryCompleted",
    "order_id": "O-100000",
    "dispatch_id": "D-100000-2",
    "event_time": "2026-01-01T01:10:32Z",
}


def raw(event: dict) -> bytes:
    """Kafka 에서 꺼낸 것처럼 bytes 로 만든다."""
    return json.dumps(event).encode("utf-8")


def without(event: dict, key: str) -> dict:
    return {k: v for k, v in event.items() if k != key}


def codes(result) -> list[str]:
    return [v.code for v in result.violations]


def fields(result) -> list[str | None]:
    return [v.field for v in result.violations]


# ---------------------------------------------------------------- R1. 읽을 수 있나


class TestR1Readable:
    def test_R1_broken_json_is_rejected(self):
        result = validate(b'{"event_id": "abc", ')
        assert not result.ok
        assert codes(result) == [INVALID_JSON]
        assert result.event is None

    def test_R1_non_utf8_bytes_are_rejected(self):
        result = validate(b"\xff\xfe\x00")
        assert codes(result) == [INVALID_JSON]

    @pytest.mark.parametrize("payload", [b"[1, 2, 3]", b"42", b'"hello"', b"null"])
    def test_R1_json_that_is_not_an_object_is_rejected(self, payload):
        result = validate(payload)
        assert codes(result) == [NOT_AN_OBJECT]
        assert result.event is None


# ---------------------------------------------------------------- R2. 아는 종류인가


class TestR2KnownType:
    def test_R2_missing_event_type_is_rejected(self):
        result = validate(raw(without(ORDER_CREATED, "event_type")))
        assert codes(result) == [MISSING_EVENT_TYPE]
        assert fields(result) == ["event_type"]

    def test_R2_unknown_event_type_is_rejected(self):
        result = validate(raw({**ORDER_CREATED, "event_type": "PizzaEaten"}))
        assert codes(result) == [UNKNOWN_EVENT_TYPE]
        assert fields(result) == ["event_type"]

    def test_R2_event_is_kept_even_when_rejected(self):
        # DLQ 에 원본을 남기려면 불합격이어도 읽어 둔 쪽지를 돌려줘야 한다
        result = validate(raw({**ORDER_CREATED, "event_type": "PizzaEaten"}))
        assert result.event["event_id"] == "evt-order-created"
        assert result.event_id == "evt-order-created"


# ---------------------------------------------------------------- R3. 양식에 맞나


class TestR3Contract:
    @pytest.mark.parametrize(
        "event", [ORDER_CREATED, ORDER_CANCELLED, DISPATCH_ACCEPTED, DELIVERY_COMPLETED]
    )
    def test_R3_valid_events_pass(self, event):
        result = validate(raw(event))
        assert result.ok, result.violations
        assert result.event == event

    def test_R3_str_input_is_accepted_too(self):
        assert validate(json.dumps(ORDER_CREATED)).ok

    def test_R3_rejected_dispatch_without_eta_passes(self):
        rejected = {**without(DISPATCH_ACCEPTED, "eta_minutes"), "result": "rejected"}
        assert validate(raw(rejected)).ok

    @pytest.mark.parametrize(
        ("event", "missing"),
        [
            (DELIVERY_COMPLETED, "order_id"),  # generator 의 missing_order_id
            (DELIVERY_COMPLETED, "dispatch_id"),  # generator 의 missing_dispatch_id
            (ORDER_CREATED, "event_id"),
            (ORDER_CREATED, "region_id"),  # OrderCreated 일 때만 필수 (if/then)
            (ORDER_CANCELLED, "cancel_reason"),  # OrderCancelled 일 때만 필수
            (DISPATCH_ACCEPTED, "eta_minutes"),  # accepted 일 때만 필수
        ],
    )
    def test_R3_missing_required_field_is_reported_by_name(self, event, missing):
        result = validate(raw(without(event, missing)))
        assert codes(result) == [SCHEMA_VIOLATION]
        assert fields(result) == [missing]

    @pytest.mark.parametrize(
        ("event", "key", "bad_value"),
        [
            (DISPATCH_ACCEPTED, "eta_minutes", 0),  # generator 의 non_positive_eta
            (DISPATCH_ACCEPTED, "eta_minutes", "35"),  # 숫자가 아니라 문자
            (DISPATCH_ACCEPTED, "result", "maybe"),
            (ORDER_CREATED, "experiment_variant", "treatmnt"),
            (ORDER_CREATED, "order_id", "ABC"),  # O-숫자 모양이 아님
            (ORDER_CREATED, "schema_version", 2),
        ],
    )
    def test_R3_wrong_value_is_reported_by_name(self, event, key, bad_value):
        result = validate(raw({**event, key: bad_value}))
        assert codes(result) == [SCHEMA_VIOLATION]
        assert fields(result) == [key]

    def test_R3_unexpected_field_is_rejected(self):
        # 오타 난 칸 이름. 어느 칸 문제인지 message 에 드러나야 한다
        result = validate(raw({**ORDER_CREATED, "order_idd": "O-1"}))
        assert codes(result) == [SCHEMA_VIOLATION]
        assert "order_idd" in result.violations[0].message

    def test_R3_all_violations_are_collected_not_just_the_first(self):
        broken = without(without(DELIVERY_COMPLETED, "order_id"), "dispatch_id")
        result = validate(raw(broken))
        assert codes(result) == [SCHEMA_VIOLATION, SCHEMA_VIOLATION]
        assert sorted(fields(result)) == ["dispatch_id", "order_id"]


# ---------------------------------------------------------------- R4. 시각이 진짜 시각인가


class TestR4EventTime:
    def test_R4_impossible_date_is_rejected(self):
        # generator 의 bad_event_time. JSON Schema 의 format 은 기본으로 검사되지 않아서
        # 양식 검사(R3)는 통과하고, 여기서 따로 잡아야 한다
        result = validate(raw({**DELIVERY_COMPLETED, "event_time": "2026-13-45T25:61:00Z"}))
        assert codes(result) == [INVALID_EVENT_TIME]
        assert fields(result) == ["event_time"]

    def test_R4_time_without_timezone_is_rejected(self):
        # 시간대가 없으면 한국 시각인지 UTC 인지 알 수 없다
        result = validate(raw({**DELIVERY_COMPLETED, "event_time": "2026-01-01T01:10:32"}))
        assert codes(result) == [INVALID_EVENT_TIME]

    def test_R4_time_with_offset_passes(self):
        assert validate(raw({**DELIVERY_COMPLETED, "event_time": "2026-01-01T10:10:32+09:00"})).ok

    def test_R4_non_string_time_is_a_schema_problem_only(self):
        # 문자열이 아니면 R3 에서 이미 잡힌다. R4 가 또 보고하지 않는다
        result = validate(raw({**DELIVERY_COMPLETED, "event_time": 1767229832}))
        assert codes(result) == [SCHEMA_VIOLATION]
        assert fields(result) == ["event_time"]


# ---------------------------------------------------------------- 채점: 정답지와 비교


EXPECTED_BY_KIND = {
    "missing_order_id": (SCHEMA_VIOLATION, "order_id"),
    "missing_dispatch_id": (SCHEMA_VIOLATION, "dispatch_id"),
    "non_positive_eta": (SCHEMA_VIOLATION, "eta_minutes"),
    "bad_event_time": (INVALID_EVENT_TIME, "event_time"),
}


@pytest.fixture(scope="module")
def batch():
    # 계약 위반을 5%로 넉넉히 섞고, 중복·지연·순서 뒤바뀜도 기본 비율로 함께 섞는다
    rates = AnomalyRates(duplicate=0.03, late=0.05, out_of_order=0.02, invalid=0.05)
    return generate(2000, seed=21, rates=rates)


class TestScoreAgainstManifest:
    def test_score_flagged_events_match_manifest_exactly(self, batch):
        results = [validate(raw(event)) for event in batch.events]
        flagged = {result.event_id for result in results if not result.ok}
        expected = {item["event_id"] for item in batch.manifest["injected"]["invalid"]}

        missed = expected - flagged  # 놓친 불량 (recall 을 깎음)
        false_alarm = flagged - expected  # 멀쩡한데 불합격 (precision 을 깎음)
        assert not missed, f"놓친 불량 {len(missed)}개"
        assert not false_alarm, f"잘못 잡은 정상 {len(false_alarm)}개"

    def test_score_each_kind_gets_the_expected_reason(self, batch):
        by_id = {event["event_id"]: event for event in batch.events}
        for item in batch.manifest["injected"]["invalid"]:
            result = validate(raw(by_id[item["event_id"]]))
            assert (codes(result), fields(result)) == (
                [EXPECTED_BY_KIND[item["kind"]][0]],
                [EXPECTED_BY_KIND[item["kind"]][1]],
            ), item

    def test_score_late_duplicate_and_out_of_order_events_are_not_rejected(self, batch):
        # 검사원은 쪽지 한 장만 보고 판단한다. 이 셋은 내용이 정상이라 통과해야 한다
        by_id = {event["event_id"]: event for event in batch.events}
        injected = batch.manifest["injected"]
        invalid_ids = {item["event_id"] for item in injected["invalid"]}
        for kind in ("late", "out_of_order", "duplicate"):
            for event_id in injected[kind]:
                if event_id in invalid_ids:
                    continue  # 계약 위반 쪽지가 중복으로 한 번 더 온 경우
                assert validate(raw(by_id[event_id])).ok, (kind, event_id)
