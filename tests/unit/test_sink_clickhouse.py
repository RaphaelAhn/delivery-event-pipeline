"""적재 로직 테스트. ClickHouse 없이 행 변환과 배치 동작만 확인한다."""

import json
from datetime import UTC, datetime

import pytest

from pipeline.consumer.sink_clickhouse import (
    COLUMNS,
    ClickHouseSink,
    Origin,
    to_dlq_rows,
    to_row,
)
from pipeline.consumer.validate import validate

ORIGIN = Origin("orders.events", 1, 42)

ORDER_CREATED = {
    "schema_version": 1,
    "event_id": "evt-1",
    "event_type": "OrderCreated",
    "order_id": "O-100000",
    "event_time": "2026-01-01T00:36:00Z",
    "region_id": "R-SEOUL-01",
    "experiment_variant": "control",
}
ORDER_CANCELLED = {
    "schema_version": 1,
    "event_id": "evt-2",
    "event_type": "OrderCancelled",
    "order_id": "O-100000",
    "event_time": "2026-01-01T00:50:00Z",
    "cancel_reason": "no_rider",
}
DISPATCH_ACCEPTED = {
    "schema_version": 1,
    "event_id": "evt-3",
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
    "event_id": "evt-4",
    "event_type": "DeliveryCompleted",
    "order_id": "O-100000",
    "dispatch_id": "D-100000-2",
    "event_time": "2026-01-01T01:10:32Z",
}


class FakeClient:
    """insert 호출을 기록만 하는 가짜 ClickHouse 클라이언트."""

    def __init__(self):
        self.inserts = []
        self.closed = False

    def insert(self, table, rows, column_names):
        self.inserts.append((table, [list(row) for row in rows], list(column_names)))

    def close(self):
        self.closed = True


def cell(table, row, column):
    return row[COLUMNS[table].index(column)]


@pytest.mark.parametrize(
    ("event", "expected_table"),
    [
        (ORDER_CREATED, "raw_order_events"),
        (ORDER_CANCELLED, "raw_order_events"),
        (DISPATCH_ACCEPTED, "raw_dispatch_events"),
        (DELIVERY_COMPLETED, "raw_delivery_events"),
    ],
)
def test_each_event_type_goes_to_its_table(event, expected_table):
    table, row = to_row(event, ORIGIN, json.dumps(event))
    assert table == expected_table
    assert len(row) == len(COLUMNS[table])
    assert cell(table, row, "event_id") == event["event_id"]


def test_event_time_is_parsed_as_utc_datetime():
    table, row = to_row(ORDER_CREATED, ORIGIN, "{}")
    assert cell(table, row, "event_time") == datetime(2026, 1, 1, 0, 36, tzinfo=UTC)


def test_offset_time_is_converted_to_utc():
    event = {**DELIVERY_COMPLETED, "event_time": "2026-01-01T10:10:32+09:00"}
    table, row = to_row(event, ORIGIN, "{}")
    assert cell(table, row, "event_time") == datetime(2026, 1, 1, 1, 10, 32, tzinfo=UTC)


def test_optional_fields_become_none_not_missing():
    # 취소 이벤트에는 지역·실험군이 없다. 열 개수는 같아야 한다.
    table, row = to_row(ORDER_CANCELLED, ORIGIN, "{}")
    assert cell(table, row, "region_id") is None
    assert cell(table, row, "experiment_variant") is None
    assert cell(table, row, "cancel_reason") == "no_rider"


def test_rejected_dispatch_has_no_eta():
    event = {k: v for k, v in DISPATCH_ACCEPTED.items() if k != "eta_minutes"}
    event["result"] = "rejected"
    table, row = to_row(event, ORIGIN, "{}")
    assert cell(table, row, "eta_minutes") is None


def test_origin_and_raw_are_kept_for_replay():
    raw = json.dumps(ORDER_CREATED)
    table, row = to_row(ORDER_CREATED, ORIGIN, raw)
    assert cell(table, row, "topic") == "orders.events"
    assert cell(table, row, "partition") == 1
    assert cell(table, row, "offset") == 42
    assert json.loads(cell(table, row, "raw")) == ORDER_CREATED


def test_dlq_gets_one_row_per_violation():
    broken = {k: v for k, v in DELIVERY_COMPLETED.items() if k != "order_id"}
    broken["event_time"] = "2026-13-45T25:61:00Z"  # 위반 2건: 필수 칸 누락 + 잘못된 시각
    raw = json.dumps(broken)
    result = validate(raw)

    rows = to_dlq_rows(result, ORIGIN, raw)
    assert len(rows) == 2
    reasons = {cell("raw_dlq_events", row, "reason_code") for row in rows}
    assert reasons == {"schema_violation", "invalid_event_time"}
    assert all(cell("raw_dlq_events", row, "event_id") == "evt-4" for row in rows)


def test_dlq_row_survives_broken_json():
    # JSON 이 깨지면 event_id 를 알 수 없다. 그래도 원본과 사유는 남겨야 한다.
    raw = '{"event_id": "abc", '
    rows = to_dlq_rows(validate(raw), ORIGIN, raw)
    assert len(rows) == 1
    assert cell("raw_dlq_events", rows[0], "event_id") is None
    assert cell("raw_dlq_events", rows[0], "reason_code") == "invalid_json"
    assert cell("raw_dlq_events", rows[0], "raw") == raw


def test_sink_batches_until_flush():
    sink = ClickHouseSink(client=FakeClient())
    for event in (ORDER_CREATED, DISPATCH_ACCEPTED, DELIVERY_COMPLETED):
        table, row = to_row(event, ORIGIN, "{}")
        sink.add(table, row)

    assert sink.pending == 3
    assert sink.client.inserts == []  # flush 전에는 아무것도 보내지 않는다

    written = sink.flush()
    assert written == {
        "raw_order_events": 1,
        "raw_dispatch_events": 1,
        "raw_delivery_events": 1,
    }
    assert {table for table, _, _ in sink.client.inserts} == set(written)
    assert sink.pending == 0


def test_flush_is_safe_when_empty():
    sink = ClickHouseSink(client=FakeClient())
    assert sink.flush() == {}
    assert sink.client.inserts == []
