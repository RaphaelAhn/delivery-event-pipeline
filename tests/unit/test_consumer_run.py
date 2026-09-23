"""컨슈머 루프 테스트. Kafka 없이 가짜 컨슈머로 순서와 커밋 규칙만 확인한다.

여기서 지키려는 규칙
- 적재(flush)가 끝난 뒤에 커밋한다. 반대면 적재 중 죽었을 때 메시지를 잃는다.
- 읽은 메시지가 없으면 커밋하지 않는다 (Kafka 가 _NO_OFFSET 오류를 낸다).
"""

import json

from pipeline.consumer.run import consume
from pipeline.consumer.sink_clickhouse import ClickHouseSink

ORDER_CREATED = {
    "schema_version": 1,
    "event_id": "evt-1",
    "event_type": "OrderCreated",
    "order_id": "O-100000",
    "event_time": "2026-01-01T00:36:00Z",
    "region_id": "R-SEOUL-01",
    "experiment_variant": "control",
}


class FakeMessage:
    def __init__(self, payload: bytes, offset: int = 0):
        self._payload = payload
        self._offset = offset

    def value(self):
        return self._payload

    def error(self):
        return None

    def topic(self):
        return "orders.events"

    def partition(self):
        return 0

    def offset(self):
        return self._offset


class FakeConsumer:
    """메시지를 순서대로 돌려주고, 다 떨어지면 None 을 준다."""

    def __init__(self, messages):
        self.messages = list(messages)
        self.calls = []

    def poll(self, _timeout):
        return self.messages.pop(0) if self.messages else None

    def commit(self, asynchronous=False):
        self.calls.append("commit")


class RecordingClient:
    def __init__(self, consumer):
        self.consumer = consumer
        self.inserts = []

    def insert(self, table, rows, column_names):
        self.consumer.calls.append(f"insert:{table}")
        self.inserts.append((table, len(rows)))

    def close(self):
        pass


def event_bytes(event: dict) -> bytes:
    return json.dumps(event).encode("utf-8")


def test_valid_and_invalid_events_are_counted_and_routed():
    messages = [
        FakeMessage(event_bytes(ORDER_CREATED), 0),
        FakeMessage(b'{"broken": ', 1),
        FakeMessage(event_bytes({**ORDER_CREATED, "event_id": "evt-2"}), 2),
    ]
    consumer = FakeConsumer(messages)
    sink = ClickHouseSink(client=RecordingClient(consumer))

    stats = consume(consumer, sink, batch_size=100, idle_timeout=0)

    assert stats["consumed"] == 3
    assert stats["accepted"] == 2
    assert stats["rejected"] == 1
    assert stats["reason:invalid_json"] == 1
    assert stats["raw_order_events"] == 2
    assert stats["raw_dlq_events"] == 1


def test_commit_happens_only_after_insert():
    consumer = FakeConsumer([FakeMessage(event_bytes(ORDER_CREATED), 0)])
    sink = ClickHouseSink(client=RecordingClient(consumer))

    consume(consumer, sink, batch_size=100, idle_timeout=0)

    assert consumer.calls == ["insert:raw_order_events", "commit"]


def test_nothing_to_read_means_nothing_to_commit():
    # 같은 그룹으로 다시 실행했을 때의 상황. 커밋하면 Kafka 가 _NO_OFFSET 오류를 낸다.
    consumer = FakeConsumer([])
    sink = ClickHouseSink(client=RecordingClient(consumer))

    stats = consume(consumer, sink, batch_size=100, idle_timeout=0)

    assert stats["consumed"] == 0
    assert consumer.calls == []


def test_batch_size_triggers_intermediate_flush():
    messages = [
        FakeMessage(event_bytes({**ORDER_CREATED, "event_id": f"evt-{i}"}), i) for i in range(5)
    ]
    consumer = FakeConsumer(messages)
    sink = ClickHouseSink(client=RecordingClient(consumer))

    consume(consumer, sink, batch_size=2, idle_timeout=0)

    # 2건마다 적재+커밋, 마지막 1건은 종료 시 적재+커밋
    assert consumer.calls.count("insert:raw_order_events") == 3
    assert consumer.calls.count("commit") == 3
    assert sink.client.inserts == [
        ("raw_order_events", 2),
        ("raw_order_events", 2),
        ("raw_order_events", 1),
    ]
