"""Kafka → 검사 → ClickHouse 적재.

순서가 중요하다.
    1) 메시지를 읽는다
    2) 검사한다 (validate.py)
    3) 합격은 원천 테이블, 불합격은 DLQ 로 배치 적재한다
    4) **적재에 성공한 뒤에** 오프셋을 커밋한다

4번을 3번보다 먼저 하면, 적재 중 죽었을 때 그 메시지를 영영 잃는다.
지금 순서에서는 죽어도 잃지 않는 대신 일부가 두 번 적재될 수 있다(at-least-once).
중복은 dbt 에서 event_id 로 제거한다 (docs/decisions/0002).
"""

from __future__ import annotations

import argparse
import signal
import time
from collections import Counter

from confluent_kafka import Consumer, KafkaError

from pipeline.common.topics import ALL_TOPICS
from pipeline.config import settings
from pipeline.consumer.sink_clickhouse import ClickHouseSink, Origin, to_dlq_rows, to_row
from pipeline.consumer.validate import validate

_stopping = False


def _request_stop(*_args) -> None:
    global _stopping
    _stopping = True


def build_consumer(bootstrap_servers: str, group_id: str, from_beginning: bool) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            # 적재 성공 후에 직접 커밋한다. 자동 커밋을 켜면 적재 전에 커밋될 수 있다.
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest" if from_beginning else "latest",
        }
    )


def consume(
    consumer: Consumer,
    sink: ClickHouseSink,
    batch_size: int = 1000,
    idle_timeout: float = 5.0,
) -> Counter:
    """새 메시지가 idle_timeout 초 동안 없으면 멈춘다 (배치 실행·데모용)."""
    stats: Counter = Counter()
    last_message_at = time.monotonic()
    uncommitted = 0

    def flush_and_commit() -> None:
        """적재를 먼저 끝내고 나서 커밋한다. 이 순서가 바뀌면 메시지를 잃을 수 있다."""
        nonlocal uncommitted
        if sink.pending:
            for table, count in sink.flush().items():
                stats[table] += count
        # 읽은 메시지가 없으면 커밋할 오프셋도 없다 (커밋하면 _NO_OFFSET 오류)
        if uncommitted:
            consumer.commit(asynchronous=False)
            uncommitted = 0

    while not _stopping:
        message = consumer.poll(1.0)
        if message is None:
            if time.monotonic() - last_message_at > idle_timeout:
                break
            continue
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise RuntimeError(message.error())

        last_message_at = time.monotonic()
        uncommitted += 1
        stats["consumed"] += 1
        payload = message.value()
        origin = Origin(message.topic(), message.partition(), message.offset())
        raw = payload.decode("utf-8", errors="replace")

        result = validate(payload)
        if result.ok:
            table, row = to_row(result.event, origin, raw)
            sink.add(table, row)
            stats["accepted"] += 1
        else:
            sink.add_many("raw_dlq_events", to_dlq_rows(result, origin, raw))
            stats["rejected"] += 1
            for violation in result.violations:
                stats[f"reason:{violation.code}"] += 1

        if sink.pending >= batch_size:
            flush_and_commit()

    flush_and_commit()
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume, validate and load events")
    parser.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--group-id", default="delivery-loader")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument(
        "--idle-timeout", type=float, default=5.0, help="이 시간(초) 동안 새 메시지가 없으면 종료"
    )
    parser.add_argument(
        "--from-beginning", action="store_true", help="이 그룹이 처음 읽을 때 맨 앞부터 읽는다"
    )
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    consumer = build_consumer(args.bootstrap_servers, args.group_id, args.from_beginning)
    consumer.subscribe(list(ALL_TOPICS))
    sink = ClickHouseSink()
    started = time.perf_counter()
    try:
        stats = consume(consumer, sink, args.batch_size, args.idle_timeout)
    finally:
        consumer.close()
        sink.close()

    elapsed = time.perf_counter() - started
    print(f"consumed : {stats['consumed']}")
    print(f"accepted : {stats['accepted']}")
    print(f"rejected : {stats['rejected']} (DLQ rows: {stats['raw_dlq_events']})")
    for key in sorted(k for k in stats if k.startswith("reason:")):
        print(f"  {key[7:]:<20} {stats[key]}")
    for table in ("raw_order_events", "raw_dispatch_events", "raw_delivery_events"):
        print(f"{table:<22} {stats[table]}")
    if elapsed > 0:
        print(f"elapsed  : {elapsed:.2f}s ({stats['consumed'] / elapsed:,.0f} events/s)")


if __name__ == "__main__":
    main()
