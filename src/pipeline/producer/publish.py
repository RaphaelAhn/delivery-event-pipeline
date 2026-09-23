"""JSONL 이벤트 파일을 도착 순서 그대로 Kafka 토픽에 발행한다.

- 토픽은 event_type 으로 결정한다 (common/topics.py).
- 메시지 key 는 order_id 다. 같은 주문의 이벤트는 같은 파티션으로 가서
  토픽 안에서는 순서가 유지된다. 단, 토픽이 다르면 순서 보장이 없다.
- order_id 가 없는 이벤트(계약 위반)도 key 없이 그대로 보낸다.
  걸러내는 것은 컨슈머 검증 단계의 책임이다.
"""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from pathlib import Path

from confluent_kafka import KafkaError, Message, Producer

from pipeline.common.topics import topic_for
from pipeline.config import settings


def build_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            # 브로커 응답을 받은 뒤 재시도해도 같은 메시지가 두 번 쓰이지 않게 한다.
            # generator 가 일부러 넣는 duplicate 는 event_id 만 같은 "다른 메시지"라
            # 여기서 막히지 않는다. 중복 제거는 다운스트림(dbt)의 책임이다.
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 20,
            "compression.type": "lz4",
        }
    )


def publish_file(path: Path, producer: Producer, rate: float = 0.0) -> Counter:
    sent: Counter = Counter()
    failed: list[str] = []

    def on_delivery(err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            failed.append(f"{msg.topic()}: {err}")

    interval = 1.0 / rate if rate > 0 else 0.0
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            event = json.loads(line)
            topic = topic_for(event["event_type"])
            key = event.get("order_id")
            producer.produce(
                topic,
                key=key.encode() if key else None,
                value=line.rstrip("\n").encode("utf-8"),
                on_delivery=on_delivery,
            )
            sent[topic] += 1
            # 내부 큐를 비우고 delivery 콜백을 처리한다
            producer.poll(0)
            if interval:
                time.sleep(interval)

    remaining = producer.flush(30)
    if remaining:
        raise RuntimeError(f"{remaining} messages were not delivered within 30s")
    if failed:
        raise RuntimeError(f"{len(failed)} messages failed: {failed[:5]}")
    return sent


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish a JSONL event file to Kafka")
    parser.add_argument("--input", type=Path, default=Path("data/events.jsonl"))
    parser.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--rate", type=float, default=0.0, help="초당 발행 건수 (0 이면 최대 속도)")
    args = parser.parse_args()

    started = time.perf_counter()
    sent = publish_file(args.input, build_producer(args.bootstrap_servers), args.rate)
    elapsed = time.perf_counter() - started
    total = sum(sent.values())
    for topic, count in sorted(sent.items()):
        print(f"{topic}: {count}")
    print(f"Published {total} events in {elapsed:.2f}s ({total / elapsed:,.0f} events/s)")


if __name__ == "__main__":
    main()
