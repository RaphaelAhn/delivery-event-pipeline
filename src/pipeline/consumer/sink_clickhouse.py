"""검사를 통과한 이벤트와 불량 이벤트를 ClickHouse 에 배치로 적재한다.

이벤트 종류마다 테이블이 다르므로, 종류별로 행을 모았다가 한 번에 insert 한다.
(한 건씩 insert 하면 ClickHouse 에서는 매우 느리다. 조각 파일이 그만큼 생긴다.)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import clickhouse_connect

from pipeline.config import settings
from pipeline.consumer.validate import ValidationResult

TABLE_BY_EVENT_TYPE = {
    "OrderCreated": "raw_order_events",
    "OrderCancelled": "raw_order_events",
    "DispatchResult": "raw_dispatch_events",
    "DeliveryCompleted": "raw_delivery_events",
}
DLQ_TABLE = "raw_dlq_events"

COLUMNS = {
    "raw_order_events": [
        "event_id",
        "event_type",
        "order_id",
        "event_time",
        "schema_version",
        "region_id",
        "experiment_variant",
        "cancel_reason",
        "topic",
        "partition",
        "offset",
        "raw",
    ],
    "raw_dispatch_events": [
        "event_id",
        "event_type",
        "order_id",
        "dispatch_id",
        "rider_id",
        "result",
        "eta_minutes",
        "event_time",
        "schema_version",
        "topic",
        "partition",
        "offset",
        "raw",
    ],
    "raw_delivery_events": [
        "event_id",
        "event_type",
        "order_id",
        "dispatch_id",
        "event_time",
        "schema_version",
        "topic",
        "partition",
        "offset",
        "raw",
    ],
    DLQ_TABLE: [
        "topic",
        "partition",
        "offset",
        "event_id",
        "event_type",
        "reason_code",
        "field",
        "message",
        "raw",
    ],
}


@dataclass(frozen=True)
class Origin:
    """메시지가 Kafka 어디에서 왔는지. 재처리·추적에 쓴다."""

    topic: str
    partition: int
    offset: int


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(UTC)


def to_row(event: dict, origin: Origin, raw: str) -> tuple[str, list]:
    """합격 이벤트를 (테이블 이름, 행) 으로 바꾼다."""
    table = TABLE_BY_EVENT_TYPE[event["event_type"]]
    common = {
        "event_id": event["event_id"],
        "event_type": event["event_type"],
        "order_id": event["order_id"],
        "event_time": parse_time(event["event_time"]),
        "schema_version": event["schema_version"],
        "topic": origin.topic,
        "partition": origin.partition,
        "offset": origin.offset,
        "raw": raw,
    }
    if table == "raw_order_events":
        common |= {
            "region_id": event.get("region_id"),
            "experiment_variant": event.get("experiment_variant"),
            "cancel_reason": event.get("cancel_reason"),
        }
    elif table == "raw_dispatch_events":
        common |= {
            "dispatch_id": event["dispatch_id"],
            "rider_id": event["rider_id"],
            "result": event["result"],
            "eta_minutes": event.get("eta_minutes"),
        }
    else:
        common["dispatch_id"] = event["dispatch_id"]
    return table, [common[column] for column in COLUMNS[table]]


def to_dlq_rows(result: ValidationResult, origin: Origin, raw: str) -> list[list]:
    """불합격 이벤트를 위반 하나당 한 행으로 바꾼다. 사유를 종류별로 셀 수 있게 하기 위해서다."""
    event = result.event or {}
    event_type = event.get("event_type")
    return [
        [
            origin.topic,
            origin.partition,
            origin.offset,
            result.event_id,
            event_type if isinstance(event_type, str) else None,
            violation.code,
            violation.field,
            violation.message,
            raw,
        ]
        for violation in result.violations
    ]


class ClickHouseSink:
    """종류별로 행을 모았다가 flush() 에서 한 번에 넣는다."""

    def __init__(self, client=None) -> None:
        self.client = client or clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_db,
        )
        self.buffers: dict[str, list[list]] = {table: [] for table in COLUMNS}

    def add(self, table: str, row: list) -> None:
        self.buffers[table].append(row)

    def add_many(self, table: str, rows: list[list]) -> None:
        self.buffers[table].extend(rows)

    @property
    def pending(self) -> int:
        return sum(len(rows) for rows in self.buffers.values())

    def flush(self) -> dict[str, int]:
        """버퍼를 비우고 테이블별 적재 건수를 돌려준다."""
        written: dict[str, int] = {}
        for table, rows in self.buffers.items():
            if not rows:
                continue
            self.client.insert(table, rows, column_names=COLUMNS[table])
            written[table] = len(rows)
            rows.clear()
        return written

    def close(self) -> None:
        self.client.close()
