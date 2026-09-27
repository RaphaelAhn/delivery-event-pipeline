"""이벤트 타입 → Kafka 토픽 매핑. 토픽 목록은 scripts/up 과 맞춰야 한다."""

from __future__ import annotations

ORDERS_TOPIC = "orders.events"
DISPATCH_TOPIC = "dispatch.events"
DELIVERY_TOPIC = "delivery.events"
SEARCH_TOPIC = "search.events"
# 센서 시나리오는 아직 Kafka·ClickHouse 경로에 연결하지 않았다.
# 그래서 ALL_TOPICS(컨슈머 구독 목록)에 넣지 않는다.
# 지금은 파일 → 검사 → 이탈 탐지 → BigQuery 배치로만 흐른다 (pipeline.sensor).
SENSOR_TOPIC = "sensor.events"

ALL_TOPICS = (ORDERS_TOPIC, DISPATCH_TOPIC, DELIVERY_TOPIC, SEARCH_TOPIC)

_TOPIC_BY_EVENT_TYPE = {
    "OrderCreated": ORDERS_TOPIC,
    "OrderCancelled": ORDERS_TOPIC,
    "DispatchResult": DISPATCH_TOPIC,
    "DeliveryCompleted": DELIVERY_TOPIC,
    "SearchQuery": SEARCH_TOPIC,
    "SearchResultClick": SEARCH_TOPIC,
    "SensorReading": SENSOR_TOPIC,
}


def topic_for(event_type: str) -> str:
    try:
        return _TOPIC_BY_EVENT_TYPE[event_type]
    except KeyError:
        raise ValueError(f"unknown event_type: {event_type!r}") from None
