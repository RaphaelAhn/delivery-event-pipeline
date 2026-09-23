-- 계약을 어겨 원천 테이블에 들어가지 못한 이벤트.
--
-- 왜 모델로 만드나: 버려진 이벤트는 그 주문의 지표에 구멍을 낸다.
-- 예) 배차 이벤트가 DLQ 로 빠지면, 배달이 완료됐는데도 "수락된 배차가 없는 주문"으로 보인다.
-- 어느 주문이 영향을 받았는지 알아야 지표를 해석할 수 있다.
--
-- order_id 는 원본 JSON 에서 꺼낸다. 필수 칸이 빠진 이벤트라 컬럼이 비어 있을 수 있다.

select
    event_id,
    nullIf(JSONExtractString(raw, 'order_id'), '') as order_id,
    event_type,
    reason_code,
    field,
    message,
    topic,
    partition,
    offset,
    ingested_at
from {{ source('raw', 'raw_dlq_events') }}
