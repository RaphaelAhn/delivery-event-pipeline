-- 주문 생성·취소 이벤트를 event_id 기준으로 중복 제거한다.
--
-- 왜 여기서 지우나: 컨슈머는 at-least-once 라서 같은 이벤트가 두 번 적재될 수 있고,
-- 생성기도 재전송 중복을 일부러 섞는다. 원천 테이블은 들어온 그대로 두고 여기서 한 건만 남긴다.
--
-- 동점 처리: 먼저 도착한 것을 남긴다. ingested_at 이 같은 밀리초일 수 있으므로
-- (topic, partition, offset) 까지 정렬 기준에 넣어 결과가 매번 같도록 고정한다.

with ranked as (

    select
        event_id,
        event_type,
        order_id,
        event_time,
        schema_version,
        region_id,
        experiment_variant,
        cancel_reason,
        topic,
        partition,
        offset,
        ingested_at,
        row_number() over (
            partition by event_id
            order by ingested_at asc, topic asc, partition asc, offset asc
        ) as arrival_rank
    from {{ source('raw', 'raw_order_events') }}

)

select
    event_id,
    event_type,
    order_id,
    event_time,
    schema_version,
    region_id,
    experiment_variant,
    cancel_reason,
    topic,
    partition,
    offset,
    ingested_at
from ranked
where arrival_rank = 1
