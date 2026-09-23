-- 배달 완료 이벤트의 중복 제거. 기준은 stg_order_events 와 같다.

with ranked as (

    select
        event_id,
        event_type,
        order_id,
        dispatch_id,
        event_time,
        schema_version,
        topic,
        partition,
        offset,
        ingested_at,
        row_number() over (
            partition by event_id
            order by ingested_at asc, topic asc, partition asc, offset asc
        ) as arrival_rank
    from {{ source('raw', 'raw_delivery_events') }}

)

select
    event_id,
    event_type,
    order_id,
    dispatch_id,
    event_time,
    schema_version,
    topic,
    partition,
    offset,
    ingested_at
from ranked
where arrival_rank = 1
