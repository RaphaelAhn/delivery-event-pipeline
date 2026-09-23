{{ config(severity = 'warn') }}

-- 부모(주문 생성)가 없는 배차·배달 이벤트를 센다.
--
-- 검사원은 이런 이벤트를 버리지 않는다. 토픽이 달라 부모가 늦게 도착할 수 있기 때문이다.
-- 판정은 여기서 한다. 다만 "곧 도착할 부모"와 "영영 없는 부모"는 구분할 수 없으므로
-- 실패(error)가 아니라 경고(warn)로 둔다. 건수가 늘면 수집 쪽을 살펴봐야 한다.

with orders as (
    select distinct order_id from {{ ref('stg_order_events') }} where event_type = 'OrderCreated'
),

children as (
    select event_id, order_id, 'dispatch' as source from {{ ref('stg_dispatch_events') }}
    union all
    select event_id, order_id, 'delivery' as source from {{ ref('stg_delivery_events') }}
)

select c.event_id, c.order_id, c.source
from children as c
left anti join orders as o on c.order_id = o.order_id
