-- 주문보다 먼저 일어난 배차·배달은 있을 수 없다.
-- 도착 순서가 뒤바뀌는 것(out of order)은 정상이지만, event_time 자체가 앞서면 데이터 오류다.

select
    order_id,
    ordered_at,
    first_dispatch_at,
    delivered_at
from {{ ref('fct_delivery_order') }}
where first_dispatch_at < ordered_at
   or delivered_at < ordered_at
