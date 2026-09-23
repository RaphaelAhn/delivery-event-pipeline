{{ config(severity = 'warn') }}

-- 배달이 완료됐는데 수락된 배차가 없는 주문.
--
-- 원인은 대부분 그 주문의 배차 이벤트가 계약 위반으로 DLQ 에 빠진 것이다.
-- rejected_dispatch_count 로 설명되는 건은 여기서 제외한다.
--
-- 그래도 남는 건이 있다: **주문번호(order_id) 칸 자체가 빠진 불량 이벤트**는
-- 어느 주문 것인지 알 수 없어 DLQ 를 주문에 귀속시킬 수 없다. 파이프라인의 오류가 아니라
-- 원천 데이터 품질의 사각지대이므로 실패(error)가 아닌 경고(warn)로 둔다.
-- 건수가 크게 늘면 수집 단계(클라이언트)에서 필수 칸이 빠지고 있다는 신호다.

select
    order_id,
    delivered_at,
    accepted_at,
    rejected_dispatch_count
from {{ ref('fct_delivery_order') }}
where order_status = 'delivered'
  and accepted_at is null
  and rejected_dispatch_count = 0
