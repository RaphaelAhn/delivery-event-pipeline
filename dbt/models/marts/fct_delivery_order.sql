-- 주문 1건 = 1행. 주문·배차·배달 이벤트를 주문번호로 이어 붙인다.
--
-- 여기서 하는 판정
--   1) 부모 없는 이벤트: 주문 생성 이벤트가 없는 배차·배달은 이 모델에 들어오지 않는다.
--      검사원이 버리지 않고 원천에 남겨 두었기 때문에, 조인 시점인 여기서 걸러진다.
--      (몇 건이 걸러졌는지는 orphan_events 테스트로 센다)
--   2) 주문의 최종 상태: delivered / cancelled / incomplete
--   3) ETA 오차: 실제 소요 시간 − 배차 시점의 예상 시간

with orders as (

    select
        order_id,
        min(event_time) as ordered_at,
        -- 주문 생성 이벤트의 값만 쓴다 (취소 이벤트에는 지역·실험군이 없다)
        argMin(region_id, event_time) as region_id,
        argMin(experiment_variant, event_time) as experiment_variant
    from {{ ref('stg_order_events') }}
    where event_type = 'OrderCreated'
    group by order_id

),

cancellations as (

    select
        order_id,
        min(event_time) as cancelled_at,
        argMin(cancel_reason, event_time) as cancel_reason
    from {{ ref('stg_order_events') }}
    where event_type = 'OrderCancelled'
    group by order_id

),

dispatches as (

    select
        order_id,
        count() as dispatch_attempts,
        countIf(result = 'rejected') as dispatch_rejections,
        min(event_time) as first_dispatch_at,
        -- 조건부 집계도 조건에 맞는 행이 없으면 NULL 이 아니라 기본값(1970-01-01)을 준다.
        -- 그대로 두면 "수락된 적 없는 주문"의 배달 소요 시간이 56년으로 계산된다.
        if(
            countIf(result = 'accepted') = 0,
            null,
            minIf(event_time, result = 'accepted')
        ) as accepted_at,
        argMaxIf(dispatch_id, event_time, result = 'accepted') as accepted_dispatch_id,
        argMaxIf(rider_id, event_time, result = 'accepted') as accepted_rider_id,
        argMaxIf(eta_minutes, event_time, result = 'accepted') as eta_minutes
    from {{ ref('stg_dispatch_events') }}
    group by order_id

),

deliveries as (

    select
        order_id,
        min(event_time) as delivered_at,
        argMin(dispatch_id, event_time) as delivery_dispatch_id
    from {{ ref('stg_delivery_events') }}
    group by order_id

),

-- 이 주문에서 계약 위반으로 버려진 이벤트가 몇 건인지. 지표의 신뢰도를 판단하는 데 쓴다.
rejected as (

    select
        order_id,
        uniqExact(event_id) as rejected_event_count,
        uniqExactIf(event_id, event_type = 'DispatchResult') as rejected_dispatch_count
    from {{ ref('stg_dlq_events') }}
    where order_id is not null
    group by order_id

)

-- ClickHouse 는 o.order_id 처럼 쓰면 컬럼 이름에 접두사가 그대로 남는다. 전부 별칭을 붙인다.
select
    o.order_id as order_id,
    o.ordered_at as ordered_at,
    toDate(o.ordered_at) as order_date,
    o.region_id as region_id,
    o.experiment_variant as experiment_variant,

    coalesce(d.dispatch_attempts, 0) as dispatch_attempts,
    coalesce(d.dispatch_rejections, 0) as dispatch_rejections,
    d.first_dispatch_at as first_dispatch_at,
    d.accepted_at as accepted_at,
    nullIf(d.accepted_dispatch_id, '') as accepted_dispatch_id,
    nullIf(d.accepted_rider_id, '') as accepted_rider_id,
    d.eta_minutes as eta_minutes,

    dl.delivered_at as delivered_at,
    c.cancelled_at as cancelled_at,
    nullIf(c.cancel_reason, '') as cancel_reason,

    -- 이 주문에서 계약 위반으로 버려진 이벤트 수. 0 보다 크면 아래 지표에 구멍이 있을 수 있다.
    coalesce(r.rejected_event_count, 0) as rejected_event_count,
    coalesce(r.rejected_dispatch_count, 0) as rejected_dispatch_count,

    -- 배차 성공 = 수락된 배차가 있는 주문
    d.accepted_at is not null as is_dispatch_accepted,

    multiIf(
        dl.delivered_at is not null, 'delivered',
        c.cancelled_at is not null, 'cancelled',
        'incomplete'
    ) as order_status,

    -- 지표. 완료되지 않은 주문은 null 로 두고 평균에서 제외한다
    if(
        d.first_dispatch_at is not null,
        dateDiff('second', o.ordered_at, d.first_dispatch_at) / 60.0,
        null
    ) as minutes_to_first_dispatch,
    if(
        dl.delivered_at is not null and d.accepted_at is not null,
        dateDiff('second', d.accepted_at, dl.delivered_at) / 60.0,
        null
    ) as actual_delivery_minutes,
    if(
        dl.delivered_at is not null and d.accepted_at is not null and d.eta_minutes is not null,
        dateDiff('second', d.accepted_at, dl.delivered_at) / 60.0 - d.eta_minutes,
        null
    ) as eta_error_minutes

from orders as o
left join dispatches as d on o.order_id = d.order_id
left join deliveries as dl on o.order_id = dl.order_id
left join cancellations as c on o.order_id = c.order_id
left join rejected as r on o.order_id = r.order_id

-- ClickHouse 의 LEFT JOIN 은 짝이 없을 때 NULL 이 아니라 "기본값"을 채운다.
-- 그대로 두면 배달되지 않은 주문의 delivered_at 이 1970-01-01 이 되어
-- 모든 주문이 '배달 완료'로 잡힌다. 다른 DB 처럼 NULL 이 되도록 켠다.
settings join_use_nulls = 1
