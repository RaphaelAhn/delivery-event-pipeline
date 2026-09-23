-- 검색 로그의 중복 제거. 기준은 다른 staging 모델과 같다 (먼저 도착한 것만 유지).
--
-- 세션 단위 집계(7일차)와 어뷰징 탐지(8일차)가 이 모델을 입력으로 쓴다.

with ranked as (

    select
        event_id,
        event_type,
        session_id,
        query,
        result_count,
        client,
        rank,
        doc_id,
        event_time,
        schema_version,
        topic,
        partition,
        offset,
        ingested_at,
        -- 받은 시각 − 사건 시각. 수집 지연 지표의 원재료다.
        dateDiff('millisecond', event_time, ingested_at) / 1000.0 as ingest_lag_seconds,
        row_number() over (
            partition by event_id
            order by ingested_at asc, topic asc, partition asc, offset asc
        ) as arrival_rank
    from {{ source('raw', 'raw_search_events') }}

)

select
    event_id,
    event_type,
    session_id,
    query,
    result_count,
    client,
    rank,
    doc_id,
    event_time,
    toDate(event_time) as event_date,
    schema_version,
    topic,
    partition,
    offset,
    ingested_at,
    ingest_lag_seconds
from ranked
where arrival_rank = 1
