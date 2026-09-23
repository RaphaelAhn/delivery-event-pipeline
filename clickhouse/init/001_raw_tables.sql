-- 원천 테이블: 들어온 이벤트를 "그대로" 보존한다.
-- 중복 제거와 부모-자식 판정은 여기서 하지 않고 dbt 에서 한다 (docs/decisions/0002).
--
-- 컨테이너가 처음 뜰 때 자동 실행되고, scripts/up 에서도 다시 적용한다.
-- (볼륨이 이미 있으면 자동 실행이 건너뛰어지므로 IF NOT EXISTS 로 두 경로를 모두 지원)

CREATE DATABASE IF NOT EXISTS delivery;

CREATE TABLE IF NOT EXISTS delivery.raw_order_events
(
    event_id            String,
    event_type          LowCardinality(String),
    order_id            String,
    event_time          DateTime64(3, 'UTC'),        -- 사건이 일어난 시각. 지표는 항상 이 값 기준
    schema_version      UInt8,
    region_id           Nullable(String),
    experiment_variant  Nullable(String),
    cancel_reason       Nullable(String),
    topic               LowCardinality(String),      -- 어디서 왔는지 (재처리·추적용)
    partition           UInt16,
    offset              UInt64,
    ingested_at         DateTime64(3, 'UTC') DEFAULT now64(3),  -- 우리가 받은 시각
    raw                 String                       -- 원본 JSON. 스키마가 바뀌어도 되돌릴 수 있게
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (order_id, event_time, event_id);

CREATE TABLE IF NOT EXISTS delivery.raw_dispatch_events
(
    event_id            String,
    event_type          LowCardinality(String),
    order_id            String,
    dispatch_id         String,
    rider_id            String,
    result              LowCardinality(String),      -- accepted / rejected
    eta_minutes         Nullable(Int32),             -- accepted 일 때만 존재
    event_time          DateTime64(3, 'UTC'),
    schema_version      UInt8,
    topic               LowCardinality(String),
    partition           UInt16,
    offset              UInt64,
    ingested_at         DateTime64(3, 'UTC') DEFAULT now64(3),
    raw                 String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (order_id, event_time, event_id);

CREATE TABLE IF NOT EXISTS delivery.raw_delivery_events
(
    event_id            String,
    event_type          LowCardinality(String),
    order_id            String,
    dispatch_id         String,
    event_time          DateTime64(3, 'UTC'),
    schema_version      UInt8,
    topic               LowCardinality(String),
    partition           UInt16,
    offset              UInt64,
    ingested_at         DateTime64(3, 'UTC') DEFAULT now64(3),
    raw                 String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (order_id, event_time, event_id);

-- 검색 로그. 공고 조직(다음 검색)이 실제로 다루는 도메인이다.
-- 세션 단위 분석과 어뷰징 탐지를 위해 session_id 를 정렬 키 앞쪽에 둔다.
CREATE TABLE IF NOT EXISTS delivery.raw_search_events
(
    event_id            String,
    event_type          LowCardinality(String),      -- SearchQuery / SearchResultClick
    session_id          String,
    query               String,
    result_count        Nullable(Int32),             -- SearchQuery 일 때만
    client              Nullable(String),
    rank                Nullable(Int32),             -- SearchResultClick 일 때만
    doc_id              Nullable(String),
    event_time          DateTime64(3, 'UTC'),
    schema_version      UInt8,
    topic               LowCardinality(String),
    partition           UInt16,
    offset              UInt64,
    ingested_at         DateTime64(3, 'UTC') DEFAULT now64(3),
    raw                 String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (session_id, event_time, event_id);

-- 불량품 보관함(DLQ): 계약을 어긴 이벤트를 사유와 함께 남긴다.
-- 버리지 않는 이유 - 무엇이 왜 실패했는지 세어야 품질 지표를 만들 수 있고, 고친 뒤 재처리할 수 있다.
CREATE TABLE IF NOT EXISTS delivery.raw_dlq_events
(
    ingested_at         DateTime64(3, 'UTC') DEFAULT now64(3),
    topic               LowCardinality(String),
    partition           UInt16,
    offset              UInt64,
    event_id            Nullable(String),            -- JSON 이 깨졌으면 알 수 없다
    event_type          Nullable(String),
    reason_code         LowCardinality(String),      -- invalid_json / schema_violation / ...
    field               Nullable(String),            -- 문제가 된 칸 이름
    message             String,
    raw                 String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (reason_code, ingested_at);
