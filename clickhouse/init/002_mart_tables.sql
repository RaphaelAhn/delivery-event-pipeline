-- Spark 가 계산한 검색 지표를 담는 마트 테이블.
--
-- 왜 Spark 결과를 여기에 넣나: 집계는 Spark 가 대용량으로 하고, 조회는 ClickHouse 가 빠르게 한다.
-- 대시보드와 이상 탐지(8일차)는 이 테이블만 보면 된다.
--
-- ReplacingMergeTree: 같은 키로 다시 적재하면 최신 버전만 남는다.
-- 배치를 다시 돌려도(백필) 행이 두 배가 되지 않는다 = 멱등성.

CREATE TABLE IF NOT EXISTS delivery.mart_search_session
(
    session_id            String,
    event_date            Date,
    queries               UInt32,
    distinct_queries      UInt32,
    clicks                UInt32,
    click_rate            Float64,
    distinct_query_ratio  Float64,
    avg_gap_seconds       Nullable(Float64),   -- 검색 1회뿐인 세션은 간격이 없다
    min_gap_seconds       Nullable(Float64),
    stddev_gap_seconds    Nullable(Float64),   -- 0 에 가까우면 기계적인 규칙성
    avg_click_rank        Nullable(Float64),
    span_seconds          Int64,
    queries_per_minute    Nullable(Float64),
    client                Nullable(String),
    first_query_at        DateTime64(3, 'UTC'),
    last_query_at         DateTime64(3, 'UTC'),
    loaded_at             DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY event_date
ORDER BY (event_date, session_id);

CREATE TABLE IF NOT EXISTS delivery.mart_search_daily
(
    event_date        Date,
    sessions          UInt64,
    queries           UInt64,
    clicks            UInt64,
    distinct_queries  UInt64,
    ctr               Nullable(Float64),
    loaded_at         DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY event_date;

CREATE TABLE IF NOT EXISTS delivery.mart_search_top_query
(
    event_date  Date,
    rank        UInt16,
    query       String,
    queries     UInt64,
    sessions    UInt64,
    clicks      UInt64,
    ctr         Float64,
    loaded_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY event_date
ORDER BY (event_date, rank);
