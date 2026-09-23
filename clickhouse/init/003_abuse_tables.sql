-- 어뷰징 탐지 결과. 세션 1개 = 1행.
--
-- 점수뿐 아니라 **신호별 값과 판정 사유**를 함께 남긴다.
-- "왜 이 세션이 막혔나"에 답할 수 없는 탐지는 운영에서 쓸 수 없다.
--
-- threshold 도 같이 저장한다. 나중에 임계값을 바꾸면 결과가 달라지는데,
-- 그때 "어떤 기준으로 판정한 결과인지"를 알 수 있어야 한다.

CREATE TABLE IF NOT EXISTS delivery.mart_session_abuse
(
    session_id   String,
    event_date   Date,
    score        Float64,             -- 0~1. 높을수록 어뷰징에 가깝다
    is_abusive   UInt8,
    speed        Float64,             -- 분당 검색 수가 집단에서 벗어난 정도
    regularity   Float64,             -- 검색 간격이 기계적으로 일정한 정도
    monotony     Float64,             -- 같은 검색어 반복 정도
    disinterest  Float64,             -- 클릭하지 않는 정도
    reasons      Array(String),
    threshold    Float64,
    scored_at    DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(scored_at)
PARTITION BY event_date
ORDER BY (event_date, session_id);
