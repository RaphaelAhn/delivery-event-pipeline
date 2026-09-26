-- 일별 배치 품질 리포트. 날짜 × 점검 항목 1개 = 1행.
--
-- 점검 결과를 파일로만 남기면 "지난주에 언제 깨졌나"를 보려면 파일을 하나씩 열어야 한다.
-- 표로 쌓아 두면 대시보드(11일차 Grafana)가 날짜별 통과·실패를 바로 그릴 수 있다.
--
-- 볼륨이 이미 있으면 이 파일은 자동 실행되지 않는다.
-- 그래서 pipeline.quality.report 가 실행될 때마다 이 파일을 다시 적용한다 (IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS delivery.mart_quality_report
(
    run_date    Date,
    check_name  LowCardinality(String),
    passed      UInt8,
    observed    Float64,
    expected    String,             -- 사람이 읽는 기준 (예: "= session_rows", "<= 0.2")
    checked_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(checked_at)
ORDER BY (run_date, check_name);
