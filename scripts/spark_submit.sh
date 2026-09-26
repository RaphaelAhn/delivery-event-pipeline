#!/usr/bin/env bash
# Spark 작업을 Docker 로 실행한다. 로컬에 Java 나 Spark 를 설치할 필요가 없다.
#
#   scripts/spark_submit.sh data/search_events.jsonl data/marts
#   scripts/spark_submit.sh "data/landing/search/*.jsonl" data/marts/dt=2026-09-25 --date 2026-09-25
set -euo pipefail
cd "$(dirname "$0")/.."

INPUT="${1:-data/search_events.jsonl}"
OUTPUT="${2:-data/marts}"
shift $(( $# < 2 ? $# : 2 ))   # 나머지 인자(--date 등)는 Spark 작업에 그대로 넘긴다
IMAGE="${SPARK_IMAGE:-apache/spark:3.5.3}"

docker run --rm \
  -v "$(pwd)":/work \
  -w /work \
  --user root \
  "$IMAGE" \
  /opt/spark/bin/spark-submit --master "local[*]" \
    --conf spark.ui.enabled=false \
    --conf spark.sql.session.timeZone=UTC \
    spark/jobs/search_session_metrics.py "$INPUT" "$OUTPUT" "$@"
