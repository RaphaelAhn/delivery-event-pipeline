#!/usr/bin/env bash
# 컨테이너를 띄우고 토픽을 만든다. 여러 번 실행해도 안전하다.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose up -d --wait

for topic in orders.events dispatch.events delivery.events search.events; do
  docker exec dep-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
done

docker exec dep-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# 원천 테이블. 볼륨이 이미 있으면 컨테이너의 자동 실행이 건너뛰어지므로 여기서도 적용한다.
for sql in clickhouse/init/*.sql; do
  docker exec -i dep-clickhouse clickhouse-client \
    --user "${CLICKHOUSE_USER:-pipeline}" --password "${CLICKHOUSE_PASSWORD:-pipeline}" \
    --multiquery < "$sql"
done
echo "ClickHouse tables ready"

echo "Kafka UI: http://localhost:8080"
