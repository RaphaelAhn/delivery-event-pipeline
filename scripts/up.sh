#!/usr/bin/env bash
# 컨테이너를 띄우고 토픽을 만든다. 여러 번 실행해도 안전하다.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose up -d --wait

for topic in orders.events dispatch.events delivery.events; do
  docker exec dep-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
done

docker exec dep-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
echo "Kafka UI: http://localhost:8080"
