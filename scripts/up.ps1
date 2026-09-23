# 컨테이너를 띄우고 토픽을 만든다. 여러 번 실행해도 안전하다.
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

docker compose up -d --wait
if ($LASTEXITCODE -ne 0) { throw "docker compose up failed" }

foreach ($topic in @("orders.events", "dispatch.events", "delivery.events")) {
    docker exec dep-kafka /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:9092 `
        --create --if-not-exists --topic $topic --partitions 3 --replication-factor 1
    if ($LASTEXITCODE -ne 0) { throw "failed to create topic $topic" }
}

docker exec dep-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
Write-Host "Kafka UI: http://localhost:8080"
