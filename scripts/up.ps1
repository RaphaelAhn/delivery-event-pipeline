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

# 원천 테이블. 볼륨이 이미 있으면 컨테이너의 자동 실행이 건너뛰어지므로 여기서도 적용한다.
$user = if ($env:CLICKHOUSE_USER) { $env:CLICKHOUSE_USER } else { "pipeline" }
$password = if ($env:CLICKHOUSE_PASSWORD) { $env:CLICKHOUSE_PASSWORD } else { "pipeline" }
Get-Content clickhouse/init/001_raw_tables.sql -Raw | docker exec -i dep-clickhouse `
    clickhouse-client --user $user --password $password --multiquery
if ($LASTEXITCODE -ne 0) { throw "failed to create ClickHouse tables" }
Write-Host "ClickHouse tables ready"

Write-Host "Kafka UI: http://localhost:8080"
