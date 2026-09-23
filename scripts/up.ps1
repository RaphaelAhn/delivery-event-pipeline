# 컨테이너를 띄우고 토픽을 만든다. 여러 번 실행해도 안전하다.
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

docker compose up -d --wait
if ($LASTEXITCODE -ne 0) { throw "docker compose up failed" }

foreach ($topic in @("orders.events", "dispatch.events", "delivery.events", "search.events")) {
    docker exec dep-kafka /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:9092 `
        --create --if-not-exists --topic $topic --partitions 3 --replication-factor 1
    if ($LASTEXITCODE -ne 0) { throw "failed to create topic $topic" }
}

docker exec dep-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# 원천 테이블. 볼륨이 이미 있으면 컨테이너의 자동 실행이 건너뛰어지므로 여기서도 적용한다.
$user = if ($env:CLICKHOUSE_USER) { $env:CLICKHOUSE_USER } else { "pipeline" }
$password = if ($env:CLICKHOUSE_PASSWORD) { $env:CLICKHOUSE_PASSWORD } else { "pipeline" }
foreach ($sql in Get-ChildItem clickhouse/init/*.sql | Sort-Object Name) {
    # cmd 의 리다이렉션으로 파일을 바이트 그대로 넘긴다.
    # PowerShell 파이프(Get-Content | docker)를 쓰면 한글 주석에서 인코딩이 깨지면서
    # 뒤따르는 줄이 통째로 사라진다 (컬럼 하나가 조용히 빠진 테이블이 만들어진다).
    cmd /c "docker exec -i dep-clickhouse clickhouse-client --user $user --password $password --multiquery < `"$($sql.FullName)`""
    if ($LASTEXITCODE -ne 0) { throw "failed to apply $($sql.Name)" }
}
Write-Host "ClickHouse tables ready"

Write-Host "Kafka UI: http://localhost:8080"
