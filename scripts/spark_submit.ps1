# Spark 작업을 Docker 로 실행한다. 로컬에 Java 나 Spark 를 설치할 필요가 없다.
#
#   .\scripts\spark_submit.ps1 data\search_events.jsonl data\marts
#   .\scripts\spark_submit.ps1 "data/landing/search/*.jsonl" data/marts/dt=2026-09-25 -Date 2026-09-25
#
# 매개변수 이름에 $Input 을 쓰지 않는다. PowerShell 의 예약 변수라 값이 덮여 실행이 실패한다.
param(
    [string]$InputPath = "data/search_events.jsonl",
    [string]$OutputDir = "data/marts",
    [string]$Date = ""
)
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

$image = if ($env:SPARK_IMAGE) { $env:SPARK_IMAGE } else { "apache/spark:3.5.3" }
$project = (Get-Location).Path
$extra = if ($Date) { @("--date", $Date) } else { @() }

docker run --rm `
    -v "${project}:/work" `
    -w /work `
    --user root `
    $image `
    /opt/spark/bin/spark-submit --master "local[*]" `
        --conf spark.ui.enabled=false `
        --conf spark.sql.session.timeZone=UTC `
        spark/jobs/search_session_metrics.py $InputPath $OutputDir @extra
if ($LASTEXITCODE -ne 0) { throw "spark-submit failed" }
