"""검색 로그 일별 배치: dbt build → Spark 집계 → 마트 적재 → 이상 탐지 → 품질 리포트.

하루 = DAG 실행 1번. 처리할 날짜는 data interval 의 시작일(`ds`)이다.
9/26 00:00 UTC 에 도는 실행은 [9/25 00:00, 9/26 00:00) 구간, 즉 9/25 하루치를 처리한다.

모든 단계가 **날짜 단위 덮어쓰기**라서 같은 날짜를 몇 번 다시 돌려도 결과가 한 벌만 남는다.
그래서 늦게 도착한 이벤트가 착지 파일에 추가되면 그 날짜를 백필로 다시 돌리면 된다.

    # 어제 하루 다시 처리 (스케줄러가 실행)
    airflow backfill create --dag-id search_daily_batch \
        --from-date 2026-09-25 --to-date 2026-09-25 --reprocess-behavior completed

    # 날짜를 직접 지정해 수동 실행
    airflow dags trigger search_daily_batch -c '{"run_date": "2026-09-25"}'
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param
from airflow.timetables.interval import CronDataIntervalTimetable

log = logging.getLogger(__name__)

PROJECT = "/opt/project"
VENV = "/opt/pipeline-venv/bin"
LANDING = os.getenv("SEARCH_LANDING_PATH", "data/landing/search/*.jsonl")

# 수동 실행에서 run_date 를 주면 그 날짜, 아니면 data interval 시작일
RUN_DATE = "{{ params.run_date or ds }}"


def notify_failure(context) -> None:
    """재시도까지 모두 실패한 작업을 알린다.

    알림은 두 곳으로 보낸다.
      - data/alerts/failures.jsonl : 항상 남는 로컬 기록 (웹훅이 없어도 확인 가능)
      - ALERT_WEBHOOK_URL          : 설정돼 있으면 Slack 호환 웹훅으로 전송
    알림이 실패해도 원래 실패 원인을 가리지 않도록 예외를 삼킨다.
    """
    ti = context["ti"]
    alert = {
        "at": datetime.now(UTC).isoformat(timespec="seconds"),
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": context["run_id"],
        "try_number": ti.try_number,
        "run_date": context["params"].get("run_date") or context.get("ds"),
        "error": str(context.get("exception")),
    }
    text = (
        f"[{alert['dag_id']}] {alert['task_id']} 실패 "
        f"(run_date={alert['run_date']}, try={alert['try_number']}): {alert['error']}"
    )
    log.error(text)
    try:
        path = Path(PROJECT) / "data" / "alerts" / "failures.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(alert, ensure_ascii=False) + "\n")

        url = os.getenv("ALERT_WEBHOOK_URL")
        if url:
            request = urllib.request.Request(
                url,
                data=json.dumps({"text": text}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(request, timeout=10)
    except Exception:
        log.exception("failed to deliver the failure alert")


with DAG(
    dag_id="search_daily_batch",
    description="dbt build → Spark 집계 → 이상 탐지 → 품질 리포트 (날짜 단위, 백필 가능)",
    doc_md=__doc__,
    # 00:00 UTC 에 전날 구간을 처리한다. data interval 기반이라 ds = 처리 대상 날짜
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=datetime(2026, 9, 20, tzinfo=UTC),
    catchup=False,
    # 같은 테이블을 날짜별로 지우고 쓰므로 실행을 겹치지 않는다
    max_active_runs=1,
    params={
        "run_date": Param(
            None,
            type=["null", "string"],
            # bash 명령에 그대로 들어가므로 날짜 형식만 허용한다
            pattern=r"^\d{4}-\d{2}-\d{2}$",
            description="처리할 날짜(UTC, YYYY-MM-DD). 비우면 data interval 시작일",
        )
    },
    default_args={
        "owner": "data-platform",
        # 컨테이너가 막 뜬 직후의 접속 실패 같은 일시적 오류를 흡수한다.
        # 운영이라면 간격을 더 길게 두지만, 로컬 시연에서는 1분이면 충분하다.
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "on_failure_callback": notify_failure,
        "cwd": PROJECT,
    },
    tags=["search", "daily", "portfolio"],
) as dag:
    # 배달 이벤트 staging → fct_delivery_order 와 dbt 테스트.
    # 모델이 table/view 로 매번 새로 만들어지므로 다시 돌려도 결과가 같다.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "cd dbt && "
            f"([ -d dbt_packages/dbt_utils ] || {VENV}/dbt deps) && "
            f"{VENV}/dbt build --profiles-dir . --target-path target/airflow"
        ),
        execution_timeout=timedelta(minutes=20),
    )

    spark_aggregate = BashOperator(
        task_id="spark_aggregate",
        bash_command=(
            f"{VENV}/spark-submit --master 'local[2]' --driver-memory 1g "
            "--conf spark.ui.enabled=false --conf spark.sql.session.timeZone=UTC "
            f"spark/jobs/search_session_metrics.py '{LANDING}' "
            f"data/batch/marts/dt={RUN_DATE} --date {RUN_DATE}"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    load_marts = BashOperator(
        task_id="load_marts",
        bash_command=(
            f"{VENV}/python -m pipeline.marts.load_search_marts "
            f"--input data/batch/marts/dt={RUN_DATE} --date {RUN_DATE}"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    detect_abuse = BashOperator(
        task_id="detect_abuse",
        bash_command=f"{VENV}/python -m pipeline.detect.run_detection --date {RUN_DATE}",
        execution_timeout=timedelta(minutes=10),
    )

    quality_report = BashOperator(
        task_id="quality_report",
        bash_command=f"{VENV}/python -m pipeline.quality.report --date {RUN_DATE}",
        # 품질 실패는 데이터 문제라 다시 돌려도 같은 결과가 나온다. 바로 알림으로 넘긴다.
        retries=0,
        execution_timeout=timedelta(minutes=5),
    )

    dbt_build >> spark_aggregate >> load_marts >> detect_abuse >> quality_report
