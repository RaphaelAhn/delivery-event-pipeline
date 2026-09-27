"""센서 마트를 BigQuery 에 적재한다.

    python -m pipeline.sensor.bigquery --project <GCP 프로젝트 ID>

pipeline.sensor.excursion 이 만든 JSONL 세 개를 날짜 파티션 테이블에 넣는다.
    sensor_readings         정제된 측정값 (중복 제거·정렬 후)
    temperature_excursions  이탈 한 건 = 한 행
    sensor_gaps             공백 한 건 = 한 행
    v_daily_excursion_summary  날짜·화물 종류·구간별 이탈 요약 (서비스·분석이 읽는 뷰)

멱등성: 날짜마다 `테이블$YYYYMMDD` 파티션을 WRITE_TRUNCATE 로 **통째로 교체**한다.
같은 날짜를 다시 적재해도 행이 늘지 않는다 (ClickHouse 쪽 Airflow 백필과 같은 원칙).

인증: `gcloud auth application-default login` 또는
환경 변수 GOOGLE_APPLICATION_CREDENTIALS=<서비스 계정 키 JSON 경로>.
BigQuery 샌드박스(결제 수단 없이 무료)에서도 적재 작업과 뷰 생성은 된다. 테이블은 60일 뒤 만료된다.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

from google.cloud import bigquery

LOCATION = "asia-northeast3"  # 서울
S = bigquery.SchemaField

TABLES = {
    "sensor_readings": (
        "event_time",
        [
            S("event_id", "STRING", mode="REQUIRED"),
            S("shipment_id", "STRING", mode="REQUIRED"),
            S("device_id", "STRING", mode="REQUIRED"),
            S("cargo_type", "STRING", mode="REQUIRED"),
            S("leg", "STRING", mode="REQUIRED"),
            S("temperature_c", "FLOAT64", mode="REQUIRED"),
            S("event_time", "TIMESTAMP", mode="REQUIRED"),
        ],
    ),
    "temperature_excursions": (
        "start",
        [
            S("shipment_id", "STRING", mode="REQUIRED"),
            S("cargo_type", "STRING", mode="REQUIRED"),
            S("leg", "STRING", mode="REQUIRED"),
            S("start", "TIMESTAMP", mode="REQUIRED"),
            S("end", "TIMESTAMP", mode="REQUIRED"),
            S("duration_minutes", "INT64", mode="REQUIRED"),
            S("readings", "INT64", mode="REQUIRED"),
            S("peak_c", "FLOAT64", mode="REQUIRED"),
        ],
    ),
    "sensor_gaps": (
        "after",
        [
            S("shipment_id", "STRING", mode="REQUIRED"),
            S("after", "TIMESTAMP", mode="REQUIRED"),
            S("before", "TIMESTAMP", mode="REQUIRED"),
            S("missing_minutes", "INT64", mode="REQUIRED"),
        ],
    ),
}

SUMMARY_VIEW = """
CREATE OR REPLACE VIEW `{dataset}.v_daily_excursion_summary` AS
SELECT
  DATE(`start`) AS event_date,
  cargo_type,
  leg,
  COUNT(*) AS excursions,
  COUNT(DISTINCT shipment_id) AS shipments_affected,
  SUM(duration_minutes) AS total_minutes,
  MAX(peak_c) AS peak_c
FROM `{dataset}.temperature_excursions`
GROUP BY event_date, cargo_type, leg
"""


def by_partition(rows: list[dict], field: str) -> dict[str, list[dict]]:
    """행을 날짜 파티션(YYYYMMDD)별로 나눈다. 시각은 UTC ISO 문자열이라 앞 10자가 날짜다."""
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        groups[row[field][:10].replace("-", "")].append(row)
    return dict(groups)


def load(marts_dir: Path, project: str, dataset: str, client=None) -> dict[str, int]:
    """테이블마다 적재한 행 수를 돌려준다."""
    client = client or bigquery.Client(project=project, location=LOCATION)
    dataset_id = f"{project}.{dataset}"
    client.create_dataset(bigquery.Dataset(dataset_id), exists_ok=True)

    loaded: dict[str, int] = {}
    for name, (partition_field, schema) in TABLES.items():
        table = bigquery.Table(f"{dataset_id}.{name}", schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(field=partition_field)
        client.create_table(table, exists_ok=True)

        path = marts_dir / f"{name}.jsonl"
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        wanted = {field.name for field in schema}
        rows = [{k: v for k, v in row.items() if k in wanted} for row in rows]

        config = bigquery.LoadJobConfig(
            schema=schema, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        for day, day_rows in sorted(by_partition(rows, partition_field).items()):
            destination = f"{dataset_id}.{name}${day}"
            client.load_table_from_json(day_rows, destination, job_config=config).result()
        loaded[name] = len(rows)

    client.query(SUMMARY_VIEW.format(dataset=dataset_id)).result()
    return loaded


def main() -> None:
    parser = argparse.ArgumentParser(description="센서 마트 → BigQuery")
    parser.add_argument("--project", required=True, help="GCP 프로젝트 ID")
    parser.add_argument("--dataset", default="cold_chain")
    parser.add_argument("--marts", type=Path, default=Path("data/sensor_marts"))
    args = parser.parse_args()

    loaded = load(args.marts, args.project, args.dataset)
    for name, count in loaded.items():
        print(f"{args.project}.{args.dataset}.{name}: {count} rows")
    print(f"View: {args.project}.{args.dataset}.v_daily_excursion_summary")


if __name__ == "__main__":
    main()
