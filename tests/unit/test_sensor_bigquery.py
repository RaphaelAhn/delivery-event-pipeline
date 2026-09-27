"""BigQuery 적재 테스트. 실제 GCP 대신 호출을 기록하는 가짜 클라이언트를 쓴다.

여기서 지키려는 것
- 날짜 파티션마다 WRITE_TRUNCATE 로 교체한다 (다시 적재해도 행이 늘지 않는다)
- 마트 JSONL 의 칸 이름이 테이블 스키마와 맞는다
"""

import json

from google.cloud import bigquery

from pipeline.producer.sensor_generator import generate_sensor
from pipeline.sensor.bigquery import TABLES, by_partition, load
from pipeline.sensor.excursion import clean, detect, write_marts


class FakeJob:
    def result(self):
        return self


class FakeClient:
    def __init__(self):
        self.loads = []  # (목적지, 행 수, 쓰기 방식)
        self.queries = []

    def create_dataset(self, dataset, exists_ok):
        assert exists_ok

    def create_table(self, table, exists_ok):
        assert exists_ok and table.time_partitioning is not None

    def load_table_from_json(self, rows, destination, job_config):
        self.loads.append((destination, len(rows), job_config.write_disposition))
        return FakeJob()

    def query(self, sql):
        self.queries.append(sql)
        return FakeJob()


def build_marts(tmp_path):
    batch = generate_sensor(40, seed=5)
    readings, _ = clean(json.dumps(e) for e in batch.events)
    write_marts(readings, *detect(readings), tmp_path)
    return tmp_path


def test_rows_are_split_by_utc_date():
    rows = [{"t": "2026-09-25T23:55:00Z"}, {"t": "2026-09-26T00:00:00Z"}]
    assert list(by_partition(rows, "t")) == ["20260925", "20260926"]


def test_mart_columns_match_the_table_schema(tmp_path):
    marts = build_marts(tmp_path)
    for name, (_, schema) in TABLES.items():
        lines = (marts / f"{name}.jsonl").read_text(encoding="utf-8").splitlines()
        assert lines, name
        assert {f.name for f in schema} <= set(json.loads(lines[0]))


def test_every_load_replaces_one_date_partition(tmp_path):
    client = FakeClient()
    loaded = load(build_marts(tmp_path), "demo-project", "cold_chain", client=client)
    assert loaded["sensor_readings"] > 0
    for destination, _, disposition in client.loads:
        assert "$" in destination  # 테이블 전체가 아니라 파티션 하나
        assert disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    assert (
        sum(n for d, n, _ in client.loads if ".sensor_readings$" in d) == loaded["sensor_readings"]
    )
    assert "v_daily_excursion_summary" in client.queries[0]


def test_reloading_issues_the_same_loads(tmp_path):
    marts = build_marts(tmp_path)
    first, second = FakeClient(), FakeClient()
    load(marts, "demo-project", "cold_chain", client=first)
    load(marts, "demo-project", "cold_chain", client=second)
    assert first.loads == second.loads
