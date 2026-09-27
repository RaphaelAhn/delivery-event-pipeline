"""콜드체인 센서 시나리오 테스트.

여기서 지키려는 것
- 생성된 측정값이 계약(sensor_event.schema.json)을 지킨다
- 15분 이상 연속 이탈만 이탈이고, 한 번 튄 값은 아니다
- 측정이 없는 구간은 공백으로 보고하고, 늦게 올라온 측정은 공백이 아니다
- 도착 순서가 섞이고 재전송이 섞여도 결과가 같다
"""

import json
import random
from datetime import UTC, datetime, timedelta

from pipeline.consumer.validate import validate
from pipeline.producer.generator import AnomalyRates
from pipeline.producer.sensor_generator import generate_sensor
from pipeline.sensor.excursion import clean, detect, score

NO_TRANSPORT_ISSUES = AnomalyRates(duplicate=0, late=0, out_of_order=0, invalid=0)
T0 = datetime(2026, 9, 25, 0, 0, tzinfo=UTC)


def reading(minute: int, temperature: float, shipment="SH-1", event_id=None) -> str:
    return json.dumps(
        {
            "schema_version": 1,
            "event_id": event_id or f"{shipment}-{minute}",
            "event_type": "SensorReading",
            "shipment_id": shipment,
            "device_id": "D-00ff",
            "cargo_type": "pharma",
            "leg": "inland",
            "temperature_c": temperature,
            "event_time": (T0 + timedelta(minutes=minute)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
    )


def run(lines):
    readings, rejected = clean(lines)
    return (*detect(readings), rejected)


def test_same_seed_produces_identical_batches():
    assert generate_sensor(30, seed=7).events == generate_sensor(30, seed=7).events


def test_every_clean_reading_passes_the_contract():
    for event in generate_sensor(40, seed=3, rates=NO_TRANSPORT_ISSUES).events:
        result = validate(json.dumps(event))
        assert result.ok, (event, result.violations)


def test_sensor_error_value_goes_to_dlq():
    _, _, rejected = run([reading(0, -999.0)])
    assert rejected == {"schema_violation": 1}


def test_single_spike_is_not_an_excursion():
    lines = [reading(m, 5.0) for m in range(0, 60, 5)]
    lines[5] = reading(25, 12.0)
    excursions, gaps, _ = run(lines)
    assert excursions == [] and gaps == []


def test_fifteen_minutes_out_of_range_is_an_excursion():
    lines = [reading(m, 12.0 if 20 <= m < 35 else 5.0) for m in range(0, 60, 5)]
    excursions, _, _ = run(lines)
    assert len(excursions) == 1
    assert excursions[0].start == T0 + timedelta(minutes=20)
    assert excursions[0].duration_minutes == 15 and excursions[0].readings == 3


def test_ten_minutes_out_of_range_is_not_yet_an_excursion():
    lines = [reading(m, 12.0 if 20 <= m < 30 else 5.0) for m in range(0, 60, 5)]
    assert run(lines)[0] == []


def test_missing_readings_are_reported_as_a_gap_not_as_normal():
    lines = [reading(m, 5.0) for m in list(range(0, 20, 5)) + list(range(60, 80, 5))]
    _, gaps, _ = run(lines)
    assert len(gaps) == 1
    assert gaps[0].after == T0 + timedelta(minutes=15)
    assert gaps[0].missing_minutes == 40


def test_out_of_range_on_both_sides_of_a_gap_is_not_one_excursion():
    # 공백 동안 온도가 어땠는지 모르므로 이어 붙여 30분 이탈로 만들지 않는다
    lines = [reading(0, 12.0), reading(5, 12.0), reading(40, 12.0), reading(45, 12.0)]
    excursions, gaps, _ = run(lines)
    assert excursions == [] and len(gaps) == 1


def test_arrival_order_and_resends_do_not_change_the_result():
    batch = generate_sensor(60, seed=11)
    lines = [json.dumps(e) for e in batch.events]
    shuffled = lines + lines[:200]  # 재전송
    random.Random(1).shuffle(shuffled)
    assert run(lines)[:2] == run(shuffled)[:2]


def test_detection_matches_the_manifest():
    batch = generate_sensor(300, seed=42)
    excursions, gaps, _ = run([json.dumps(e) for e in batch.events])
    result = score(excursions, gaps, batch.manifest["truth"])
    assert result["excursion_precision"] == 1.0
    assert result["excursion_recall"] == 1.0
    assert result["spikes_flagged"] == 0
    assert result["gap_recall"] == 1.0
    assert result["offline_misreported_as_gap"] == 0
