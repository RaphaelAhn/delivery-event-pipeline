"""콜드체인 온도 이탈 탐지.

    python -m pipeline.producer.sensor_generator --shipments 500
    python -m pipeline.sensor.excursion                 # 정제 → 탐지 → 정답지 채점 → 마트 파일

판정 규칙
    이탈(excursion) : 허용 범위를 벗어난 측정이 **15분 이상 연속**되면 한 건.
                      한 번 튄 값(spike)은 이탈이 아니다. 현장에서 알림 피로를 만드는 오탐이다.
    공백(gap)       : 다음 측정까지 **간격이 15분을 넘으면** 한 건. 그 사이 온도는 모른다.
                      모르는 구간을 정상으로 치지 않고 따로 보고한다. 공백이 끼면 연속도 끊긴다.

15분은 "기록계 3회 연속"이다. 의약품 GDP 가이드라인처럼 실제 현장 기준은 화주·품목마다 달라
여기서는 상수 하나로 두었다 (MIN_EXCURSION, MAX_GAP).

순서
    1. 계약 검사 (pipeline.consumer.validate 와 같은 검사원) → 불합격은 DLQ
    2. event_id 중복 제거 (재전송)
    3. **event_time 기준 정렬**. 통신이 끊겼다 한꺼번에 올라온 측정값이 제자리로 간다.
       도착 순서대로 보면 이 구간이 공백과 뒤늦은 끼어들기로 보인다.
    4. 화물별로 연속 이탈·공백 찾기
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pipeline.consumer.validate import validate
from pipeline.producer.sensor_generator import READING_INTERVAL, TEMPERATURE_RANGE

MIN_EXCURSION = timedelta(minutes=15)
MAX_GAP = timedelta(minutes=15)


@dataclass(frozen=True)
class Reading:
    event_id: str
    shipment_id: str
    device_id: str
    cargo_type: str
    leg: str
    temperature_c: float
    event_time: datetime


@dataclass(frozen=True)
class Excursion:
    shipment_id: str
    cargo_type: str
    leg: str
    start: datetime
    end: datetime  # 마지막 이탈 측정 시각
    duration_minutes: int
    readings: int
    peak_c: float


@dataclass(frozen=True)
class Gap:
    shipment_id: str
    after: datetime  # 마지막으로 받은 측정
    before: datetime  # 다음 측정
    missing_minutes: int


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(UTC)


def clean(lines) -> tuple[list[Reading], Counter]:
    """계약 검사 → 중복 제거 → event_time 정렬. 불합격 사유별 건수도 돌려준다."""
    readings: dict[str, Reading] = {}
    rejected: Counter = Counter()
    for line in lines:
        result = validate(line)
        if not result.ok:
            rejected[result.violations[0].code] += 1
            continue
        e = result.event
        if e["event_id"] in readings:
            rejected["duplicate"] += 1
            continue
        readings[e["event_id"]] = Reading(
            e["event_id"],
            e["shipment_id"],
            e["device_id"],
            e["cargo_type"],
            e["leg"],
            float(e["temperature_c"]),
            _parse(e["event_time"]),
        )
    ordered = sorted(readings.values(), key=lambda r: (r.shipment_id, r.event_time))
    return ordered, rejected


def _out_of_range(reading: Reading) -> bool:
    low, high = TEMPERATURE_RANGE[reading.cargo_type]
    return not low <= reading.temperature_c <= high


def _excursion(run: list[Reading]) -> list[Excursion]:
    """연속 이탈 측정 묶음이 15분 이상이면 이탈 한 건. 첫 이탈부터 다음 측정 직전까지로 잰다."""
    if not run:
        return []
    duration = run[-1].event_time - run[0].event_time + READING_INTERVAL
    if duration < MIN_EXCURSION:
        return []
    first = run[0]
    return [
        Excursion(
            first.shipment_id,
            first.cargo_type,
            first.leg,
            first.event_time,
            run[-1].event_time,
            int(duration.total_seconds() // 60),
            len(run),
            max(r.temperature_c for r in run),
        )
    ]


def detect(readings: list[Reading]) -> tuple[list[Excursion], list[Gap]]:
    """정렬된 측정값에서 이탈과 공백을 찾는다."""
    by_shipment: dict[str, list[Reading]] = defaultdict(list)
    for reading in readings:
        by_shipment[reading.shipment_id].append(reading)

    excursions: list[Excursion] = []
    gaps: list[Gap] = []
    for shipment_id, series in by_shipment.items():
        run: list[Reading] = []
        for previous, current in zip([None, *series], series, strict=False):
            if previous is not None and current.event_time - previous.event_time > MAX_GAP:
                # 공백을 사이에 두고 이어진 이탈은 연속으로 보지 않는다
                excursions += _excursion(run)
                run = []
                missing = current.event_time - previous.event_time - READING_INTERVAL
                gaps.append(
                    Gap(
                        shipment_id,
                        previous.event_time,
                        current.event_time,
                        int(missing.total_seconds() // 60),
                    )
                )
            if _out_of_range(current):
                run.append(current)
            else:
                excursions += _excursion(run)
                run = []
        excursions += _excursion(run)
    return excursions, gaps


def score(excursions: list[Excursion], gaps: list[Gap], truth: dict) -> dict:
    """정답지와 화물 단위로 비교한다. 이탈은 시작 시각까지 맞아야 정답으로 친다."""
    found = {(e.shipment_id, e.start) for e in excursions}
    expected = {(t["shipment_id"], _parse(t["start"])) for t in truth["excursion"]}
    hit = len(found & expected)
    spikes = {t["shipment_id"] for t in truth["spike"]}

    gap_found = {g.shipment_id for g in gaps}
    gap_expected = {t["shipment_id"] for t in truth["dropout"]}
    offline = {t["shipment_id"] for t in truth["offline"]}
    return {
        "excursion_precision": hit / len(found) if found else 1.0,
        "excursion_recall": hit / len(expected) if expected else 1.0,
        "spikes_flagged": len(spikes & {e.shipment_id for e in excursions}),
        "gap_recall": len(gap_found & gap_expected) / len(gap_expected) if gap_expected else 1.0,
        "gap_false_alarms": len(gap_found - gap_expected),
        "offline_misreported_as_gap": len(gap_found & offline),
    }


def _row(item) -> dict:
    return {k: _as_text(v) for k, v in asdict(item).items()}


def _as_text(value):
    return value.strftime("%Y-%m-%dT%H:%M:%SZ") if isinstance(value, datetime) else value


def write_marts(readings, excursions, gaps, output_dir: Path) -> None:
    """BigQuery 적재(pipeline.sensor.bigquery)가 읽는 JSONL 세 개를 쓴다."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, items in (
        ("sensor_readings", readings),
        ("temperature_excursions", excursions),
        ("sensor_gaps", gaps),
    ):
        with (output_dir / f"{name}.jsonl").open("w", encoding="utf-8") as handle:
            for item in items:
                handle.write(json.dumps(_row(item), ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="콜드체인 온도 이탈 탐지")
    parser.add_argument("--input", type=Path, default=Path("data/sensor_events.jsonl"))
    parser.add_argument("--marts", type=Path, default=Path("data/sensor_marts"))
    args = parser.parse_args()

    lines = args.input.read_text(encoding="utf-8").splitlines()
    readings, rejected = clean(lines)
    excursions, gaps = detect(readings)
    write_marts(readings, excursions, gaps, args.marts)

    print(f"Input {len(lines)} lines → readings {len(readings)} | rejected {dict(rejected)}")
    print(f"Excursions {len(excursions)} | gaps {len(gaps)} → {args.marts}")
    manifest_path = args.input.with_suffix(".manifest.json")
    if manifest_path.exists():
        truth = json.loads(manifest_path.read_text(encoding="utf-8"))["truth"]
        print(f"Score vs manifest: {score(excursions, gaps, truth)}")


if __name__ == "__main__":
    main()
