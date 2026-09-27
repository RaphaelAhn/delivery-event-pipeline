"""합성 콜드체인 센서 데이터 생성기.

화물(shipment) 하나에 온도 기록계(device)가 하나 붙어 5분마다 온도를 잰다.
화물은 창고 → 내륙 운송 → 국제 운송 구간(leg)을 차례로 지난다.

화물마다 아래 중 하나를 골라 섞고, **무엇을 섞었는지 정답지(manifest)에 남긴다.**
이탈 탐지(pipeline.sensor.excursion)가 그 정답지로 채점된다.

    excursion : 허용 범위를 20~90분 연속으로 벗어난다. 구간이 바뀌는 환적 시점에 문이 열리는 상황.
                → 탐지해야 한다
    spike     : 한 번만 튄다. 센서 잡음이나 문 앞을 잠깐 지난 경우.
                → 탐지하면 오탐이다 (현장에서 알림 피로를 만드는 원인)
    dropout   : 30~90분 동안 측정값이 아예 없다 (배터리·통신 장애로 유실).
                → 온도를 모르는 구간이므로 "정상"으로 넘기지 않고 공백으로 따로 보고해야 한다
    offline   : 30~90분 통신이 끊겼다가 **재연결 때 한꺼번에 올라온다** (기록계 내부 버퍼).
                → 측정값은 다 있다. 도착 시각이 아니라 event_time 으로 정렬하면 정상이다

전송 문제(duplicate·invalid)는 배달·검색 생성기와 같은 AnomalyRates 로 섞는다.
"""

from __future__ import annotations

import argparse
import random
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pipeline.producer.generator import AnomalyRates, GeneratedBatch, _Pending, _ts, write_batch

READING_INTERVAL = timedelta(minutes=5)
# 화물 종류별 허용 온도 (°C). 이탈 탐지도 이 표를 쓴다.
TEMPERATURE_RANGE = {"pharma": (2.0, 8.0), "frozen": (-25.0, -15.0)}
LEGS = ("warehouse", "inland", "international")
# 구간별 측정 횟수 (5분 간격). 창고 1~3시간, 내륙 2~5시간, 국제 4~8시간
LEG_READINGS = {"warehouse": (12, 36), "inland": (24, 60), "international": (48, 96)}


@dataclass(frozen=True)
class SensorMix:
    """화물별 상황 비율. 합이 1 을 넘으면 안 된다. 나머지는 정상 화물이다."""

    excursion: float = 0.10
    spike: float = 0.10
    dropout: float = 0.05
    offline: float = 0.05

    def __post_init__(self) -> None:
        values = asdict(self).values()
        if any(not 0 <= v <= 1 for v in values) or sum(values) > 1:
            raise ValueError(f"shipment mix must be within 0..1 and sum to <= 1: {self}")


class _SensorGenerator:
    def __init__(self, seed: int, start: datetime) -> None:
        self.rng = random.Random(seed)
        self.start = start

    def event_id(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def normal_temperature(self, cargo: str) -> float:
        low, high = TEMPERATURE_RANGE[cargo]
        # 범위 가운데 근처에서 조금 흔들린다. 범위 끝까지 가지 않는다.
        return round((low + high) / 2 + self.rng.gauss(0, (high - low) / 10), 2)

    def outside_temperature(self, cargo: str) -> float:
        low, high = TEMPERATURE_RANGE[cargo]
        # 의약품은 주로 더워져서, 냉동은 녹아서 벗어난다
        return round(high + self.rng.uniform(1.0, 6.0), 2)

    def reading(self, shipment_id, device_id, cargo, leg, at, temperature) -> dict:
        return {
            "schema_version": 1,
            "event_id": self.event_id(),
            "event_type": "SensorReading",
            "shipment_id": shipment_id,
            "device_id": device_id,
            "cargo_type": cargo,
            "leg": leg,
            "temperature_c": temperature,
            "humidity_pct": round(self.rng.uniform(30, 70), 1),
            "event_time": at,
        }

    def corrupt(self, event: dict) -> tuple[dict, str]:
        broken = dict(event)
        kind = self.rng.choice(["missing_device_id", "sensor_error_value", "bad_event_time"])
        if kind == "missing_device_id":
            del broken["device_id"]
        elif kind == "sensor_error_value":
            broken["temperature_c"] = -999.0  # 기록계가 측정 실패 때 보내는 값
        else:
            broken["event_time"] = "2026-13-45T25:61:00Z"
        return broken, kind


def generate_sensor(
    shipments: int,
    seed: int = 42,
    start: datetime | None = None,
    rates: AnomalyRates | None = None,
    mix: SensorMix | None = None,
) -> GeneratedBatch:
    """센서 측정값을 만든다. 반환 형식은 다른 생성기와 같다 (도착 순서 + 정답지)."""
    if shipments < 1:
        raise ValueError("shipments must be >= 1")
    rates = rates or AnomalyRates()
    mix = mix or SensorMix()
    start = start or (datetime.now(UTC) - timedelta(days=1)).replace(microsecond=0)

    gen = _SensorGenerator(seed, start)
    rng = gen.rng
    pending: list[_Pending] = []
    injected: dict[str, list] = {"duplicate": [], "invalid": []}
    truth: dict[str, list] = {"excursion": [], "spike": [], "dropout": [], "offline": []}
    clean_readings = 0

    for number in range(1, shipments + 1):
        shipment_id = f"SH-{number}"
        device_id = f"D-{rng.getrandbits(32):08x}"
        cargo = rng.choice(tuple(TEMPERATURE_RANGE))
        # 5분 격자에 맞춘다. 탐지가 "몇 분 연속"을 셀 때 흔들리지 않게 하기 위해서다.
        at = start + READING_INTERVAL * rng.randrange(0, 12 * 24)

        timeline = []  # (시각, 구간)
        for leg in LEGS:
            for _ in range(rng.randint(*LEG_READINGS[leg])):
                timeline.append((at, leg))
                at += READING_INTERVAL

        draw = rng.random()
        situation = "normal"
        for name, share in asdict(mix).items():
            if draw < share:
                situation = name
                break
            draw -= share

        # 상황이 벌어지는 위치. 환적 시점(두 번째 구간 시작) 근처에 둔다.
        handover = next(i for i, (_, leg) in enumerate(timeline) if leg == LEGS[1])
        span = rng.randint(4, 18)  # 5분 × 4~18 = 20~90분
        affected = range(handover, handover + span)
        if situation == "excursion":
            truth["excursion"].append(
                {
                    "shipment_id": shipment_id,
                    "start": _ts(timeline[affected.start][0]),
                    "end": _ts(timeline[affected.stop - 1][0]),
                    "leg": timeline[affected.start][1],
                }
            )
        elif situation == "spike":
            affected = range(handover, handover + 1)
            truth["spike"].append({"shipment_id": shipment_id, "at": _ts(timeline[handover][0])})
        elif situation in ("dropout", "offline"):
            span = rng.randint(6, 18)  # 30~90분
            affected = range(handover, handover + span)
            truth[situation].append(
                {
                    "shipment_id": shipment_id,
                    "start": _ts(timeline[affected.start][0]),
                    "end": _ts(timeline[affected.stop - 1][0]),
                }
            )
        else:
            affected = range(0)

        reconnect_at = timeline[affected.stop][0] if situation == "offline" else None
        for index, (reading_at, leg) in enumerate(timeline):
            if situation == "dropout" and index in affected:
                continue  # 유실. 아예 만들지 않는다
            out_of_range = situation in ("excursion", "spike") and index in affected
            temperature = (
                gen.outside_temperature(cargo) if out_of_range else gen.normal_temperature(cargo)
            )
            event = gen.reading(shipment_id, device_id, cargo, leg, reading_at, temperature)
            clean_readings += 1

            arrival = reading_at + timedelta(seconds=rng.uniform(1, 30))
            if situation == "offline" and index in affected:
                # 끊긴 동안 쌓아 두었다가 재연결 때 한꺼번에 보낸다
                arrival = reconnect_at + timedelta(seconds=rng.uniform(1, 30))
            elif rng.random() < rates.invalid:
                event, kind = gen.corrupt(event)
                injected["invalid"].append({"event_id": event["event_id"], "kind": kind})
            pending.append(_Pending(arrival, event))

    for item in list(pending):
        if rng.random() < rates.duplicate:
            resend = item.arrival + timedelta(seconds=rng.uniform(1, 600))
            pending.append(_Pending(resend, dict(item.event)))
            injected["duplicate"].append(item.event["event_id"])

    pending.sort(key=lambda p: p.arrival)
    events_out = [
        {k: _ts(v) if isinstance(v, datetime) else v for k, v in p.event.items()} for p in pending
    ]
    manifest = {
        "seed": seed,
        "shipments": shipments,
        "start": _ts(start),
        "rates": asdict(rates),
        "mix": asdict(mix),
        "counts": {
            "clean_readings": clean_readings,
            "emitted_events": len(events_out),
            **{name: len(items) for name, items in injected.items()},
            **{f"{name}_shipments": len(items) for name, items in truth.items()},
        },
        "injected": injected,
        # 이탈 탐지의 정답지
        "truth": truth,
    }
    return GeneratedBatch(events=events_out, manifest=manifest)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--shipments", type=int, default=500)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("data/sensor_events.jsonl"))
    args = parser.parse_args()

    batch = generate_sensor(args.shipments, seed=args.seed)
    manifest_path = write_batch(batch, args.output)
    counts = batch.manifest["counts"]
    print(f"Wrote {counts['emitted_events']} readings for {args.shipments} shipments")
    print(
        f"Shipments: excursion={counts['excursion_shipments']} spike={counts['spike_shipments']} "
        f"dropout={counts['dropout_shipments']} offline={counts['offline_shipments']} | "
        f"duplicate={counts['duplicate']} invalid={counts['invalid']}"
    )
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
