"""합성 배달 이벤트 생성기.

주문 1건마다 정상 흐름(주문 생성 → 배차 시도 1~3회 → 배달 완료 또는 취소)을 만든 뒤,
실제 로그에서 흔한 문제를 지정한 비율만큼 섞는다.

- duplicate    : 같은 event_id 로 재전송된 이벤트 (at-least-once 전송에서 발생)
- late         : event_time 은 정상이지만 10~120분 늦게 도착
- out_of_order : 부모 OrderCreated 보다 먼저 도착하는 자식 이벤트
- invalid      : 계약 위반 (필수 필드 누락, eta_minutes <= 0, 잘못된 시각 문자열)

이벤트 파일에는 "도착 순서"대로 이벤트만 기록하고, 어떤 이벤트에 무엇을 섞었는지는
별도 manifest 파일에 남긴다. 나중에 검증기·중복 제거·이상 탐지가 제대로 잡는지
정답과 비교하는 데 사용한다.
"""

from __future__ import annotations

import argparse
import json
import random
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

REGIONS = ("R-SEOUL-01", "R-SEOUL-02", "R-SEOUL-03", "R-GYEONGGI-01", "R-GYEONGGI-02")
VARIANTS = ("control", "treatment")
MAX_DISPATCH_ATTEMPTS = 3
# 정책 실험 신호: treatment 의 배차 수락 확률을 조금 높게 둔다 (2주차 이후 A/B 분석용)
ACCEPT_PROBABILITY = {"control": 0.75, "treatment": 0.82}
CANCEL_AFTER_ACCEPT_PROBABILITY = 0.03
RIDER_COUNT = 200


@dataclass(frozen=True)
class AnomalyRates:
    duplicate: float = 0.03
    late: float = 0.05
    out_of_order: float = 0.02
    invalid: float = 0.01

    def __post_init__(self) -> None:
        for name, value in asdict(self).items():
            if not 0 <= value <= 1:
                raise ValueError(f"{name} rate must be between 0 and 1, got {value}")
        if self.late + self.out_of_order + self.invalid > 1:
            raise ValueError("late + out_of_order + invalid must not exceed 1")


@dataclass
class GeneratedBatch:
    events: list[dict]  # 도착 순서
    manifest: dict = field(default_factory=dict)


@dataclass
class _Pending:
    arrival: datetime
    event: dict


def _ts(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class _Generator:
    def __init__(self, seed: int, start: datetime) -> None:
        self.rng = random.Random(seed)
        self.start = start

    def event_id(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def ingest_latency(self) -> timedelta:
        return timedelta(seconds=self.rng.uniform(0, 5))

    def order_events(self, order_number: int) -> list[dict]:
        rng = self.rng
        order_id = f"O-{order_number}"
        variant = rng.choice(VARIANTS)
        created_at = self.start + timedelta(seconds=rng.uniform(0, 24 * 3600))
        events = [
            {
                "schema_version": 1,
                "event_id": self.event_id(),
                "event_type": "OrderCreated",
                "order_id": order_id,
                "event_time": created_at,
                "region_id": rng.choice(REGIONS),
                "experiment_variant": variant,
            }
        ]

        now = created_at
        accepted_dispatch_id = None
        eta = None
        for attempt in range(1, MAX_DISPATCH_ATTEMPTS + 1):
            now += timedelta(minutes=rng.uniform(1, 4))
            dispatch_id = f"D-{order_number}-{attempt}"
            accepted = rng.random() < ACCEPT_PROBABILITY[variant]
            event = {
                "schema_version": 1,
                "event_id": self.event_id(),
                "event_type": "DispatchResult",
                "order_id": order_id,
                "dispatch_id": dispatch_id,
                "rider_id": f"rider-{rng.randint(1, RIDER_COUNT):03d}",
                "result": "accepted" if accepted else "rejected",
                "event_time": now,
            }
            if accepted:
                eta = rng.randint(15, 40)
                event["eta_minutes"] = eta
                accepted_dispatch_id = dispatch_id
            events.append(event)
            if accepted:
                break

        if accepted_dispatch_id is None:
            events.append(self._cancel(order_id, now + timedelta(minutes=2), "no_rider"))
        elif rng.random() < CANCEL_AFTER_ACCEPT_PROBABILITY:
            cancelled_at = now + timedelta(minutes=rng.uniform(1, 10))
            events.append(self._cancel(order_id, cancelled_at, "customer_request"))
        else:
            # 실제 소요 시간 = ETA + 정규 오차, 최소 5분
            actual = max(5.0, rng.gauss(eta, 5))
            events.append(
                {
                    "schema_version": 1,
                    "event_id": self.event_id(),
                    "event_type": "DeliveryCompleted",
                    "order_id": order_id,
                    "dispatch_id": accepted_dispatch_id,
                    "event_time": now + timedelta(minutes=actual),
                }
            )
        return events

    def _cancel(self, order_id: str, at: datetime, reason: str) -> dict:
        return {
            "schema_version": 1,
            "event_id": self.event_id(),
            "event_type": "OrderCancelled",
            "order_id": order_id,
            "event_time": at,
            "cancel_reason": reason,
        }

    def corrupt(self, event: dict) -> tuple[dict, str]:
        """계약을 위반하는 사본을 만든다. (사본, 위반 종류) 반환."""
        broken = dict(event)
        options = ["missing_order_id", "bad_event_time"]
        if "dispatch_id" in broken:
            options.append("missing_dispatch_id")
        if "eta_minutes" in broken:
            options.append("non_positive_eta")
        kind = self.rng.choice(options)
        if kind == "missing_order_id":
            del broken["order_id"]
        elif kind == "missing_dispatch_id":
            del broken["dispatch_id"]
        elif kind == "non_positive_eta":
            broken["eta_minutes"] = 0
        else:
            broken["event_time"] = "2026-13-45T25:61:00Z"
        return broken, kind


def generate(
    orders: int,
    seed: int = 42,
    start: datetime | None = None,
    rates: AnomalyRates | None = None,
) -> GeneratedBatch:
    if orders < 1:
        raise ValueError("orders must be >= 1")
    rates = rates or AnomalyRates()
    start = start or datetime(2026, 1, 1, tzinfo=UTC)
    gen = _Generator(seed, start)
    rng = gen.rng

    pending: list[_Pending] = []
    injected: dict[str, list] = {"duplicate": [], "late": [], "out_of_order": [], "invalid": []}
    clean_event_count = 0

    for i in range(orders):
        order_events = gen.order_events(100_000 + i)
        clean_event_count += len(order_events)
        parent_arrival = order_events[0]["event_time"] + gen.ingest_latency()
        pending.append(_Pending(parent_arrival, order_events[0]))

        for event in order_events[1:]:
            arrival = event["event_time"] + gen.ingest_latency()
            # late / out_of_order / invalid 는 한 이벤트에 하나만 적용 (구간을 나눠 한 번만 추첨)
            draw = rng.random()
            if draw < rates.late:
                arrival += timedelta(minutes=rng.uniform(10, 120))
                injected["late"].append(event["event_id"])
            elif draw < rates.late + rates.out_of_order:
                arrival = parent_arrival - timedelta(seconds=rng.uniform(1, 30))
                injected["out_of_order"].append(event["event_id"])
            elif draw < rates.late + rates.out_of_order + rates.invalid:
                event, kind = gen.corrupt(event)
                injected["invalid"].append({"event_id": event["event_id"], "kind": kind})
            pending.append(_Pending(arrival, event))

    # 재전송: 최종 형태의 이벤트를 1초~10분 뒤에 한 번 더 보낸다 (event_id 동일)
    for item in list(pending):
        if rng.random() < rates.duplicate:
            resend = item.arrival + timedelta(seconds=rng.uniform(1, 600))
            pending.append(_Pending(resend, dict(item.event)))
            injected["duplicate"].append(item.event["event_id"])

    # sort 는 안정 정렬이므로 같은 도착 시각이면 생성 순서가 유지된다
    pending.sort(key=lambda p: p.arrival)
    events = [
        {k: _ts(v) if isinstance(v, datetime) else v for k, v in p.event.items()} for p in pending
    ]

    manifest = {
        "seed": seed,
        "orders": orders,
        "start": _ts(start),
        "rates": asdict(rates),
        "counts": {
            "clean_events": clean_event_count,
            "emitted_events": len(events),
            **{name: len(ids) for name, ids in injected.items()},
        },
        "injected": injected,
    }
    return GeneratedBatch(events=events, manifest=manifest)


def write_batch(batch: GeneratedBatch, output: Path) -> Path:
    """이벤트는 JSONL, manifest 는 같은 이름의 .manifest.json 으로 저장한다."""
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as handle:
        for event in batch.events:
            handle.write(json.dumps(event, ensure_ascii=False) + "\n")
    manifest_path = output.with_suffix(".manifest.json")
    manifest_path.write_text(
        json.dumps(batch.manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return manifest_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--orders", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("data/events.jsonl"))
    parser.add_argument("--duplicate-rate", type=float, default=AnomalyRates.duplicate)
    parser.add_argument("--late-rate", type=float, default=AnomalyRates.late)
    parser.add_argument("--out-of-order-rate", type=float, default=AnomalyRates.out_of_order)
    parser.add_argument("--invalid-rate", type=float, default=AnomalyRates.invalid)
    args = parser.parse_args()

    rates = AnomalyRates(
        duplicate=args.duplicate_rate,
        late=args.late_rate,
        out_of_order=args.out_of_order_rate,
        invalid=args.invalid_rate,
    )
    batch = generate(args.orders, seed=args.seed, rates=rates)
    manifest_path = write_batch(batch, args.output)
    counts = batch.manifest["counts"]
    print(f"Wrote {counts['emitted_events']} events for {args.orders} orders to {args.output}")
    print(
        "Injected: "
        + ", ".join(
            f"{name}={counts[name]}" for name in ("duplicate", "late", "out_of_order", "invalid")
        )
    )
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
