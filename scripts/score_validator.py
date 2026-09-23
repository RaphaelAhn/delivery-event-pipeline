"""검사원 성능 채점: 생성기 정답지(manifest)와 검사 결과를 비교한다.

    python scripts/score_validator.py --orders 2000 --seed 21 --invalid-rate 0.05

출력은 영문으로 둔다 (Windows 콘솔 인코딩 문제 회피).
"""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter

from pipeline.consumer.validate import validate
from pipeline.producer.generator import AnomalyRates, generate


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--orders", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=21)
    parser.add_argument("--invalid-rate", type=float, default=0.05)
    args = parser.parse_args()

    rates = AnomalyRates(duplicate=0.03, late=0.05, out_of_order=0.02, invalid=args.invalid_rate)
    batch = generate(args.orders, seed=args.seed, rates=rates)
    payloads = [json.dumps(event).encode("utf-8") for event in batch.events]

    started = time.perf_counter()
    results = [validate(payload) for payload in payloads]
    elapsed = time.perf_counter() - started

    flagged = {result.event_id for result in results if not result.ok}
    expected = {item["event_id"] for item in batch.manifest["injected"]["invalid"]}
    tp, fp, fn = len(flagged & expected), len(flagged - expected), len(expected - flagged)
    precision = tp / (tp + fp) if tp + fp else 1.0
    recall = tp / (tp + fn) if tp + fn else 1.0

    # 지연/중복/순서 뒤바뀜은 내용이 정상이므로 통과해야 한다
    by_id = {event["event_id"]: event for event in batch.events}
    wrongly_rejected = [
        event_id
        for kind in ("late", "out_of_order", "duplicate")
        for event_id in batch.manifest["injected"][kind]
        if event_id not in expected and not validate(json.dumps(by_id[event_id]).encode()).ok
    ]

    print(f"events            : {len(payloads)}")
    print(f"flagged           : {len(flagged)}")
    print(f"injected invalid  : {len(expected)}")
    print(f"TP / FP / FN      : {tp} / {fp} / {fn}")
    print(f"precision / recall: {precision:.4f} / {recall:.4f}")
    print(f"throughput        : {elapsed:.2f}s, {len(payloads) / elapsed:,.0f} events/s")
    reason_codes = Counter(v.code for r in results for v in r.violations)
    injected_kinds = Counter(i["kind"] for i in batch.manifest["injected"]["invalid"])
    print(f"reason codes      : {dict(reason_codes)}")
    print(f"injected kinds    : {dict(injected_kinds)}")
    print(f"late/dup/ooo wrongly rejected: {len(wrongly_rejected)}")
    print("RESULT: PASS" if (fp, fn, len(wrongly_rejected)) == (0, 0, 0) else "RESULT: FAIL")


if __name__ == "__main__":
    main()
