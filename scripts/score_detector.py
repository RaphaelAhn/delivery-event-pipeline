"""어뷰징 탐지 채점: 정답지(봇 세션)와 비교해 임계값별 성능을 낸다.

    python scripts/score_detector.py --sessions 4000 --seed 21

출력은 영문으로 둔다 (Windows 콘솔 인코딩 회피).
"""

from __future__ import annotations

import argparse

from pipeline.detect.abuse import DEFAULT_THRESHOLD, evaluate, score_sessions
from pipeline.detect.features import from_events
from pipeline.producer.generator import AnomalyRates
from pipeline.producer.search_generator import SearchMix, generate_search

THRESHOLDS = (0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sessions", type=int, default=4000)
    parser.add_argument("--seed", type=int, default=21)
    parser.add_argument("--bot-ratio", type=float, default=0.05)
    parser.add_argument("--stealth-share", type=float, default=0.4)
    args = parser.parse_args()

    batch = generate_search(
        args.sessions,
        seed=args.seed,
        rates=AnomalyRates(),
        mix=SearchMix(bot_session_ratio=args.bot_ratio, stealth_share=args.stealth_share),
    )
    truth = set(batch.manifest["bot_sessions"])
    profiles = batch.manifest["bot_profiles"]

    features = from_events(batch.events)
    scores = score_sessions(features)
    by_id = {s.session_id: s for s in scores}

    counts = batch.manifest["counts"]
    print(f"sessions        : {args.sessions}")
    print(
        f"bots (truth)    : {len(truth)} "
        f"(aggressive={counts['aggressive_bots']} stealth={counts['stealth_bots']})"
    )
    print(f"scored sessions : {len(scores)} (sessions with >= 5 queries are judged)")
    print()
    print("threshold  precision  recall    f1       TP   FP   FN")
    for threshold in THRESHOLDS:
        e = evaluate(scores, truth, threshold)
        mark = " <- default" if abs(threshold - DEFAULT_THRESHOLD) < 1e-9 else ""
        print(
            f"{threshold:>9.2f}  {e.precision:>9.3f}  {e.recall:>6.3f}  {e.f1:>6.3f}  "
            f"{e.true_positives:>4} {e.false_positives:>4} {e.false_negatives:>4}{mark}"
        )

    print()
    print("recall by bot type at the default threshold")
    for kind in ("aggressive", "stealth"):
        ids = {sid for sid, k in profiles.items() if k == kind}
        caught = sum(1 for sid in ids if sid in by_id and by_id[sid].is_abusive(DEFAULT_THRESHOLD))
        judged = sum(1 for sid in ids if sid in by_id)
        rate = caught / judged if judged else 0.0
        print(f"  {kind:<11} {caught}/{judged} = {rate:.3f}")

    missed = [
        by_id[sid] for sid in truth if sid in by_id and not by_id[sid].is_abusive(DEFAULT_THRESHOLD)
    ]
    false_alarms = [
        s for s in scores if s.is_abusive(DEFAULT_THRESHOLD) and s.session_id not in truth
    ]
    print()
    print(
        f"missed bots     : {len(missed)} (example scores: "
        f"{[round(s.score, 2) for s in missed[:5]]})"
    )
    print(
        f"false alarms    : {len(false_alarms)} (example scores: "
        f"{[round(s.score, 2) for s in false_alarms[:5]]})"
    )


if __name__ == "__main__":
    main()
