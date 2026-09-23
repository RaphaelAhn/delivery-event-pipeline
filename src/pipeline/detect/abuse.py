"""어뷰징(봇) 세션 탐지.

점수는 네 가지 신호를 더해서 만든다. 하나만 보면 정상 사용자를 오탐하기 쉽다.
예) 검색을 20번 한 사람은 있다. 하지만 20번을 2초 간격으로, 같은 검색어로,
    한 번도 클릭하지 않는 사람은 드물다.

    1) 속도      분당 검색 수가 집단 평균에서 얼마나 벗어나는가 (z-score)
    2) 규칙성    검색 간격이 얼마나 일정한가 (표준편차 ÷ 평균 = 변동계수)
    3) 단조로움  같은 검색어를 얼마나 반복하는가
    4) 무관심    검색해 놓고 클릭하지 않는가

z-score 를 쓰는 이유: "분당 30회 이상"처럼 절대 기준을 박아 두면 트래픽 성격이 바뀔 때마다
다시 고쳐야 한다. 그날 집단의 분포에서 얼마나 튀는지로 보면 기준이 따라 움직인다.

각 신호는 0~1 로 정규화한 뒤 가중치를 곱해 더한다. 최종 점수도 0~1 이다.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field

from pipeline.detect.features import SessionFeatures

# 신호별 가중치. 합이 1 이 되도록 둔다.
WEIGHTS = {
    "speed": 0.35,  # 분당 검색 수 (z-score)
    "regularity": 0.25,  # 간격의 규칙성
    "monotony": 0.20,  # 검색어 반복
    "disinterest": 0.20,  # 클릭하지 않음
}
# 검색 횟수가 너무 적으면 어떤 신호도 믿을 수 없다 (2번 검색한 사람은 판단 불가)
MIN_QUERIES = 5
# 임계값 0.50 은 F1 이 가장 높은 지점이다 (아래 표는 scripts/score_detector.py 출력).
#   0.45  precision 0.936  recall 0.995   놓침은 거의 없지만 사람 15명을 잘못 막는다
#   0.50  precision 0.977  recall 0.982   균형점 (F1 0.979)
#   0.60  precision 1.000  recall 0.785   오탐 0 이지만 봇 47개를 놓친다
# 사람을 잘못 막는 비용이 더 크면 0.60 쪽으로, 놓치는 비용이 더 크면 0.45 쪽으로 옮긴다.
DEFAULT_THRESHOLD = 0.50


@dataclass(frozen=True)
class SessionScore:
    session_id: str
    score: float
    signals: dict[str, float] = field(default_factory=dict)
    reasons: tuple[str, ...] = ()

    def is_abusive(self, threshold: float = DEFAULT_THRESHOLD) -> bool:
        return self.score >= threshold


def _robust_z(value: float, center: float, scale: float) -> float:
    """중앙값과 MAD 로 계산한 z-score.

    평균·표준편차를 쓰면 **극단적인 봇이 기준 자체를 끌어올려** 중간 강도의 봇이
    평균 이하로 보인다. 실제로 이 데이터에서 분당 검색 수의 평균은 4.57,
    표준편차는 12.78 이었고(공격적 봇 34.7 때문), 사람인 척하는 봇(3.63)은
    평균보다 낮아 신호가 0 이 됐다.

    중앙값(1.10)과 MAD(0.19)는 극단값에 끌려가지 않아 같은 세션이 크게 튀어 보인다.
    """
    return (value - center) / scale if scale > 0 else 0.0


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def _population_stats(features: list[SessionFeatures]) -> tuple[float, float]:
    """분당 검색 수의 중앙값과 MAD. 판단 가능한 세션만으로 계산한다.

    MAD 에 1.4826 을 곱하면 정규분포에서 표준편차와 같은 눈금이 된다.
    """
    rates = [
        f.queries_per_minute
        for f in features
        if f.queries >= MIN_QUERIES and f.queries_per_minute is not None
    ]
    if len(rates) < 2:
        return 0.0, 0.0
    center = statistics.median(rates)
    mad = statistics.median([abs(rate - center) for rate in rates])
    scale = mad * 1.4826
    # 모든 세션의 속도가 똑같으면 MAD 가 0 이다. 그때는 표준편차로 물러선다.
    return center, scale if scale > 0 else statistics.pstdev(rates)


def score_session(
    features: SessionFeatures, population_center: float, population_scale: float
) -> SessionScore:
    signals: dict[str, float] = {}
    reasons: list[str] = []

    if features.queries < MIN_QUERIES:
        # 근거가 부족한 세션은 0 점. 놓치는 쪽이 정상 사용자를 막는 것보다 낫다.
        return SessionScore(features.session_id, 0.0, {}, ("too_few_queries",))

    rate = features.queries_per_minute
    if rate is not None:
        # z 가 3 이상이면 신호 최대. 집단에서 크게 벗어난 속도다.
        z = _robust_z(rate, population_center, population_scale)
        signals["speed"] = _clamp01(z / 3.0)
        if signals["speed"] > 0.5:
            reasons.append(f"speed_z={z:.1f}")
    else:
        signals["speed"] = 0.0

    if features.avg_gap_seconds and features.stddev_gap_seconds is not None:
        # 변동계수: 간격이 평균 대비 얼마나 흔들리는가. 사람은 크고 기계는 작다.
        variation = features.stddev_gap_seconds / features.avg_gap_seconds
        signals["regularity"] = _clamp01(1.0 - variation / 0.6)
        if signals["regularity"] > 0.5:
            reasons.append(f"regular_gaps_cv={variation:.2f}")
    else:
        signals["regularity"] = 0.0

    signals["monotony"] = _clamp01(1.0 - features.distinct_query_ratio / 0.5)
    if signals["monotony"] > 0.5:
        reasons.append(f"repeated_queries={features.distinct_query_ratio:.2f}")

    signals["disinterest"] = _clamp01(1.0 - features.click_rate / 0.35)
    if signals["disinterest"] > 0.5:
        reasons.append(f"low_click_rate={features.click_rate:.2f}")

    score = sum(WEIGHTS[name] * value for name, value in signals.items())
    return SessionScore(features.session_id, round(score, 4), signals, tuple(reasons))


def score_sessions(features: list[SessionFeatures]) -> list[SessionScore]:
    """집단 통계를 먼저 구한 뒤 세션마다 점수를 매긴다."""
    center, scale = _population_stats(features)
    return [score_session(f, center, scale) for f in features]


@dataclass(frozen=True)
class Evaluation:
    threshold: float
    true_positives: int
    false_positives: int
    false_negatives: int

    @property
    def precision(self) -> float:
        flagged = self.true_positives + self.false_positives
        return self.true_positives / flagged if flagged else 1.0

    @property
    def recall(self) -> float:
        actual = self.true_positives + self.false_negatives
        return self.true_positives / actual if actual else 1.0

    @property
    def f1(self) -> float:
        p, r = self.precision, self.recall
        return 2 * p * r / (p + r) if p + r else 0.0


def evaluate(scores: list[SessionScore], truth: set[str], threshold: float) -> Evaluation:
    """정답지(봇 세션 목록)와 비교해 채점한다."""
    flagged = {s.session_id for s in scores if s.is_abusive(threshold)}
    scored = {s.session_id for s in scores}
    actual = truth & scored  # 평가 대상에 없는 세션은 제외
    return Evaluation(
        threshold=threshold,
        true_positives=len(flagged & actual),
        false_positives=len(flagged - actual),
        false_negatives=len(actual - flagged),
    )
