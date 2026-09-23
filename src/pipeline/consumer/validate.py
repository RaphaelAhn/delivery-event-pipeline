"""이벤트 검사원: Kafka 에서 꺼낸 쪽지 한 장이 계약을 지키는지 검사한다.

쪽지 한 장만 보고 판단할 수 있는 것만 검사한다 (stateless).
- 검사함: 읽을 수 있나(R1), 아는 종류인가(R2), 양식에 맞나(R3), 시각이 진짜 시각인가(R4)
- 검사 안 함: 중복, 부모 없는 쪽지, 지연
  → 여러 장을 봐야 알 수 있다. 특히 "부모 없음"은 부모가 아직 안 온 것일 수도 있어서
    (토픽이 다르면 순서 보장이 없다) 여기서 버리면 정상 이벤트를 잃는다. dbt 에서 판정한다.

테스트: pytest tests/unit/test_validate.py
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from jsonschema import Draft202012Validator

CONTRACTS_DIR = Path(__file__).resolve().parents[3] / "contracts"
SCHEMA_FILE_BY_EVENT_TYPE = {
    "OrderCreated": "order_event.schema.json",
    "OrderCancelled": "order_event.schema.json",
    "DispatchResult": "dispatch_event.schema.json",
    "DeliveryCompleted": "delivery_event.schema.json",
}

# 불합격 사유 코드. 4일차에 DLQ 테이블에 그대로 저장한다.
INVALID_JSON = "invalid_json"
NOT_AN_OBJECT = "not_an_object"
MISSING_EVENT_TYPE = "missing_event_type"
UNKNOWN_EVENT_TYPE = "unknown_event_type"
SCHEMA_VIOLATION = "schema_violation"
INVALID_EVENT_TIME = "invalid_event_time"


@dataclass(frozen=True)
class Violation:
    code: str  # 위 사유 코드 중 하나
    field: str | None  # 문제가 된 칸 이름. 특정할 수 없으면 None
    message: str  # 사람이 읽을 설명


@dataclass
class ValidationResult:
    event: dict | None  # 읽어 낸 쪽지. JSON 객체로 읽지 못했으면 None
    violations: list[Violation] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.violations

    @property
    def event_id(self) -> str | None:
        if isinstance(self.event, dict):
            value = self.event.get("event_id")
            return value if isinstance(value, str) else None
        return None


def _build_validators() -> dict[str, Draft202012Validator]:
    """스키마 파일은 모듈을 처음 불러올 때 한 번만 읽는다 (쪽지마다 읽으면 느리다)."""
    by_filename: dict[str, Draft202012Validator] = {}
    for filename in set(SCHEMA_FILE_BY_EVENT_TYPE.values()):
        schema = json.loads((CONTRACTS_DIR / filename).read_text(encoding="utf-8"))
        by_filename[filename] = Draft202012Validator(schema)
    return {
        event_type: by_filename[filename]
        for event_type, filename in SCHEMA_FILE_BY_EVENT_TYPE.items()
    }


_VALIDATORS = _build_validators()


def _field_of(error) -> str | None:
    """문제가 된 칸 이름을 찾는다."""
    if error.validator == "required":
        # required 위반은 path 가 비어 있고, message 가 "'order_id' is a required property"
        return error.message.split("'")[1]
    if error.path:
        return str(error.path[0])
    return None  # additionalProperties 처럼 특정 칸으로 좁힐 수 없는 경우


def _parse_event_time(value: str) -> None:
    """ISO 8601 로 해석되지 않거나 시간대가 없으면 ValueError."""
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError(f"timezone is required: {value!r}")


def validate(raw: bytes | str) -> ValidationResult:
    """쪽지 한 장을 검사한다. R1 · R2 는 실패하면 즉시 반환하고, R3 · R4 는 문제를 모두 모은다."""
    # R1. 읽을 수 있나
    try:
        event = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        return ValidationResult(None, [Violation(INVALID_JSON, None, str(exc))])
    if not isinstance(event, dict):
        return ValidationResult(
            None,
            [Violation(NOT_AN_OBJECT, None, f"expected an object, got {type(event).__name__}")],
        )

    # R2. 아는 종류인가. 여기부터는 불합격이어도 원본을 DLQ 에 남기려고 event 를 함께 돌려준다.
    event_type = event.get("event_type")
    if event_type is None:
        return ValidationResult(
            event, [Violation(MISSING_EVENT_TYPE, "event_type", "event_type is missing")]
        )
    validator = _VALIDATORS.get(event_type)
    if validator is None:
        return ValidationResult(
            event,
            [Violation(UNKNOWN_EVENT_TYPE, "event_type", f"unknown event_type: {event_type!r}")],
        )

    # R3. 양식에 맞나. validate() 는 첫 문제에서 멈추므로 iter_errors() 로 전부 모은다.
    violations = [
        Violation(SCHEMA_VIOLATION, _field_of(error), error.message)
        for error in validator.iter_errors(event)
    ]

    # R4. 시각이 진짜 시각인가. 문자열일 때만 본다 (문자열이 아니면 R3 가 이미 잡았다).
    # JSON Schema 의 "format": "date-time" 은 기본적으로 검사되지 않아 여기서 직접 확인한다.
    event_time = event.get("event_time")
    if isinstance(event_time, str):
        try:
            _parse_event_time(event_time)
        except ValueError as exc:
            violations.append(Violation(INVALID_EVENT_TIME, "event_time", str(exc)))

    return ValidationResult(event, violations)
