# delivery-event-pipeline

[![ci](https://github.com/RaphaelAhn/delivery-event-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/RaphaelAhn/delivery-event-pipeline/actions/workflows/ci.yml)

합성 배달 이벤트(주문·배차·배달 완료)를 **Kafka로 수집 → 검증 → ClickHouse 적재 → dbt로 정제·마트 생성**하는
로컬 데이터 파이프라인입니다.

설계 문서(이벤트 계약, 지표 정의, 운영 Runbook)는
[data-portfolio / delivery-data-platform](https://github.com/RaphaelAhn/data-portfolio/tree/main/projects/delivery-data-platform)에 있고,
이 저장소는 그 설계를 실제로 실행되는 코드로 구현합니다.

> 진행 중: 1주차 (수집 → 적재 → staging). 아래 체크리스트 참고.

## 아키텍처

```
generator ──► Kafka (orders / dispatch / delivery.events)
                 │
                 ▼
          Python consumer ── 계약 위반 ──► raw_dlq_events
                 │
                 ▼
     ClickHouse raw_*_events (들어온 그대로 보존)
                 │
                 ▼
     dbt staging (타입 정리, event_id 중복 제거)
                 │
                 ▼
     fct_delivery_order ──► mart_delivery_daily
```

## 일부러 섞는 데이터 문제

실제 로그에서 흔한 문제를 generator가 비율만큼 섞고, 무엇을 섞었는지 `*.manifest.json`에 정답으로 남깁니다.
파이프라인이 이 문제를 제대로 처리하는지 정답과 비교해 검증합니다.

| 종류 | 내용 | 파이프라인에서 처리하는 곳 |
|---|---|---|
| duplicate | 같은 `event_id` 재전송 | dbt staging 중복 제거 |
| late | 10~120분 늦게 도착 | event_time 기준 집계, 재처리 |
| out_of_order | 부모 주문보다 먼저 도착 | 적재 시 orphan으로 버리지 않음 |
| invalid | 필수 필드 누락, 잘못된 값 | 컨슈머 검증 → DLQ |

## 실행

필요: Docker Desktop, Python 3.11+

```bash
python -m venv .venv
.venv\Scripts\activate          # macOS/Linux: source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env

# 컨테이너 기동 + 토픽 생성
./scripts/up.ps1                # macOS/Linux: ./scripts/up.sh

# 이벤트 생성 → Kafka 발행
python -m pipeline.producer.generator --orders 1000
python -m pipeline.producer.publish
```

Kafka UI: http://localhost:8080

## 테스트

```bash
pytest -q
ruff check .
```

검사원이 불량 이벤트를 제대로 잡는지는 생성기가 남긴 정답지(manifest)와 대조해 채점합니다.

```bash
python scripts/score_validator.py --orders 2000 --seed 21
```

## 설계 결정

- [0001. DuckDB 대신 ClickHouse](docs/decisions/0001-clickhouse-over-duckdb.md)
- [0002. at-least-once와 다운스트림 중복 제거](docs/decisions/0002-at-least-once-and-downstream-dedup.md)

## 진행 상황

- [x] Docker Compose (Kafka KRaft, ClickHouse, Kafka UI)
- [x] 이벤트 계약 (`contracts/`)
- [x] 이상 이벤트 비율을 조절하는 generator + 단위 테스트
- [x] Kafka 발행 (`publish.py`)
- [x] 이벤트 검증 규칙 (`consumer/validate.py`) — 정답지 대비 precision·recall 1.0
- [ ] ClickHouse 원천 테이블과 컨슈머 적재, DLQ
- [ ] dbt staging 모델과 테스트
- [ ] `fct_delivery_order`, end-to-end 실행 스크립트
- [ ] 결과 수치 (처리량, DLQ 비율, 중복 제거 정확도)

## 한계

- 합성 데이터이며 실제 서비스의 주문량·배차 로직을 대표하지 않습니다.
- 단일 노드 Kafka와 ClickHouse로 구성된 로컬 환경이며, 운영 규모의 처리량이나 SLA를 주장하지 않습니다.
