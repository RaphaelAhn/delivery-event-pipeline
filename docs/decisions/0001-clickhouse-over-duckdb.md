# 0001. 저장소로 DuckDB 대신 ClickHouse를 쓴다

- 상태: 채택
- 날짜: 2026-09-22

## 배경
컨슈머는 이벤트를 계속 적재하고, dbt는 주기적으로 같은 원천 테이블을 읽어 모델을 만든다.
이전 실습(`commerce-dbt-lab`)에서는 DuckDB 파일 하나를 사용했다.

## 결정
ClickHouse를 Docker로 띄워 원천 테이블과 dbt 모델을 모두 여기에 둔다.

## 이유
- DuckDB 파일은 한 번에 한 프로세스만 쓸 수 있어서, 컨슈머가 적재하는 동안 dbt를 돌릴 수 없다.
- 이벤트 로그처럼 추가만 되는 대용량 데이터에 맞는 컬럼형 저장소다 (MergeTree, 파티션, TTL).
- `dbt-clickhouse` 어댑터가 있어서 이전 실습의 dbt 패턴을 그대로 옮길 수 있다.

## 감수하는 것
- 로컬 실행에 Docker가 필요하고 메모리를 더 쓴다.
- ClickHouse는 행 단위 UPDATE/DELETE가 비싸다. 중복 제거는 적재 시점이 아니라
  dbt 모델(또는 ReplacingMergeTree)에서 한다 → [0002](0002-at-least-once-and-downstream-dedup.md)
