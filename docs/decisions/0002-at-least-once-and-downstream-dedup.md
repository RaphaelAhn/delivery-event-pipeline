# 0002. at-least-once로 받고, 중복 제거는 다운스트림에서 한다

- 상태: 채택
- 날짜: 2026-09-22

## 배경
실제 로그 수집에서는 클라이언트 재전송, 컨슈머 재시작 등으로 같은 이벤트가 두 번 이상 들어온다.
exactly-once를 파이프라인 전체에서 보장하려면 Kafka 트랜잭션과 저장소 쪽 원자적 커밋이 필요하다.

## 결정
- 프로듀서: `enable.idempotence=true`로 전송 재시도에 의한 중복만 막는다.
- 컨슈머: 적재에 성공한 뒤 오프셋을 커밋한다 (at-least-once). 재시작하면 일부가 다시 적재될 수 있다.
- 중복 제거: dbt staging 모델에서 `event_id` 기준으로 한 건만 남긴다.

## 이유
- 컨슈머를 단순하게 유지할 수 있다.
- 원천 테이블에는 들어온 그대로를 남기므로, 중복이 얼마나 들어왔는지를 품질 지표로 측정할 수 있다.
- generator가 섞는 duplicate(같은 `event_id`의 재전송)는 어차피 프로듀서 idempotence로 막을 수 없다.

## 검증 방법
- `data/*.manifest.json`의 `injected.duplicate` 건수와 원천 테이블 중복 건수가 일치하는지
- staging 모델에서 `event_id`가 unique 테스트를 통과하는지
