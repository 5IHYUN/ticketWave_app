# k6 부하 테스트

## 구성
| 파일 | 용도 |
|---|---|
| `baseline.js` | 기준값 측정. 정상 예매 흐름을 일정 부하로 반복 |
| `lib/api.js` | API 호출 공통 함수 (요청별 `name` 태그로 엔드포인트별 지표 분리) |
| `scripts/reset-local.sh` | 로컬 테스트 데이터 정리 (테스트 사용자 데이터만, FLUSHALL 미사용) |
| `results/` | 측정 결과 JSON (`<날짜>-<커밋SHA>-<시나리오>.json`) |

## baseline.js 흐름
```
setup   테스트 사용자 loadtest_1~N 회원가입/로그인, 대상 공연 선택 (SHOW_ID 미지정 시 목록 첫 공연)
반복    대기열 입장 → (WAITING 시 1초 폴링) → 좌석 조회 → 내 구역 빈 좌석 선점
        → 예약 요청 → 결과 폴링 → (CONFIRMED 시) 예약 취소 → 1초 대기
```
- VU = 동시 사용자 1명, VU마다 전용 계정과 전용 좌석 구역 사용 → VU 간 좌석 경합 최소화
- 확정 후 예약 취소 → 좌석 재사용, 360석 공연에서도 매진 없이 일정 부하 유지
- 경합 상황(여러 명이 같은 좌석)은 별도 시나리오로 분리 예정

## 사전 조건
- `docker compose up -d` 기동 완료 (`curl localhost:8080/actuator/health` → UP)
- k6 설치: `brew install k6`

## 실행
저장소 루트에서 실행
```bash
SHA=$(git rev-parse --short HEAD)
TS=$(date +%Y%m%d-%H%M)
k6 run -e GIT_SHA=$SHA --summary-export=tests/k6/results/$TS-$SHA-baseline.json tests/k6/baseline.js
```

### 환경변수
| 이름 | 기본값 | 설명 |
|---|---|---|
| `BASE_URL` | `http://localhost:8080` | 대상 서버 |
| `VUS` | 30 | 동시 사용자 수 (= 테스트 계정 수) |
| `DURATION` | 3m | 측정 시간 |
| `SHOW_ID` | 공연 목록 첫 번째 | 대상 공연 |
| `TEST_PASSWORD` | test1234 | 테스트 계정 비밀번호 |
| `QUEUE_TIMEOUT_MS` | 60000 | 대기열 입장 최대 대기 |
| `RESULT_TIMEOUT_MS` | 30000 | 예약 결과 최대 대기 |
| `RESULT_POLL_SEC` | 0.25 | 결과 폴링 간격 (= `booking_e2e_ms` 측정 해상도) |
| `THINK_TIME_SEC` | 1 | 반복 간 대기 |
| `CANCEL_AFTER_CONFIRM` | true | 확정 후 예약 취소 여부 |

## 지표
| 지표 | 의미 |
|---|---|
| `http_req_duration{name:...}` | 엔드포인트별 응답 시간 (queue_enter, queue_status, seats, hold, booking, booking_status) |
| `http_req_failed{phase:main}` | 측정 대상 요청 실패율 (409는 정상 업무 응답으로 제외) |
| `queue_wait_ms` | 입장 시도 → Queue Token 수령 시간 |
| `booking_e2e_ms` | 예약 접수(202) → 확정 확인 시간 (클라이언트 관점, 폴링 간격만큼 오차) |
| `booking_confirm_rate` | 예약 접수 대비 확정 비율 |
| `seat_hold_conflict`, `booking_hold_expired` | 좌석 경합, 선점 만료 |
| `booking_conflict`, `booking_failed`, `booking_result_timeout` | 정상 거절, 시스템 실패, Worker 적체 신호 |
| `unexpected_error` | 예상 외 응답 코드 (step 태그로 단계 구분) |

서버 관점 e2e는 Prometheus `booking_confirm_e2e_seconds`로 함께 확인

### 서버 기준 e2e 평균 (실행 전후 차이)
`booking_confirm_e2e_seconds`는 앱 기동 후 누적값 → 실행 전후 값 차이로 이번 실행분만 계산
```bash
e2e() { curl -s localhost:8080/actuator/prometheus | awk '/^booking_confirm_e2e_seconds_count/{c=$2} /^booking_confirm_e2e_seconds_sum/{s=$2} END{print c, s}'; }
read C0 S0 <<< "$(e2e)"
k6 run ... tests/k6/baseline.js
read C1 S1 <<< "$(e2e)"
awk -v c="$((${C1%.*}-${C0%.*}))" -v s="$(echo "$S1 - $S0" | bc)" 'BEGIN{printf "server e2e: count=%d avg=%.1fms\n", c, s/c*1000}'
```

## 결과 기록
- 결과 JSON과 함께 아래 항목 기록: 커밋 SHA, 실행 환경(로컬/AWS dev), VUS/DURATION, 특이사항
- 팀 프로젝트 당시 수치와 혼용 금지 (측정 조건 상이)

## 정리
테스트 중단 등으로 예약이 남은 경우
```bash
bash tests/k6/scripts/reset-local.sh
```
