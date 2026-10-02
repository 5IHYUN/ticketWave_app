# 로컬 관측 스택

## 구성
| 서비스 | 주소 | 역할 |
|---|---|---|
| Grafana | http://localhost:3000 | 로그·메트릭 조회 (로컬 전용 익명 관리자) |
| Loki | http://localhost:3100 | 로그 저장 (7일 보존) |
| Alloy | http://localhost:12345 | 앱 컨테이너 로그 수집 (파이프라인 상태 UI) |
| Prometheus | http://localhost:9090 | 앱 `/actuator/prometheus` 수집 (5초 주기, 7일 보존) |

```
app stdout(JSON 한 줄) → Alloy(docker 수집, JSON 파싱) → Loki → Grafana Explore
app /actuator/prometheus → Prometheus → Grafana
```

## 로그 라벨 기준
- 라벨(인덱스): `service`, `container`, `level` — 값 종류가 적은 필드만
- 고유 값(`requestId`, `bookingRequestId`, `userId`, `seatId`)은 라벨 미사용, `| json` 파싱 후 필터
  - 라벨로 쓰면 스트림 수 폭증 → Loki 성능 저하

## 자주 쓰는 LogQL
Grafana → Explore → 데이터소스 Loki

```logql
# 예약 1건 전 구간 추적 (API → SQS → Worker)
{service="ticket-service"} | json | bookingRequestId="<ID>"

# HTTP 요청 1건의 모든 로그
{service="ticket-service"} | json | requestId="<ID>"

# 느린 요청 (300ms 이상, 5xx)
{service="ticket-service"} | json | event="http.request"

# e2e 200ms 이상 예약 처리
{service="ticket-service"} | json | event="worker.processed" | e2eMs > 200

# 이미 판매된 좌석으로 CONFIRMED 처리된 경로 (정합성 점검)
{service="ticket-service"} | json | event="worker.already_sold"

# ERROR 로그
{service="ticket-service", level="ERROR"}
```

### 로그 기반 지표
```logql
# 구간별 p95 (1분 단위): sqsSendMs / queueWaitMs / processMs
quantile_over_time(0.95, {service="ticket-service"} | json | event="booking.sqs_sent" | unwrap sqsSendMs [1m]) by ()
quantile_over_time(0.95, {service="ticket-service"} | json | event="worker.received" | unwrap queueWaitMs [1m]) by ()
quantile_over_time(0.95, {service="ticket-service"} | json | event="worker.processed" | unwrap processMs [1m]) by ()

# 레벨별 로그 발생량
sum by (level) (count_over_time({service="ticket-service"}[1m]))
```
`by ()` 생략 시 JSON 필드 전체가 라벨로 남아 결과가 요청 단위로 쪼개짐

## 확인
```bash
docker compose up -d
curl -s localhost:3100/ready                                  # ready
curl -s localhost:9090/api/v1/targets | grep -o '"health":"[a-z]*"'   # "health":"up"
```
Alloy UI(http://localhost:12345) → `loki.source.docker.app` 정상(초록) 확인
