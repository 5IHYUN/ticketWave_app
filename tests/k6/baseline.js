// 기준값(baseline) 측정 시나리오
// - VU별 전용 테스트 사용자(loadtest_<n>@example.com)가 실제 화면과 같은 순서로 예매 반복
//   대기열 입장 → (대기 시 폴링) → 좌석 조회 → 선점 → 예약 → 결과 폴링 → (확정 시) 예약 취소
// - VU별 좌석 구역 분리 → VU 간 좌석 경합 최소화 (정상 흐름 측정 목적)
// - 확정 후 예약 취소 → 좌석 재사용, 공연당 360석 매진 없이 일정 부하 유지
//
// 실행 방법, 결과 저장 방법은 tests/k6/README.md 참고
import { sleep } from 'k6';
import http from 'k6/http';
import { Counter, Rate, Trend } from 'k6/metrics';
import * as api from './lib/api.js';

// ===== 설정 (환경변수로 덮어쓰기) =====
const VUS = parseInt(__ENV.VUS || '30');
const DURATION = __ENV.DURATION || '3m';
const PASSWORD = __ENV.TEST_PASSWORD || 'test1234';
const QUEUE_TIMEOUT_MS = parseInt(__ENV.QUEUE_TIMEOUT_MS || '60000');
const RESULT_TIMEOUT_MS = parseInt(__ENV.RESULT_TIMEOUT_MS || '30000');
const RESULT_POLL_SEC = parseFloat(__ENV.RESULT_POLL_SEC || '0.25'); // 결과 폴링 간격 = e2e 측정 해상도
const THINK_TIME_SEC = parseFloat(__ENV.THINK_TIME_SEC || '1');
const CANCEL_AFTER_CONFIRM = (__ENV.CANCEL_AFTER_CONFIRM || 'true') === 'true';
const GIT_SHA = __ENV.GIT_SHA || 'unknown';  // 실행 로그 기록용
// 도착률 모드 (장애 시나리오용, analysis/11)
//   RATE 미지정: constant-vus (closed-loop, 기준값 측정) — VU가 결과를 받아야 다음 요청 → 서버가 느려지면 유입도 감소
//   RATE 지정: constant-arrival-rate (open-loop) — 서버 상태와 무관하게 초당 RATE건 시작 → 실제 티켓 오픈처럼 적체 발생
//     VUS = 최대 VU 수 = 테스트 계정 수. 부족하면 k6 dropped_iterations 증가
const RATE = parseInt(__ENV.RATE || '0');

// 409(선점 경합, 선점 만료)는 정상 업무 응답 → http_req_failed에서 제외
http.setResponseCallback(http.expectedStatuses({ min: 200, max: 299 }, 409));

// ===== 커스텀 메트릭 =====
const queueWait = new Trend('queue_wait_ms', true);            // 입장 시도 → Queue Token 수령
const queueTimeout = new Counter('queue_timeout');             // 제한 시간 내 미입장
const holdOk = new Counter('seat_hold_ok');
const holdConflict = new Counter('seat_hold_conflict');        // 다른 사용자가 선점·판매
const noSeat = new Counter('seat_unavailable');                // 선택 가능한 좌석 없음
const bookingAccepted = new Counter('booking_accepted');       // 202 (SQS 전송)
const bookingHoldExpired = new Counter('booking_hold_expired');// 409 (선점 만료)
const confirmed = new Counter('booking_confirmed');
const conflicted = new Counter('booking_conflict');            // DB Unique 위반 (정상 거절)
const failed = new Counter('booking_failed');                  // 시스템 실패
const resultTimeout = new Counter('booking_result_timeout');   // 제한 시간 내 결과 없음 (Worker 적체 신호)
const e2eConfirm = new Trend('booking_e2e_ms', true);          // 예약 접수 → 확정 (SLI)
const confirmRate = new Rate('booking_confirm_rate');          // 예약 접수 대비 확정 비율
const unexpected = new Counter('unexpected_error');            // 예상 외 응답 코드

export const options = {
  setupTimeout: '2m',
  scenarios: {
    baseline: RATE > 0
      ? { executor: 'constant-arrival-rate', rate: RATE, timeUnit: '1s', duration: DURATION,
          preAllocatedVUs: Math.min(VUS, RATE * 3), maxVUs: VUS, gracefulStop: '40s' }
      : { executor: 'constant-vus', vus: VUS, duration: DURATION, gracefulStop: '40s' },
  },
  thresholds: {
    // 기준값 측정용 관찰 기준 (SLO 아님, 실패해도 결과는 기록)
    'http_req_failed{phase:main}': ['rate<0.01'],
    'http_req_duration{name:hold}': ['p(95)<500'],
    'http_req_duration{name:booking}': ['p(95)<500'],
    // 엔드포인트별 응답 시간 기록용 (k6는 임계치가 있는 태그 지표만 요약/결과 JSON에 포함)
    'http_req_duration{name:seats}': ['p(95)<1000'],
    'http_req_duration{name:queue_enter}': ['p(95)<1000'],
    'http_req_duration{name:booking_status}': ['p(95)<1000'],
    booking_e2e_ms: ['p(95)<5000'],
    booking_confirm_rate: ['rate>0.95'],
  },
  // 측정 대상 요청에만 phase:main 부여, setup/cleanup 요청은 별도 phase
  tags: { phase: 'main' },
};

// ===== setup: 테스트 사용자 준비, 대상 공연 선택 =====
export function setup() {
  let showId = __ENV.SHOW_ID;
  if (!showId) {
    const shows = api.listShows();
    if (shows.length === 0) throw new Error('공연 없음 — 앱 기동/더미 데이터 확인');
    showId = String(shows[0].showId);
  }

  const users = [];
  for (let i = 1; i <= VUS; i++) {
    const jwt = api.signupAndLogin(`loadtest_${i}@example.com`, PASSWORD, `loadtest${i}`);
    if (jwt) users.push(jwt);
  }
  if (users.length < VUS) throw new Error(`테스트 사용자 준비 실패: ${users.length}/${VUS}`);

  console.log(`baseline 시작 — showId=${showId}, VUs=${VUS}, rate=${RATE || 'closed-loop'}, duration=${DURATION}, sha=${GIT_SHA}`);
  return { showId, users };
}

// ===== VU 반복 =====
export default function (data) {
  const vuIdx = (__VU - 1) % VUS;
  const jwt = data.users[vuIdx];
  const showId = data.showId;

  // 1) 대기열 입장
  const q = api.enterQueue(jwt, showId, QUEUE_TIMEOUT_MS, 1);
  if (!q.token) {
    if (q.status === 200) queueTimeout.add(1); else unexpected.add(1, { step: 'queue' });
    sleep(THINK_TIME_SEC);
    return;
  }
  queueWait.add(q.waitMs);

  // 2) 좌석 조회 → 내 구역(seatId 순서 기준 vuIdx 나머지)의 빈 좌석 선택
  const seats = api.getSeats(jwt, q.token, showId);
  if (!seats) { unexpected.add(1, { step: 'seats' }); sleep(THINK_TIME_SEC); return; }
  seats.sort((a, b) => a.seatId - b.seatId);
  const mine = seats.filter((s, i) => i % VUS === vuIdx && s.status === 'AVAILABLE');
  const pool = mine.length > 0 ? mine : seats.filter((s) => s.status === 'AVAILABLE');
  if (pool.length === 0) { noSeat.add(1); sleep(THINK_TIME_SEC); return; }
  const seatId = pool[Math.floor(Math.random() * pool.length)].seatId;

  // 3) 좌석 선점
  const hold = api.holdSeat(jwt, q.token, seatId);
  if (hold.status === 409) { holdConflict.add(1); sleep(THINK_TIME_SEC); return; }
  if (hold.status !== 200) { unexpected.add(1, { step: 'hold' }); sleep(THINK_TIME_SEC); return; }
  holdOk.add(1);

  // 4) 예약 요청
  const booking = api.requestBooking(jwt, q.token, seatId);
  if (booking.status === 409) { bookingHoldExpired.add(1); sleep(THINK_TIME_SEC); return; }
  if (booking.status !== 202) { unexpected.add(1, { step: 'booking' }); sleep(THINK_TIME_SEC); return; }
  bookingAccepted.add(1);

  // 5) 결과 폴링
  const r = api.pollBookingResult(jwt, booking.json('requestId'), RESULT_TIMEOUT_MS, RESULT_POLL_SEC);
  if (r.status === 'CONFIRMED') { confirmed.add(1); confirmRate.add(true); e2eConfirm.add(r.elapsedMs); }
  else if (r.status === 'CONFLICT') { conflicted.add(1); confirmRate.add(false); }
  else if (r.status === 'FAILED') { failed.add(1); confirmRate.add(false); }
  else { resultTimeout.add(1); confirmRate.add(false); }

  // 6) 좌석 재사용을 위한 예약 취소
  if (r.status === 'CONFIRMED' && CANCEL_AFTER_CONFIRM) api.cancelMyBookings(jwt);

  sleep(THINK_TIME_SEC);
}
