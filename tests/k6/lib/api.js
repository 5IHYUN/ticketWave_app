// Ticket Wave API 호출 공통 함수
// 각 요청에 name 태그 지정 → 엔드포인트별 지표 분리 (http_req_duration{name:hold} 등)
import http from 'k6/http';
import { sleep } from 'k6';

export const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const API = `${BASE_URL}/api/v1`;
const JSON_HEADERS = { 'Content-Type': 'application/json' };

function auth(jwt, queueToken) {
  const h = { Authorization: `Bearer ${jwt}`, 'Content-Type': 'application/json' };
  if (queueToken) h['X-Queue-Token'] = queueToken;
  return h;
}

// 회원가입(이미 있으면 409 무시) 후 로그인, JWT 반환
export function signupAndLogin(email, password, name) {
  // 이미 가입된 계정이면 4xx/5xx 응답 → 실패율 집계에서 제외 (바로 로그인 시도)
  http.post(`${API}/auth/signup`, JSON.stringify({ email, password, name }),
    { headers: JSON_HEADERS, tags: { name: 'signup', phase: 'setup' },
      responseCallback: http.expectedStatuses({ min: 200, max: 599 }) });
  const res = http.post(`${API}/auth/login`, JSON.stringify({ email, password }),
    { headers: JSON_HEADERS, tags: { name: 'login', phase: 'setup' } });
  if (res.status !== 200) {
    console.error(`login 실패 ${email}: ${res.status} ${res.body}`);
    return null;
  }
  return res.json('accessToken');
}

// 공연 목록 조회
export function listShows() {
  const res = http.get(`${API}/shows`, { tags: { name: 'shows', phase: 'setup' } });
  return res.status === 200 ? res.json() : [];
}

// 대기열 입장 → WAITING이면 status 폴링 → ACTIVE 시 Queue Token 반환
// 반환: { token, waitMs } / 실패 시 token null
export function enterQueue(jwt, showId, timeoutMs, pollSec) {
  const start = Date.now();
  const res = http.post(`${API}/shows/${showId}/queue/enter`, null,
    { headers: auth(jwt), tags: { name: 'queue_enter' } });
  if (res.status !== 200) return { token: null, waitMs: Date.now() - start, status: res.status };

  let body = res.json();
  while (body.status !== 'ACTIVE' && Date.now() - start < timeoutMs) {
    sleep(pollSec);
    const st = http.get(`${API}/shows/${showId}/queue/status`,
      { headers: auth(jwt), tags: { name: 'queue_status' } });
    if (st.status !== 200) return { token: null, waitMs: Date.now() - start, status: st.status };
    body = st.json();
  }
  return { token: body.status === 'ACTIVE' ? body.queueToken : null, waitMs: Date.now() - start, status: 200 };
}

// 좌석 목록 조회 (Queue Token 필요)
export function getSeats(jwt, queueToken, showId) {
  const res = http.get(`${API}/performances/${showId}/seats`,
    { headers: auth(jwt, queueToken), tags: { name: 'seats' } });
  return res.status === 200 ? res.json() : null;
}

// 좌석 선점: 200 성공 / 409 이미 선점·판매
export function holdSeat(jwt, queueToken, seatId) {
  return http.post(`${API}/seats/${seatId}/hold`, null,
    { headers: auth(jwt, queueToken), tags: { name: 'hold' } });
}

// 예약 요청: 202 접수(requestId) / 409 선점 만료
export function requestBooking(jwt, queueToken, seatId) {
  return http.post(`${API}/bookings`, JSON.stringify({ seatId }),
    { headers: auth(jwt, queueToken), tags: { name: 'booking' } });
}

// 예약 결과 폴링: CONFIRMED | CONFLICT | FAILED | TIMEOUT
export function pollBookingResult(jwt, requestId, timeoutMs, pollSec) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    sleep(pollSec);
    const res = http.get(`${API}/bookings/status/${requestId}`,
      { headers: auth(jwt), tags: { name: 'booking_status' } });
    if (res.status === 200) {
      const s = res.json('status');
      if (s !== 'PROCESSING') return { status: s, elapsedMs: Date.now() - start };
    }
  }
  return { status: 'TIMEOUT', elapsedMs: Date.now() - start };
}

// 내 예약 전체 취소 (좌석 재사용 목적, 측정 대상 아님)
export function cancelMyBookings(jwt) {
  const res = http.get(`${API}/bookings/me`, { headers: auth(jwt), tags: { name: 'my_bookings', phase: 'cleanup' } });
  if (res.status !== 200) return 0;
  let n = 0;
  // 취소 요청에 출처 식별 X-Request-Id 부여 (k6 실행 Pod·VU·반복 번호, 서버 로그 requestId로 기록)
  //   같은 예약 중복 취소 시 어느 실행·VU가 보냈는지 확인 (analysis/09)
  const host = String(__ENV.HOSTNAME || 'local').replace(/[^A-Za-z0-9-]/g, '').slice(-24);
  for (const b of res.json()) {
    const reqId = `k6-${host}-vu${__VU}-i${__ITER}-${String(b.bookingId).slice(0, 8)}`.slice(0, 64);
    const h = auth(jwt);
    h['X-Request-Id'] = reqId;
    const d = http.del(`${API}/bookings/${b.bookingId}`, null,
      { headers: h, tags: { name: 'cancel', phase: 'cleanup' } });
    if (d.status === 204) n++;
  }
  return n;
}
