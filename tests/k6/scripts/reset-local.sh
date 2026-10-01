#!/usr/bin/env bash
# 로컬 부하 테스트 데이터 정리 (docker compose 환경 전용)
# 대상: 테스트 사용자(loadtest_%@example.com)의 예약, 해당 좌석 상태, 관련 Redis 키
# 일반 사용자 데이터, 다른 Redis 키는 건드리지 않음 (FLUSHALL 미사용)
set -euo pipefail

MYSQL=(docker exec -i team5-ticket-mysql mysql -uticketing -pticketing1234 ticketing -N)
REDIS=(docker exec -i team5-ticket-redis redis-cli)

USER_IDS=$("${MYSQL[@]}" -e "SELECT user_id FROM users WHERE email LIKE 'loadtest\_%@example.com';" 2>/dev/null)
SEAT_IDS=$("${MYSQL[@]}" -e "SELECT b.seat_id FROM bookings b JOIN users u ON u.user_id=b.user_id WHERE u.email LIKE 'loadtest\_%@example.com';" 2>/dev/null)

echo "[reset] 테스트 사용자 $(echo "$USER_IDS" | grep -c . || true)명, 남은 예약 $(echo "$SEAT_IDS" | grep -c . || true)건"

# 1) 예약 삭제, 좌석 AVAILABLE 복구, 잔여석 재계산
"${MYSQL[@]}" 2>/dev/null <<'SQL'
UPDATE seats s JOIN bookings b ON b.seat_id = s.seat_id JOIN users u ON u.user_id = b.user_id
   SET s.status = 'AVAILABLE'
 WHERE u.email LIKE 'loadtest\_%@example.com';
DELETE b FROM bookings b JOIN users u ON u.user_id = b.user_id
 WHERE u.email LIKE 'loadtest\_%@example.com';
UPDATE seat_grades g
   SET g.remaining_seats = (SELECT COUNT(*) FROM seats s
                             WHERE s.show_id = g.show_id AND s.price = g.price AND s.status = 'AVAILABLE');
SQL

# 2) 해당 좌석의 sold 키 삭제
for sid in $SEAT_IDS; do "${REDIS[@]}" DEL "sold:$sid" >/dev/null; done

# 3) 테스트 사용자의 대기열/입장/토큰/선점 키 삭제
for uid in $USER_IDS; do
  for key in $("${REDIS[@]}" --scan --pattern 'queue:active:*') $("${REDIS[@]}" --scan --pattern 'queue:waiting:*'); do
    "${REDIS[@]}" ZREM "$key" "$uid" >/dev/null
  done
  for key in $("${REDIS[@]}" --scan --pattern "queue:user-token:*:$uid"); do
    tok=$("${REDIS[@]}" GET "$key")
    "${REDIS[@]}" DEL "$key" "queue:token:$tok" >/dev/null
  done
done
"${REDIS[@]}" --scan --pattern 'seat:*' | while read -r key; do
  holder=$("${REDIS[@]}" GET "$key")
  if echo "$USER_IDS" | grep -qx "$holder"; then "${REDIS[@]}" DEL "$key" >/dev/null; fi
done

echo "[reset] 완료"
