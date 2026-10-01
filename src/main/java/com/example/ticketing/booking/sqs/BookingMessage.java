package com.example.ticketing.booking.sqs;

import java.io.Serializable;

public record BookingMessage(
    String requestId,          // 예약 요청 ID (= 로그의 bookingRequestId, 결과 조회 키)
    Long showId,
    Long seatId,
    String userId,
    String queueToken,
    long createdAtEpochMs,     // 큐 인입 시각 (e2e 확정시간 측정용)
    String httpRequestId       // 예약 요청을 보낸 HTTP 요청 ID (API 로그와 Worker 로그 연결용, 구버전 메시지는 null)
) implements Serializable {}
