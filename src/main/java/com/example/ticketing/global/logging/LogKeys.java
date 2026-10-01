package com.example.ticketing.global.logging;

/**
 * 구조화 로그 공통 필드 이름 (MDC 키, JSON 필드명)
 * Loki/Grafana 검색 기준이므로 이름 변경 시 대시보드·쿼리·런북 함께 수정
 */
public final class LogKeys {
    public static final String REQUEST_ID = "requestId";                // HTTP 요청 단위 ID (X-Request-Id)
    public static final String BOOKING_REQUEST_ID = "bookingRequestId"; // 예약 요청 단위 ID (API → SQS → Worker 공통)
    public static final String USER_ID = "userId";
    public static final String SHOW_ID = "showId";
    public static final String SEAT_ID = "seatId";
    public static final String EVENT = "event";                         // 이벤트 이름 (예: booking.sqs_sent)

    private LogKeys() {
    }
}
