package com.example.ticketing.booking.service;

import com.example.ticketing.booking.entity.Booking;
import com.example.ticketing.seat.entity.Seat;
import com.example.ticketing.show.repository.SeatGradeRepository;
import com.example.ticketing.booking.sqs.BookingMessage;
import com.example.ticketing.global.exception.ConflictException;
import com.example.ticketing.global.exception.ErrorCode;
import com.example.ticketing.global.exception.NotFoundException;
import com.example.ticketing.queue.service.QueueService;
import com.example.ticketing.seat.repository.SeatRepository;
import io.swagger.v3.oas.annotations.media.Schema;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import io.micrometer.core.instrument.MeterRegistry;
import com.example.ticketing.global.logging.LogKeys;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import com.example.ticketing.booking.repository.BookingRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static net.logstash.logback.argument.StructuredArguments.kv;

@Slf4j
@Service
@RequiredArgsConstructor
public class BookingService {
    private final StringRedisTemplate redisTemplate;
    private final SqsTemplate sqsTemplate;
    private final BookingRepository bookingRepository;
    private final SeatRepository seatRepository;
    private final QueueService queueService;
    private final SeatGradeRepository seatGradeRepository;
    private final MeterRegistry meterRegistry;

    @Value("${app.sqs.booking-queue:booking-queue}")
    private String bookingQueue;

    // seat_id -> show_id 로컬 캐시.
    // 좌석의 소속 공연(show)은 불변이므로 만료 없이 영구 캐시 가능.
    // requestBooking 진입 시 매 요청 DB 조회(findShowIdBySeatId)를 제거하여
    // booking-hikari 커넥션 풀 경합(pending 적체)의 근본 원인을 없앤다.
    private final ConcurrentHashMap<Long, Long> seatShowCache = new ConcurrentHashMap<>();

    public BookingAcceptResponse requestBooking(Long seatId, String userId, String queueToken) {
        // 캐시 우선 조회: 미스일 때만 DB 1회, 이후 동일 seatId는 캐시 히트(불변 데이터).
        // computeIfAbsent 는 seatId 단위로 락이 분산되고, 미스는 좌석당 최초 1회뿐이라
        // 매처(전 요청이 단일 노드 경쟁) 케이스와 달리 락 경합이 사실상 없다.
        Long showId = seatShowCache.computeIfAbsent(seatId, id ->
                seatRepository.findShowIdBySeatId(id)
                        .orElseThrow(() -> new NotFoundException("좌석이 존재하지 않습니다.")));

        // 서비스 계층에서 Queue Token을 한 번 더 검증
        queueService.validateQueueToken(queueToken, showId, userId);

        String holdKey = "seat:" + seatId;
        String requestId = UUID.randomUUID().toString();
        // 이후 로그(API 구간)에 예약 요청 ID, 좌석, 공연 포함 → Worker 로그와 같은 키로 검색
        MDC.put(LogKeys.BOOKING_REQUEST_ID, requestId);
        MDC.put(LogKeys.SHOW_ID, String.valueOf(showId));
        MDC.put(LogKeys.SEAT_ID, String.valueOf(seatId));

        // Redis Lua Script: HOLD 여부 확인 및 즉시 삭제(Settle)
        String lua = """
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                redis.call('DEL', KEYS[1])
                redis.call('SET', 'sold:' .. ARGV[2], ARGV[1], 'EX', 600)
                return 1
            else
                return 0
            end
            """;

        Long result = redisTemplate.execute(
                new DefaultRedisScript<>(lua, Long.class),
                List.of(holdKey),
                userId,
                String.valueOf(seatId)
        );

        if (result == null || result == 0) {
            throw new ConflictException(ErrorCode.HOLD_EXPIRED);
        }

        // FIFO 큐 전송: 공연별 순서 보장 및 중복 방지 헤더 포함
        // HTTP 요청 ID를 메시지에 실어 Worker 로그와 연결
        // 요청 스레드에서 동기 전송 → SQS 지연이 곧 사용자 응답 지연 (전송 시간 메트릭·로그로 상시 관측)
        long sendStart = System.nanoTime();
        try {
            sqsTemplate.send(to -> to
                .queue(bookingQueue)
                .payload(new BookingMessage(requestId, showId, seatId, userId, queueToken,
                        System.currentTimeMillis(), MDC.get(LogKeys.REQUEST_ID)))
                .header("message-group-id", String.valueOf(showId))
                .header("message-deduplication-id", requestId)
            );
        } catch (RuntimeException e) {
            recordSqsSend(sendStart, "error");   // 전송 실패도 소요 시간·건수 기록 (예외는 GlobalExceptionHandler에서 로그)
            throw e;
        }
        long sqsSendMs = recordSqsSend(sendStart, "success");
        log.info("예약 요청 SQS 전송 {} {}", kv(LogKeys.EVENT, "booking.sqs_sent"), kv("sqsSendMs", sqsSendMs));

        return new BookingAcceptResponse(requestId, "ACCEPTED");
    }

    /**
     * SQS 전송 시간 기록 → booking_sqs_send_seconds{outcome="success|error"} 히스토그램
     * 로그(sqsSendMs)는 요청 단위 원인 추적용, 메트릭은 대시보드·알림용
     */
    private long recordSqsSend(long startNanos, String outcome) {
        long elapsedNanos = System.nanoTime() - startNanos;
        meterRegistry.timer("booking.sqs.send", "outcome", outcome).record(elapsedNanos, TimeUnit.NANOSECONDS);
        return elapsedNanos / 1_000_000;
    }

    public BookingStatusResponse getBookingStatus(String requestId) {
    	String status = redisTemplate.opsForValue().get("result:" + requestId);
    	if (status == null) {
    		return new BookingStatusResponse(requestId, "PROCESSING");
    	}
        return new BookingStatusResponse(requestId, status);
    }

    @Transactional(readOnly = true)
    public List<UserBookingResponse> getUserBookings(String userId) {
        return bookingRepository.findByUserIdWithSeatAndShow(userId).stream()
                .map(booking -> new UserBookingResponse(
                        booking.getId(),
                        booking.getSeat().getShow().getTitle(),
                        booking.getSeat().getShow().getVenue(),
                        booking.getSeat().getSeatNumber(),
                        booking.getSeat().getPrice(),
                        booking.getBookedAt()
                ))
                .collect(Collectors.toList());
    }

    @Schema(description = "내 예매 내역 응답")
    public record UserBookingResponse(
            @Schema(description = "예매 ID", example = "BK-20240615-0001")
            String bookingId,
            @Schema(description = "공연 제목", example = "S-Tier Concert")
            String showTitle,
            @Schema(description = "공연장", example = "KSPO DOME")
            String venue,
            @Schema(description = "좌석 번호", example = "A-1")
            String seatNumber,
            @Schema(description = "예매 가격", example = "150000")
            int price,
            @Schema(description = "예매 일시", example = "2026-06-15T19:30:00")
            java.time.LocalDateTime bookedAt
    ) {}

   @Transactional
    public void cancelBooking(String bookingId, String userId) {
        // 취소 요청 추적 로그 (analysis/09: 같은 예약 동시 취소로 커밋 시 DELETE 0건 → 500, FOR UPDATE 조회로 404 처리)
        //   requested: 요청 진입 시점, 같은 bookingId로 여러 건이면 중복 취소 요청 → requestId로 출처 확인
        //   cancelled: 커밋 성공 후에만 기록 (커밋 실패 요청은 requested만 남음)
        log.info("예약 취소 요청 {} {}", kv(LogKeys.EVENT, "booking.cancel_requested"), kv("bookingId", bookingId));
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                log.info("예약 취소 완료 {} {}", kv(LogKeys.EVENT, "booking.cancelled"), kv("bookingId", bookingId));
            }
        });

        // 1) 예매 조회 + 행 잠금 (FOR UPDATE)
        //    동시 취소 시 두 번째 요청은 여기서 대기 → 첫 번째 커밋 후 행이 없으므로 404
        Booking booking = bookingRepository.findByIdForUpdate(bookingId)
                .orElseThrow(() -> new NotFoundException("예매 내역을 찾을 수 없습니다."));
 
        // 2) 본인 예매인지 검증
        if (!booking.getUser().getUserId().equals(userId)) {
            throw new ConflictException(ErrorCode.ACCESS_DENIED);
        }
 
        Seat seat = booking.getSeat();
        Long showId = seat.getShow().getShowId();
        int price = seat.getPrice();
        Long seatId = seat.getId();
 
        // 3) 좌석 상태 복구 SOLD -> AVAILABLE (더티체킹)
        seat.available();
 
        // 4) 잔여석 +1 (showId + price로 등급 역매칭)
        seatGradeRepository.findFirstByShowIdAndPrice(showId, price)
                .ifPresent(grade -> seatGradeRepository.increaseRemainingSeats(grade.getId()));
 
        // 5) 예매 레코드 삭제
        bookingRepository.delete(booking);
 
        // 6) Redis sold 키 정리 (재예매 가능하도록)
        redisTemplate.delete("sold:" + seatId);
    }

    @Schema(description = "예매 요청 접수 응답")
    public record BookingAcceptResponse(
            @Schema(description = "예매 요청 ID", example = "9a1f57bc-ff25-4a8b-a9a3-5c93847f0f52")
            String requestId,
            @Schema(description = "접수 상태", example = "ACCEPTED")
            String status
    ) {}

    @Schema(description = "예매 처리 상태 응답")
    public record BookingStatusResponse(
            @Schema(description = "예매 요청 ID", example = "9a1f57bc-ff25-4a8b-a9a3-5c93847f0f52")
            String requestId,
            @Schema(description = "처리 상태", example = "PROCESSING", allowableValues = {"PROCESSING", "CONFIRMED", "FAILED"})
            String status
    ) {}
}