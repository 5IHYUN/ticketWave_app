package com.example.ticketing.booking.worker;

import com.example.ticketing.booking.sqs.BookingMessage;
import com.example.ticketing.global.fault.FaultInjector;
import com.example.ticketing.global.logging.LogKeys;
import com.example.ticketing.queue.service.QueueService;
import io.awspring.cloud.sqs.annotation.SqsListener;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.slf4j.MDC;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import static net.logstash.logback.argument.StructuredArguments.kv;

@Component
@Profile("worker")
@Slf4j
@RequiredArgsConstructor
public class BookingWorker {
    private final BookingProcessor bookingProcessor;
    private final StringRedisTemplate redisTemplate;
    private final MeterRegistry meterRegistry;
    private final QueueService queueService;
    private final FaultInjector faultInjector;
    private static final Duration RESULT_TTL = Duration.ofMinutes(5);

    @SqsListener("${app.sqs.booking-queue:booking-queue}")
    public void confirmBooking(BookingMessage message) {
        // 메시지의 추적 키를 MDC에 복원 → 이 메시지 처리 중 모든 로그에 API 구간과 같은 키 포함
        putTraceContext(message);
        try {
            // 큐 대기 시간: API의 SQS 전송 시각 → Worker 수신 시각 (Worker 처리 지연·적체 지표)
            long queueWaitMs = System.currentTimeMillis() - message.createdAtEpochMs();
            log.info("예약 메시지 수신 {} {}", kv(LogKeys.EVENT, "worker.received"), kv("queueWaitMs", queueWaitMs));
            // 큐 대기 시간 → booking_queue_wait_seconds (SQS 전송 시간 포함, Worker 처리량 부족·적체 시 증가)
            meterRegistry.timer("booking.queue.wait").record(Math.max(queueWaitMs, 0), TimeUnit.MILLISECONDS);

            // 장애 주입(시나리오 1): 트랜잭션 밖 지연 → 처리 슬롯 점유·처리량 감소, 비활성 시 즉시 반환
            faultInjector.workerDelay();

            long processStart = System.nanoTime();
            BookingResult outcome;
            try {
                outcome = bookingProcessor.process(message);   // CONFIRMED 또는 FAILED
            } catch (DataIntegrityViolationException e) {
                // Unique 최후 방어선 — 동시 요청 중 패배자. 트랜잭션은 이미 롤백됨. (정상적 거절)
                outcome = BookingResult.CONFLICT;
            } catch (RuntimeException e) {
                // 예상 외 실패: 결과 미저장 + 예외 전파 → SQS 재전달 대상
                recordProcess(processStart, "error");   // 재전달 대상 실패도 처리 시간·건수 기록
                log.error("예약 처리 실패 {}", kv(LogKeys.EVENT, "worker.failed"), e);
                throw e;
            }
            // Worker 처리 시간 → booking_worker_process_seconds{result} (DB 트랜잭션 구간, 커넥션 풀 대기 포함)
            long processMs = recordProcess(processStart, outcome.metricLabel());

            // 확정 결과 카운터 → booking_confirm_total{result="confirmed|conflict|failed"} (SLO용)
            meterRegistry.counter("booking.confirm", "result", outcome.metricLabel()).increment();

            // e2e 확정 시간 → booking_confirm_e2e_seconds (큐 인입~확정, 진짜 SLI)
            long elapsedMs = System.currentTimeMillis() - message.createdAtEpochMs();
            meterRegistry.timer("booking.confirm.e2e").record(elapsedMs, TimeUnit.MILLISECONDS);

            // status 응답/폴링용 코드값 저장 (CONFIRMED/CONFLICT/FAILED)
            saveResult(message.requestId(), outcome.name());

            if (outcome == BookingResult.CONFIRMED) {
                // 최종 예매 성공 시점에만 Queue Token을 정리한다.
                // CONFLICT/FAILED는 사용자가 같은 입장 권한으로 다른 좌석을 다시 시도할 수 있도록 유지한다.
                queueService.completeAdmission(message.showId(), message.userId(), message.queueToken());
            }

            // CONFLICT는 정상 거절(WARN), FAILED는 시스템 실패(ERROR), CONFIRMED는 INFO
            Object[] fields = {kv(LogKeys.EVENT, "worker.processed"), kv("result", outcome.name()),
                    kv("queueWaitMs", queueWaitMs), kv("processMs", processMs), kv("e2eMs", elapsedMs)};
            switch (outcome) {
                case CONFIRMED -> log.info("예약 처리 완료 {} {} {} {} {}", fields);
                case CONFLICT -> log.warn("예약 처리 완료 {} {} {} {} {}", fields);
                case FAILED -> log.error("예약 처리 완료 {} {} {} {} {}", fields);
            }
        } finally {
            MDC.clear();
        }
    }

    private long recordProcess(long startNanos, String result) {
        long elapsedNanos = System.nanoTime() - startNanos;
        meterRegistry.timer("booking.worker.process", "result", result).record(elapsedNanos, TimeUnit.NANOSECONDS);
        return elapsedNanos / 1_000_000;
    }

    private void putTraceContext(BookingMessage message) {
        MDC.put(LogKeys.BOOKING_REQUEST_ID, message.requestId());
        if (message.httpRequestId() != null) {
            MDC.put(LogKeys.REQUEST_ID, message.httpRequestId());
        }
        MDC.put(LogKeys.USER_ID, message.userId());
        MDC.put(LogKeys.SHOW_ID, String.valueOf(message.showId()));
        MDC.put(LogKeys.SEAT_ID, String.valueOf(message.seatId()));
    }

    private void saveResult(String requestId, String status) {
        redisTemplate.opsForValue().set("result:" + requestId, status, RESULT_TTL);
    }
}
