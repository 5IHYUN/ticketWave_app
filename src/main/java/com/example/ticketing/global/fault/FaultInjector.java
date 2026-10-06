package com.example.ticketing.global.fault;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import static net.logstash.logback.argument.StructuredArguments.kv;

/**
 * 장애 시나리오 검증용 지연 주입 (dev·로컬 전용, 기본 비활성).
 *
 * 켜고 끄기: Redis 키 값(ms)으로 런타임 제어 → Pod 재시작 없이 적용
 *   - 재시작 시 콜드 스타트 지연이 장애 효과에 섞임 (analysis/09) → 환경변수 방식 미사용
 *   - 모든 Worker Pod가 2초 주기로 같은 키 조회 → 확장으로 새로 뜬 Pod도 동일 적용
 *
 * 키
 *   fault:worker-delay-ms  시나리오 1 Worker 처리 지연. 메시지 수신 후 DB 트랜잭션 밖에서 대기
 *                          → 처리 슬롯 점유·처리량 감소 → 큐 적체·큐 대기 증가 (Worker 처리 시간 지표는 불변)
 *   fault:db-hold-ms       시나리오 2 DB 풀 고갈. 트랜잭션 안(커넥션 획득 후)에서 대기
 *                          → 커넥션 반납 지연 → pending 증가·획득 타임아웃 (Worker 처리 시간 지표 상승)
 *
 * 증거 기록
 *   - 값 변경 시 WARN 로그 event=fault.changed (주입 시각·값, 포스트모템 근거)
 *   - 게이지 fault_injected_ms{type} (대시보드에서 주입 구간 표시)
 *
 * 활성화: app.fault.enabled=true (FAULT_INJECTION_ENABLED). 비활성 시 Redis 조회·지연 모두 생략
 */
@Component
@Slf4j
public class FaultInjector {

    public static final String WORKER_DELAY_KEY = "fault:worker-delay-ms";
    public static final String DB_HOLD_KEY = "fault:db-hold-ms";
    private static final long MAX_DELAY_MS = 30_000;   // 오입력(단위 착오 등) 방지 상한

    private final StringRedisTemplate redisTemplate;
    private final boolean enabled;
    private volatile long workerDelayMs = 0;
    private volatile long dbHoldMs = 0;

    public FaultInjector(StringRedisTemplate redisTemplate,
                         MeterRegistry meterRegistry,
                         @Value("${app.fault.enabled:false}") boolean enabled) {
        this.redisTemplate = redisTemplate;
        this.enabled = enabled;
        Gauge.builder("fault.injected.ms", this, f -> f.workerDelayMs)
                .tag("type", "worker_delay")
                .description("주입 중인 Worker 처리 지연 (ms, 0=미주입)")
                .register(meterRegistry);
        Gauge.builder("fault.injected.ms", this, f -> f.dbHoldMs)
                .tag("type", "db_hold")
                .description("주입 중인 DB 커넥션 점유 시간 (ms, 0=미주입)")
                .register(meterRegistry);
        if (enabled) {
            log.warn("장애 주입 기능 활성화 {} {}", kv("event", "fault.enabled"),
                    kv("keys", WORKER_DELAY_KEY + "," + DB_HOLD_KEY));
        }
    }

    @Scheduled(fixedDelay = 2000)
    public void refresh() {
        if (!enabled) {
            return;
        }
        try {
            workerDelayMs = update("worker_delay", workerDelayMs, redisTemplate.opsForValue().get(WORKER_DELAY_KEY));
            dbHoldMs = update("db_hold", dbHoldMs, redisTemplate.opsForValue().get(DB_HOLD_KEY));
        } catch (RuntimeException e) {
            // Redis 조회 실패 시 직전 값 유지 (장애 실험 중 Redis 문제로 주입이 임의 해제되는 것 방지)
            log.warn("장애 주입 설정 조회 실패 {}", kv("event", "fault.refresh_failed"), e);
        }
    }

    /** 시나리오 1: DB 트랜잭션 밖 처리 지연 (BookingWorker) */
    public void workerDelay() {
        sleep(workerDelayMs);
    }

    /** 시나리오 2: 커넥션 점유 상태 지연 (BookingProcessor 트랜잭션 안) */
    public void dbHold() {
        sleep(dbHoldMs);
    }

    private long update(String type, long current, String raw) {
        long next = parse(raw);
        if (next != current) {
            log.warn("장애 주입 값 변경 {} {} {} {}", kv("event", "fault.changed"), kv("faultType", type),
                    kv("fromMs", current), kv("toMs", next));
        }
        return next;
    }

    private static long parse(String raw) {
        if (raw == null || raw.isBlank()) {
            return 0;
        }
        try {
            return Math.min(Math.max(Long.parseLong(raw.trim()), 0), MAX_DELAY_MS);
        } catch (NumberFormatException e) {
            return 0;   // 잘못된 값 = 미주입
        }
    }

    private static void sleep(long ms) {
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
