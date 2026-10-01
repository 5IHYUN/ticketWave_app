package com.example.ticketing.queue.service;

import com.example.ticketing.global.exception.BusinessException;
import com.example.ticketing.global.exception.NotFoundException;
import com.example.ticketing.global.exception.ErrorCode;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class QueueService {
    private final StringRedisTemplate redisTemplate;
    private static final String WAITING_QUEUE_KEY_PREFIX = "queue:waiting:";
    private static final String ACTIVE_QUEUE_KEY_PREFIX = "queue:active:";
    // 활성 show 레지스트리 — promote가 KEYS 전체 스캔 대신 이 SET만 읽는다 (5만 대기열 대비)
    private static final String ACTIVE_SHOWS_KEY = "queue:active-shows";
    private static final String USER_TOKEN_KEY_PREFIX = "queue:user-token:";
    private static final String TOKEN_KEY_PREFIX = "queue:token:";
    // 다중 Pod에서 승격 스케줄러가 동시에 돌지 않도록 하는 분산 락 키
    private static final String PROMOTE_LOCK_KEY = "queue:promote-lock";

    // Queue Token / active 입장 유효 시간 (검증 성공 시 sliding 연장)
    @Value("${queue.token-ttl-seconds:300}")
    private long queueTokenTtlSeconds;
    private Duration queueTokenTtl;

    // 동시 입장 상한 (active ZSET 최대 인원)
    @Value("${queue.max-active-users:${queue.active-entry-limit:10000}}")
    private int maxActiveUsers;

    // 승격 주기(promotion-interval-ms)마다 승격할 최대 인원 N
    // 산정: N ≈ 동시 입장 상한 / 평균 체류 시간(초) = 10000 / 300 ≈ 34 (초당)
    // 실제 승격 인원은 PROMOTE_LUA 안에서 min(N, 상한 - 현재 active)로 제한
    @Value("${queue.promotion-batch-size:34}")
    private int promotionBatchSize;

    @Value("${queue.promotion-interval-ms:1000}")
    private long promotionIntervalMs = 1000;

    // ⚠️ 부하테스트(A 방식) 전용: 큐토큰 검증 우회 스위치.
    //    기본 false. dev/loadtest 프로파일에서만 QUEUE_TOKEN_BYPASS=true 로 켤 것.
    //    운영 프로파일에선 절대 true 금지 (대기열 자체가 무력화됨).
    @Value("${queue.token.bypass:false}")
    private boolean queueTokenBypass;

    @PostConstruct
    void initQueueSettings() {
        this.queueTokenTtl = Duration.ofSeconds(queueTokenTtlSeconds);
        if (promotionBatchSize <= 0) {
            // 0 이하면 스케줄 승격이 꺼져, 한 명이라도 대기하면 이후 진입자 전원이 대기열에 고착된다
            log.warn("queue.promotion-batch-size={} : 스케줄 승격 비활성화 상태", promotionBatchSize);
        }
        log.info("Queue settings - maxActiveUsers={}, promotionBatchSize={}/{}ms, tokenTtl={}s",
                maxActiveUsers, promotionBatchSize, promotionIntervalMs, queueTokenTtlSeconds);
        warnIfBypassEnabled();
    }

    private void warnIfBypassEnabled() {
        if (queueTokenBypass) {
            log.warn("==== QUEUE TOKEN BYPASS 활성화됨 (부하테스트 모드). 운영 환경이면 즉시 끌 것! ====");
        }
    }

    public String joinQueue(Long showId, String userId) {
        checkBookingOpenTime(showId);
        double score = Instant.now().toEpochMilli();

        // 같은 사용자가 다시 진입하면 score를 갱신해 대기열 뒤로 보냄
        redisTemplate.opsForZSet().add(waitingQueueKey(showId), userId, score);
        return "JOINED";
    }

    /**
     * enter 진입 판정을 Redis Lua로 원자화한 스크립트.
     * Redis는 단일 스레드라 스크립트 실행 중 다른 명령이 끼어들 수 없으므로,
     * 기존 synchronized(JVM 락)로 보호하던 check-then-act(여유 확인 → ZADD)를
     * 락 없이 정합성 있게 처리한다.
     *
     * KEYS[1] = active ZSET, KEYS[2] = waiting ZSET
     * ARGV[1] = userId, ARGV[2] = now(ms), ARGV[3] = activeExpiresAt(ms), ARGV[4] = maxActiveUsers
     *
     * 반환: { code, rank }
     *   code 0 = 이미 ACTIVE
     *   code 1 = 즉시 입장 가능(여유 있음) — 실제 active ZADD는 issueQueueToken이 수행
     *   code 2 = WAITING (rank = 1-based 순번)
     *
     * active 등록(ZADD)은 ENTER_LUA가 원자적으로 수행(동시성 정합성 핵심).
     * issueQueueToken의 ZADD는 기존 active의 TTL 갱신용(멱등).
     */
    private static final String ENTER_LUA =
            // 만료된 active 정리 (score < now 제거)
            "redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[2]) " +
            // 카운트를 먼저 구해 모든 분기에서 함께 반환 (Java의 별도 ZCARD 왕복 제거)
            "local activeCnt = redis.call('ZCARD', KEYS[1]) " +
            "local waitingCnt = redis.call('ZCARD', KEYS[2]) " +
            // 이미 active면 code 0
            "if redis.call('ZSCORE', KEYS[1], ARGV[1]) then return {0, -1, waitingCnt, activeCnt} end " +
            // 이미 waiting이면 현재 순번
            "local wr = redis.call('ZRANK', KEYS[2], ARGV[1]) " +
            "if wr then return {2, wr + 1, waitingCnt, activeCnt} end " +
            // 대기자 0명 && active 여유 있으면 즉시 입장 (여유 확인+ZADD 원자적)
            "if waitingCnt == 0 and activeCnt < tonumber(ARGV[4]) then " +
            "  redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1]) " +
            "  redis.call('SADD', KEYS[3], ARGV[5]) " +
            "  return {1, -1, waitingCnt, activeCnt + 1} " +
            "end " +
            // 그 외 → waiting 추가 후 순번 반환 + 활성 show 등록
            "redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1]) " +
            "redis.call('SADD', KEYS[3], ARGV[5]) " +
            "local nr = redis.call('ZRANK', KEYS[2], ARGV[1]) " +
            "return {2, nr + 1, waitingCnt + 1, activeCnt}";
    // 매 요청 new DefaultRedisScript 생성은 SHA1 재계산 낭비. 불변·스레드안전이므로 1회만 생성해 재사용.
    private static final DefaultRedisScript<java.util.List> ENTER_SCRIPT =
            new DefaultRedisScript<>(ENTER_LUA, java.util.List.class);

    /**
     * promote 배치를 원자화한 Lua. waiting 상위 N명을 active로 한 번에 승급한다.
     * 기존 while 루프(range+remove+add를 N회 = Redis 3N회 왕복, batch 100이면 300회)를
     * Lua 1회 실행으로 대체 — 5만 대기열에서 promote가 매초 Redis를 대량 점유하던 문제 해소.
     *
     * KEYS[1]=waiting ZSET, KEYS[2]=active ZSET, KEYS[3]=active-shows SET
     * ARGV[1]=batchSize(주기당 최대 승급 인원), ARGV[2]=activeExpiresAt(ms), ARGV[3]=showId,
     * ARGV[4]=now(ms), ARGV[5]=maxActiveUsers
     * 실제 승급 인원 = min(batchSize, maxActiveUsers - 현재 active) — 상한 계산을 Lua 안에서 원자적으로 수행
     * 반환: 승급한 인원 수
     */
    private static final String PROMOTE_LUA =
            // 만료된 active 정리 후 빈 슬롯을 Lua 안에서 계산 (enter와의 경쟁으로 상한 초과 방지)
            "redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[4]) " +
            "local free = tonumber(ARGV[5]) - redis.call('ZCARD', KEYS[2]) " +
            "local slots = math.min(tonumber(ARGV[1]), free) " +
            "if slots <= 0 then return 0 end " +
            // waiting 상위 slots명 조회 (score 오름차순 = 먼저 온 순)
            "local users = redis.call('ZRANGE', KEYS[1], 0, slots - 1) " +
            "local n = #users " +
            "if n == 0 then " +
            // 대기자 없음 + active도 비면 레지스트리에서 제거
            "  if redis.call('ZCARD', KEYS[2]) == 0 then redis.call('SREM', KEYS[3], ARGV[3]) end " +
            "  return 0 " +
            "end " +
            // 조회한 n명을 waiting에서 제거하고 active에 추가
            "for i = 1, n do " +
            "  redis.call('ZREM', KEYS[1], users[i]) " +
            "  redis.call('ZADD', KEYS[2], ARGV[2], users[i]) " +
            "end " +
            // 승급 후 waiting/active 모두 비면 레지스트리 정리
            "if redis.call('ZCARD', KEYS[1]) == 0 and redis.call('ZCARD', KEYS[2]) == 0 then " +
            "  redis.call('SREM', KEYS[3], ARGV[3]) " +
            "end " +
            "return n";
    private static final DefaultRedisScript<Long> PROMOTE_SCRIPT =
            new DefaultRedisScript<>(PROMOTE_LUA, Long.class);

    /**
     * ACTIVE 토큰 발급을 원자화한 Lua. 기존 issueQueueToken의 Redis 4회 왕복
     * (ZADD + GET + SET + SET)을 1회로 축소 — 5000 VU에서 enter가 무거워지던 원인 완화.
     *
     * KEYS[1]=active ZSET, KEYS[2]=userTokenKey, KEYS[3]=tokenKey(신규 후보)
     * ARGV[1]=userId, [2]=activeExpiresAt(ms), [3]=newToken(Java 생성 UUID),
     * [4]=tokenValue(showId:userId), [5]=ttlSeconds
     * 반환: 실제 사용된 토큰 (기존 있으면 기존, 없으면 신규)
     *
     * 주의: 기존 토큰이 있으면 KEYS[3](신규 후보 tokenKey)는 쓰이지 않음.
     *       신규일 때만 tokenKey(newToken)에 저장되므로, Java가 newToken 기준으로 KEYS[3] 전달.
     */
    private static final String TOKEN_LUA =
            "redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1]) " +
            "local existing = redis.call('GET', KEYS[2]) " +
            "if existing then " +
            "  redis.call('EXPIRE', KEYS[2], tonumber(ARGV[5])) " +
            "  redis.call('EXPIRE', 'queue:token:' .. existing, tonumber(ARGV[5])) " +
            "  return existing " +
            "end " +
            "redis.call('SET', KEYS[2], ARGV[3], 'EX', tonumber(ARGV[5])) " +
            "redis.call('SET', KEYS[3], ARGV[4], 'EX', tonumber(ARGV[5])) " +
            "return ARGV[3]";
    private static final DefaultRedisScript<String> TOKEN_SCRIPT =
            new DefaultRedisScript<>(TOKEN_LUA, String.class);

    /**
     * enterQueue: synchronized 제거. 진입 판정은 ENTER_LUA가 원자적으로 수행한다.
     * 파드당 직렬화 병목(JVM 단일 락)이 사라지고, promoteQueues와도 락을 공유하지 않는다.
     */
    public QueueStatusResponse enterQueue(Long showId, String userId) {
        checkBookingOpenTime(showId);

        @SuppressWarnings("unchecked")
        java.util.List<Long> result = redisTemplate.execute(
                ENTER_SCRIPT,
                java.util.List.of(activeQueueKey(showId), waitingQueueKey(showId), ACTIVE_SHOWS_KEY),
                userId,
                String.valueOf(Instant.now().toEpochMilli()),
                String.valueOf(activeExpiresAt()),
                String.valueOf(maxActiveUsers),
                String.valueOf(showId)
        );

        long code = (result != null && !result.isEmpty()) ? result.get(0) : 2L;
        // Lua가 계산한 카운트를 재사용 → 별도 ZCARD 왕복 제거
        long waitingCnt = (result != null && result.size() > 2) ? result.get(2) : 0L;
        long activeCnt  = (result != null && result.size() > 3) ? result.get(3) : 0L;
        QueueCounts counts = new QueueCounts(waitingCnt, activeCnt);

        if (code == 0L || code == 1L) {
            // ACTIVE (기존이거나 신규 승급) → 토큰 발급
            return activeResponse(showId, userId, counts);
        }
        // WAITING
        Long rank = (result != null && result.size() > 1) ? result.get(1) : null;
        return waitingResponse(showId, rank, counts);
    }

    /**
     * getStatus: 읽기 전용(rank 조회)이라 락이 불필요. cleanup만 원자적으로 처리.
     */
    public QueueStatusResponse getStatus(Long showId, String userId) {
        cleanupExpiredActiveUsers(showId);

        // Redis ZSET rank는 0부터 시작하므로 응답에서는 1을 더함
        Long rank = redisTemplate.opsForZSet().rank(waitingQueueKey(showId), userId);
        QueueCounts counts = queueCounts(showId);
        if (rank == null && isActiveUser(showId, userId)) {
            return activeResponse(showId, userId, counts);
        }

        if (rank == null) {
            throw new NotFoundException(ErrorCode.QUEUE_NOT_FOUND);
        }

        return waitingResponse(showId, rank + 1, counts);
    }

    public void leaveQueue(Long showId, String userId) {
        // 사용자를 해당 공연의 대기열 ZSET에서 제거
        redisTemplate.opsForZSet().remove(waitingQueueKey(showId), userId);
        redisTemplate.opsForZSet().remove(activeQueueKey(showId), userId);

        // 발급된 Queue Token 관련 키도 함께 삭제
        String token = redisTemplate.opsForValue().get(userTokenKey(showId, userId));
        if (StringUtils.hasText(token)) {
            redisTemplate.delete(tokenKey(token));
        }
        redisTemplate.delete(userTokenKey(showId, userId));
    }

    public void completeAdmission(Long showId, String userId, String queueToken) {
        if (queueTokenBypass || !StringUtils.hasText(queueToken)) {
            return;
        }

        String userTokenKey = userTokenKey(showId, userId);
        String currentToken = redisTemplate.opsForValue().get(userTokenKey);
        if (!queueToken.equals(currentToken)) {
            log.warn("예매 확정 후 Queue Token 정리 건너뜀 - 현재 토큰과 메시지 토큰 불일치. showId={}, userId={}", showId, userId);
            return;
        }

        // 예매 확정(CONFIRMED) 이후에는 같은 입장 권한으로 추가 예매를 시도할 수 없도록 ACTIVE와 토큰 매핑을 정리
        redisTemplate.opsForZSet().remove(activeQueueKey(showId), userId);
        redisTemplate.delete(tokenKey(queueToken));
        redisTemplate.delete(userTokenKey);
    }

    public void validateQueueToken(String token, Long showId, String userId) {
        // ⚠️ 부하테스트(A) 모드: 큐토큰 검증을 통째로 건너뜀.
        //    인터셉터/서비스 양쪽이 이 메서드를 타므로 여기 한 곳이면 둘 다 우회됨.
        if (queueTokenBypass) {
            return;
        }

        if (!StringUtils.hasText(token)) {
            throw new BusinessException(ErrorCode.QUEUE_TOKEN_REQUIRED);
        }

        cleanupExpiredActiveUsers(showId);

        String savedTokenValue = redisTemplate.opsForValue().get(tokenKey(token));
        if (savedTokenValue == null) {
            throw new BusinessException(ErrorCode.QUEUE_TOKEN_EXPIRED);
        }

        String userToken = redisTemplate.opsForValue().get(userTokenKey(showId, userId));
        boolean tokenMismatch = !token.equals(userToken);
        boolean ownerMismatch = !tokenValue(showId, userId).equals(savedTokenValue);
        boolean inactiveUser = !isActiveUser(showId, userId);
        if (tokenMismatch || ownerMismatch || inactiveUser) {
            throw new BusinessException(ErrorCode.INVALID_QUEUE_TOKEN);
        }

        // 토큰 검증 성공 시, 활성 상태 유지를 위해 만료 시간 연장 (Sliding Window)
        redisTemplate.opsForZSet().add(activeQueueKey(showId), userId, activeExpiresAt());
        redisTemplate.expire(userTokenKey(showId, userId), queueTokenTtl);
        redisTemplate.expire(tokenKey(token), queueTokenTtl);
    }

    // promote 전용 락. enterQueue/getStatus와 'this' 락을 공유하지 않아
    // 1초 주기 promote가 enter 진입을 막던 톱니 간섭을 제거한다.
    // (promote끼리의 중복 실행만 막으면 충분 — enter는 ENTER_LUA가 원자성 보장)
    private final Object promoteLock = new Object();

    @Scheduled(fixedRateString = "${queue.promotion-interval-ms:1000}")
    public void promoteQueues() {
        // 다중 Pod 분산 락: 승격 주기당 한 Pod만 승격을 수행한다.
        // 락 없이 Pod마다 돌면 실제 승격 속도가 N × Pod 수가 되어 batch-size가 고정값 의미를 잃는다.
        // 락은 해제하지 않고 TTL(주기의 90%)로 자연 만료시켜, 같은 주기 안의 다른 Pod 실행을 건너뛰게 한다.
        Boolean acquired = redisTemplate.opsForValue().setIfAbsent(
                PROMOTE_LOCK_KEY, "1", Duration.ofMillis(Math.max(100, promotionIntervalMs * 9 / 10)));
        if (!Boolean.TRUE.equals(acquired)) {
            return;
        }
        synchronized (promoteLock) {
            for (Long showId : findQueueShowIds()) {
                cleanupExpiredActiveUsers(showId);
                promoteWaitingUsers(showId);
            }
        }
    }

    private String waitingQueueKey(Long showId) {
        // 공연 단위로 대기열을 분리
        return WAITING_QUEUE_KEY_PREFIX + showId;
    }

    private boolean isActiveUser(Long showId, String userId) {
        return redisTemplate.opsForZSet().score(activeQueueKey(showId), userId) != null;
    }

    private boolean canEnterImmediately(Long showId) {
        return zSetSize(waitingQueueKey(showId)) == 0 && zSetSize(activeQueueKey(showId)) < maxActiveUsers;
    }

    private void cleanupExpiredActiveUsers(Long showId) {
        redisTemplate.opsForZSet().removeRangeByScore(
                activeQueueKey(showId),
                Double.NEGATIVE_INFINITY,
                Instant.now().toEpochMilli()
        );
    }

    private void promoteWaitingUsers(Long showId) {
        if (maxActiveUsers <= 0 || promotionBatchSize <= 0) {
            return;
        }

        // 빈 슬롯 계산과 승급을 PROMOTE_LUA 한 번에 원자 처리 (Java에서 ZCARD 후 승급하면 enter와 경쟁해 상한 초과 가능)
        redisTemplate.execute(
                PROMOTE_SCRIPT,
                java.util.List.of(waitingQueueKey(showId), activeQueueKey(showId), ACTIVE_SHOWS_KEY),
                String.valueOf(promotionBatchSize),
                String.valueOf(activeExpiresAt()),
                String.valueOf(showId),
                String.valueOf(Instant.now().toEpochMilli()),
                String.valueOf(maxActiveUsers)
        );
    }

    private Set<Long> findQueueShowIds() {
        // KEYS 전체 스캔(O(N), Redis 블로킹) 제거 → active-shows SET만 읽음.
        // 5만 대기열 + 유저별 토큰키(10만+)에서 KEYS는 매 promote마다 전체 키스페이스를
        // 스캔해 Redis를 멈추게 하므로, 활성 show만 담은 작은 SET을 단일 출처로 사용한다.
        Set<String> members = redisTemplate.opsForSet().members(ACTIVE_SHOWS_KEY);
        if (members == null || members.isEmpty()) {
            return java.util.Collections.emptySet();
        }
        Set<Long> showIds = new HashSet<>();
        for (String m : members) {
            try {
                showIds.add(Long.parseLong(m));
            } catch (NumberFormatException ignored) {
            }
        }
        return showIds;
    }

    private long zSetSize(String key) {
        Long size = redisTemplate.opsForZSet().zCard(key);
        return size == null ? 0 : size;
    }

    private QueueStatusResponse activeResponse(Long showId, String userId, QueueCounts counts) {
        // counts는 ENTER_LUA가 이미 계산한 값을 재사용 (별도 ZCARD 왕복 없음)
        return new QueueStatusResponse("ACTIVE", null, true, issueQueueToken(showId, userId),
                counts.waitingCount(), counts.activeCount(), counts.totalCount(), null);
    }

    private QueueStatusResponse waitingResponse(Long showId, Long position, QueueCounts counts) {
        // counts는 ENTER_LUA가 이미 계산한 값을 재사용 (별도 ZCARD 왕복 없음)
        return new QueueStatusResponse("WAITING", position, false, null,
                counts.waitingCount(), counts.activeCount(), counts.totalCount(), estimateWaitSeconds(position));
    }

    private QueueCounts queueCounts(Long showId) {
        long waitingCount = zSetSize(waitingQueueKey(showId));
        long activeCount = zSetSize(activeQueueKey(showId));
        return new QueueCounts(waitingCount, activeCount);
    }

    private Long estimateWaitSeconds(Long position) {
        if (position == null || promotionBatchSize <= 0 || promotionIntervalMs <= 0) {
            return null;
        }

        double admissionPerSecond = promotionBatchSize * (1000.0 / promotionIntervalMs);
        return Math.max(1, (long) Math.ceil(position / admissionPerSecond));
    }

    private double activeExpiresAt() {
        return Instant.now().plus(queueTokenTtl).toEpochMilli();
    }

    private String issueQueueToken(Long showId, String userId) {
        // Redis 4회 왕복(ZADD+GET+SET+SET)을 TOKEN_LUA 1회로 축소.
        // UUID는 Lua에서 못 만들므로 Java가 신규 후보를 생성해 넘기고,
        // Lua가 기존 토큰 있으면 그것을, 없으면 넘긴 신규를 저장·반환한다.
        String newToken = UUID.randomUUID().toString();
        String used = redisTemplate.execute(
                TOKEN_SCRIPT,
                java.util.List.of(activeQueueKey(showId), userTokenKey(showId, userId), tokenKey(newToken)),
                userId,
                String.valueOf(activeExpiresAt()),
                newToken,
                tokenValue(showId, userId),
                String.valueOf(queueTokenTtl.getSeconds())
        );
        return used;
    }

    private String activeQueueKey(Long showId) {
        return ACTIVE_QUEUE_KEY_PREFIX + showId;
    }

    private String userTokenKey(Long showId, String userId) {
        return USER_TOKEN_KEY_PREFIX + showId + ":" + userId;
    }

    private String tokenKey(String token) {
        return TOKEN_KEY_PREFIX + token;
    }

    public void clearQueue(Long showId) {
        log.info("Queue clearing triggered for showId: {}", showId);
        String waitingKey = waitingQueueKey(showId);
        String activeKey = activeQueueKey(showId);

        String userTokenPattern = USER_TOKEN_KEY_PREFIX + showId + ":*";
        Set<String> userTokenKeys = redisTemplate.keys(userTokenPattern);
        if (userTokenKeys != null && !userTokenKeys.isEmpty()) {
            Set<String> tokenKeysToDelete = new HashSet<>();
            for (String userTokKey : userTokenKeys) {
                String token = redisTemplate.opsForValue().get(userTokKey);
                if (StringUtils.hasText(token)) {
                    tokenKeysToDelete.add(tokenKey(token));
                }
            }
            if (!tokenKeysToDelete.isEmpty()) {
                redisTemplate.delete(tokenKeysToDelete);
            }
            redisTemplate.delete(userTokenKeys);
        }

        redisTemplate.delete(waitingKey);
        redisTemplate.delete(activeKey);
        log.info("Queue cleared for showId: {}", showId);
    }

    public void clearAllQueues() {
        log.info("Queue clearing triggered for all shows");
        Set<String> keys = redisTemplate.keys("queue:*");
        if (keys != null && !keys.isEmpty()) {
            redisTemplate.delete(keys);
        }
        log.info("All queues cleared");
    }

    private String tokenValue(Long showId, String userId) {
        return showId + ":" + userId;
    }

    private void checkBookingOpenTime(Long showId) {
        String key = "show:" + showId + ":booking_open_at";
        String val = redisTemplate.opsForValue().get(key);
        if (StringUtils.hasText(val)) {
            try {
                long openTime = Long.parseLong(val);
                long now = Instant.now().getEpochSecond();
                if (now < openTime) {
                    throw new BusinessException(ErrorCode.BOOKING_NOT_OPEN);
                }
            } catch (NumberFormatException e) {
                // Ignore parse errors
            }
        }
    }

    @Schema(description = "대기열 상태 응답")
    public record QueueStatusResponse(
            @Schema(description = "대기열 상태", example = "WAITING", allowableValues = {"WAITING", "ACTIVE"})
            String status,
            @Schema(description = "대기 순번. ACTIVE 상태에서는 null입니다.", example = "37")
            Long position,
            @Schema(description = "예매 화면 입장 가능 여부", example = "false")
            boolean canEnter,
            @Schema(description = "예매 요청에 사용할 Queue Token. WAITING 상태에서는 null입니다.", example = "550e8400-e29b-41d4-a716-446655440000")
            String queueToken,
            @Schema(description = "현재 대기 중인 사용자 수", example = "1200")
            long waitingCount,
            @Schema(description = "현재 입장 가능 상태인 사용자 수", example = "300")
            long activeCount,
            @Schema(description = "현재 대기열 전체 인원 수", example = "1500")
            long totalQueueCount,
            @Schema(description = "예상 대기 시간(초). ACTIVE 상태에서는 null입니다.", example = "180")
            Long estimatedWaitSeconds
    ) {}

    private record QueueCounts(long waitingCount, long activeCount) {
        long totalCount() {
            return waitingCount + activeCount;
        }
    }
}
