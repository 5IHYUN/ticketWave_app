package com.example.ticketing.seat.service;

import com.example.ticketing.seat.entity.Seat;
import com.example.ticketing.seat.entity.SeatStatus;
import com.example.ticketing.seat.repository.SeatRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.KeyExpirationEventMessageListener;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@Slf4j
public class RedisKeyExpirationListener extends KeyExpirationEventMessageListener {

    private final SeatRepository seatRepository;
    private final StringRedisTemplate redisTemplate;

    public RedisKeyExpirationListener(RedisMessageListenerContainer listenerContainer,
                                      SeatRepository seatRepository,
                                      StringRedisTemplate redisTemplate) {
        super(listenerContainer);
        setKeyspaceNotificationsConfigParameter(null);
        this.seatRepository = seatRepository;
        this.redisTemplate = redisTemplate;
        log.info("RedisKeyExpirationListener 빈이 성공적으로 생성 및 스프링 컨테이너에 등록되었습니다.");
    }

    @Override
    @Transactional
    public void onMessage(Message message, byte[] pattern) {
        String expiredKey = message.toString();

        // 만료된 키가 좌석 선점 키("seat:{seatId}")인지 확인
        // 그 외 키(result:*, queue:promote-lock 등)는 정상 TTL 만료 → 로그 미기록 (초당 수십 건, 로그 노이즈)
        if (expiredKey.startsWith("seat:")) {
            log.info("좌석 선점 만료 감지: {}", expiredKey);
            try {
                String seatIdStr = expiredKey.substring("seat:".length());
                Long seatId = Long.parseLong(seatIdStr);

                // [가드 로직] 이미 결제 확정(sold:{seatId}) 상태라면, 만료 이벤트로 인해 좌석이 재오픈되지 않도록 무시
                String soldKey = "sold:" + seatId;
                Boolean isSold = redisTemplate.hasKey(soldKey);
                if (Boolean.TRUE.equals(isSold)) {
                    log.info("좌석 {}은 이미 결제 확정(SOLD) 상태이므로 선점 해제 처리를 스킵합니다.", seatId);
                    return;
                }

                // [상태 복구] 데이터베이스의 좌석 상태가 HOLD인 경우 AVAILABLE로 원복
                Seat seat = seatRepository.findById(seatId).orElse(null);
                if (seat != null) {
                    if (seat.getStatus() == SeatStatus.HOLD) {
                        seat.available();
                        seatRepository.save(seat);
                        log.info("좌석 {}의 상태가 시간 초과로 인해 AVAILABLE로 복구되었습니다.", seatId);
                    } else {
                        log.info("좌석 {}의 DB 상태가 HOLD가 아니므로 복구를 건너뜁니다. (현재 상태: {})", seatId, seat.getStatus());
                    }
                }
            } catch (Exception e) {
                log.error("만료된 좌석 선점 해제 처리 중 에러 발생: ", e);
            }
        }
    }
}
