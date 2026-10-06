package com.example.ticketing.booking.worker;

import com.example.ticketing.booking.entity.Booking;
import com.example.ticketing.booking.repository.BookingRepository;
import com.example.ticketing.booking.sqs.BookingMessage;
import com.example.ticketing.global.fault.FaultInjector;
import com.example.ticketing.seat.entity.Seat;
import com.example.ticketing.seat.entity.SeatStatus;
import com.example.ticketing.seat.repository.SeatRepository;
import com.example.ticketing.show.repository.SeatGradeRepository;
import com.example.ticketing.user.entity.User;
import com.example.ticketing.user.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import static net.logstash.logback.argument.StructuredArguments.kv;

@Component
@Slf4j
@RequiredArgsConstructor
public class BookingProcessor {
    private final SeatRepository seatRepository;
    private final UserRepository userRepository;
    private final BookingRepository bookingRepository;
    private final SeatGradeRepository seatGradeRepository;
    private final FaultInjector faultInjector;

    /**
     * 예매 확정 처리. 결과를 BookingResult로 반환한다.
     * Unique 제약 위반 시에는 예외를 그대로 전파시켜 트랜잭션을 롤백한다(BookingWorker가 잡아 CONFLICT 처리).
     */
    @Transactional
    public BookingResult process(BookingMessage message) {
        Seat seat = seatRepository.findById(message.seatId()).orElse(null);
        // 장애 주입(시나리오 2): 첫 쿼리로 커넥션 획득 후 지연 → 커넥션 반납 지연·풀 고갈, 비활성 시 즉시 반환
        faultInjector.dbHold();
        if (seat == null) {
            log.error("좌석을 찾을 수 없습니다. - 좌석 : {}", message.seatId());
            return BookingResult.FAILED;
        }

        // 같은 메시지 재전송 멱등 처리 (최초 처리 때 이미 감소했으므로 여기선 감소하지 않음)
        if (seat.getStatus() == SeatStatus.SOLD) {
            // 소유자 확인 없이 CONFIRMED 반환하는 경로 (재전송 멱등 처리 의도, 타 사용자 메시지도 통과 → 정합성 점검 대상)
            log.warn("이미 판매 완료된 좌석 {}", kv("event", "worker.already_sold"));   // seatId는 MDC에 포함
            return BookingResult.CONFIRMED;
        }

        User user = userRepository.findById(message.userId()).orElse(null);
        if (user == null) {
            log.error("사용자를 찾을 수 없습니다. 사용자 : {}", message.userId());
            return BookingResult.FAILED;
        }

        seat.sold();
        // saveAndFlush로 INSERT 즉시 실행 → 제약 위반이 여기서 발생하고 예외가 전파됨.
        // 예외가 메서드 밖으로 나가야 트랜잭션이 정상 롤백된다(seat.sold()도 함께 원복).
        bookingRepository.saveAndFlush(new Booking(seat, user));

        // 잔여석 -1 (확정 성공 시에만, 화면 표시용 카운터 동기화)
        Long showId = seat.getShow().getShowId();
        int price = seat.getPrice();
        seatGradeRepository.findFirstByShowIdAndPrice(showId, price)
                .ifPresent(grade -> {
                    int updated = seatGradeRepository.decreaseRemainingSeats(grade.getId());
                    if (updated == 0) {
                        log.warn("잔여석 카운터가 0이라 감소 실패 (예매는 성공). 좌석: {}, 등급: {}",
                                seat.getId(), grade.getId());
                    }
                });

        log.debug("예매 확정 완료 - 좌석: {}", seat.getId());   // 결과 로그는 BookingWorker의 worker.processed
        return BookingResult.CONFIRMED;
    }
}