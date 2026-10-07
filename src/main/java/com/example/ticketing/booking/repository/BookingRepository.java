package com.example.ticketing.booking.repository;

import com.example.ticketing.booking.entity.Booking;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import java.util.List;
import java.util.Optional;

public interface BookingRepository extends JpaRepository<Booking, String> {

    @Query("SELECT b FROM Booking b JOIN FETCH b.seat s JOIN FETCH s.show WHERE b.user.userId = :userId ORDER BY b.bookedAt DESC")
    List<Booking> findByUserIdWithSeatAndShow(@Param("userId") String userId);

    /**
     * 예매 취소용 조회 (SELECT ... FOR UPDATE).
     * 같은 예매에 대한 동시 취소를 한 줄로 세움: 두 번째 요청은 첫 번째 커밋까지 대기한 뒤
     * 이미 삭제된 행을 읽게 되어 빈 결과 → 404로 응답됨 (기존: 커밋 시 DELETE 0건 → 500).
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT b FROM Booking b WHERE b.id = :bookingId")
    Optional<Booking> findByIdForUpdate(@Param("bookingId") String bookingId);
}
