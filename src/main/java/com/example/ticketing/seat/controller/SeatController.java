package com.example.ticketing.seat.controller;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.CookieValue;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.DeleteMapping;

import com.example.ticketing.global.exception.ConflictException;
import com.example.ticketing.seat.dto.SeatResponseDto;
import com.example.ticketing.seat.service.SeatService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.example.ticketing.global.logging.LogKeys;

import static net.logstash.logback.argument.StructuredArguments.kv;

@Slf4j
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
@Tag(name = "Seats", description = "좌석 조회, 선점, 선점 해제 API")
public class SeatController {

    private final SeatService seatService;

    @Operation(summary = "공연 좌석 목록 조회", description = "공연 ID에 해당하는 전체 좌석과 현재 상태를 조회합니다.")
    @ApiResponse(responseCode = "200", description = "좌석 목록 조회 성공",
            content = @Content(schema = @Schema(implementation = SeatResponseDto.class),
                    examples = @ExampleObject(value = """
                            [
                              {
                                "seatId": 101,
                                "showId": 1,
                                "seatNumber": "A-1",
                                "price": 150000,
                                "status": "AVAILABLE"
                              }
                            ]
                            """)))
    @GetMapping("/performances/{id}/seats")
    public ResponseEntity<List<SeatResponseDto>> getSeats(
            @Parameter(description = "공연 ID", example = "1")
            @PathVariable("id") Long id,
            @AuthenticationPrincipal String userId
    ) {
        return ResponseEntity.ok(seatService.getSeats(id, userId));
    }

    @Operation(summary = "좌석 선점", description = "로그인 사용자가 좌석을 임시 선점합니다. 선점 정보는 제한 시간 동안 유지됩니다.",
            security = @SecurityRequirement(name = "bearerAuth"))
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "좌석 선점 성공",
                    content = @Content(examples = @ExampleObject(value = """
                            {
                              "seatId": 101,
                              "ttlSeconds": 300
                            }
                            """))),
            @ApiResponse(responseCode = "401", description = "인증 필요", content = @Content),
            @ApiResponse(responseCode = "409", description = "이미 선점되었거나 판매된 좌석", content = @Content)
    })
    @PostMapping("/seats/{seatId}/hold")
    public ResponseEntity<?> holdSeat(
            @Parameter(description = "좌석 ID", example = "101")
            @PathVariable Long seatId,
            @AuthenticationPrincipal String userId,
            @Parameter(description = "대기열 입장 후 발급된 Queue Token", example = "550e8400-e29b-41d4-a716-446655440000")
            @RequestHeader(value = "X-Queue-Token", required = false) String queueTokenHeader,
            @CookieValue(value = "queueToken", required = false) String queueTokenCookie
    ) {
        String queueToken = resolveQueueToken(queueTokenHeader, queueTokenCookie);
        var response = seatService.holdSeat(seatId, userId, queueToken);
        return ResponseEntity.ok(response);
    }

    private String resolveQueueToken(String queueTokenHeader, String queueTokenCookie) {
        return StringUtils.hasText(queueTokenHeader) ? queueTokenHeader : queueTokenCookie;
    }
    
    @Operation(summary = "좌석 선점 해제", description = "현재 사용자가 선점한 좌석을 해제합니다.",
            security = @SecurityRequirement(name = "bearerAuth"))
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "좌석 선점 해제 성공"),
            @ApiResponse(responseCode = "401", description = "인증 필요", content = @Content),
            @ApiResponse(responseCode = "409", description = "선점 해제 불가", content = @Content)
    })
    @DeleteMapping("/seats/{seatId}/hold")
    public ResponseEntity<?> releaseSeat(
            @Parameter(description = "좌석 ID", example = "101")
            @PathVariable Long seatId,
            @AuthenticationPrincipal String userId
    ) {
        seatService.releaseSeat(seatId, userId);
        return ResponseEntity.ok().build();
    }

    @ExceptionHandler(ConflictException.class)
    public ResponseEntity<String> handleConflictException(ConflictException e) {
        // 좌석 경합(이미 선점·판매): 정상 업무 거절 → INFO
        log.info("좌석 선점 실패 {} {} {}", kv(LogKeys.EVENT, "seat.hold_rejected"),
                kv("errorCode", e.getErrorCode().getCode()), kv("reason", e.getMessage()));
        return ResponseEntity.status(HttpStatus.CONFLICT).body("ERR_SEAT_ALREADY_TAKEN");
    }
}
