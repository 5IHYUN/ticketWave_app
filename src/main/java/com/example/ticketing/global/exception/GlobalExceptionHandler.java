package com.example.ticketing.global.exception;

import com.example.ticketing.global.logging.LogKeys;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import static net.logstash.logback.argument.StructuredArguments.kv;

@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    /**
     * 업무 예외 로그 레벨 기준
     * - 5xx: ERROR (시스템 문제)
     * - 401/403: WARN (인증·Queue Token 실패, 반복 시 이상 징후)
     * - 그 외 4xx(409 좌석 경합·선점 만료, 404 등): INFO (정상 업무 거절, ERROR 알림 노이즈 방지)
     */
    @ExceptionHandler(BusinessException.class)
    protected ResponseEntity<ErrorResponse> handleBusinessException(BusinessException e) {
        ErrorCode errorCode = e.getErrorCode();
        int status = errorCode.getStatus().value();
        Object[] fields = {kv(LogKeys.EVENT, "business.rejected"), kv("errorCode", errorCode.getCode()),
                kv("status", status), kv("reason", e.getMessage())};
        if (status >= 500) {
            log.error("업무 예외 {} {} {} {}", fields);
        } else if (status == 401 || status == 403) {
            log.warn("업무 예외 {} {} {} {}", fields);
        } else {
            log.info("업무 예외 {} {} {} {}", fields);
        }
        ErrorResponse response = new ErrorResponse(errorCode.getCode(), e.getMessage());
        return new ResponseEntity<>(response, errorCode.getStatus());
    }

    @ExceptionHandler(Exception.class)
    protected ResponseEntity<ErrorResponse> handleException(Exception e) {
        log.error("처리되지 않은 예외 {}", kv(LogKeys.EVENT, "unhandled.exception"), e);
        ErrorResponse response = new ErrorResponse(ErrorCode.INTERNAL_SERVER_ERROR.getCode(), e.getMessage());
        return new ResponseEntity<>(response, ErrorCode.INTERNAL_SERVER_ERROR.getStatus());
    }

    public record ErrorResponse(String code, String message) {}
}
