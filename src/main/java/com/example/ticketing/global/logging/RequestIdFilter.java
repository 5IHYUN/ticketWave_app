package com.example.ticketing.global.logging;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;
import java.util.regex.Pattern;

import static net.logstash.logback.argument.StructuredArguments.kv;

/**
 * HTTP 요청 단위 추적 키 부여
 * - X-Request-Id 헤더가 있으면 사용(형식 검증), 없으면 생성 → MDC, 응답 헤더에 설정
 * - 요청 종료 시 MDC 정리 (스레드 재사용으로 인한 키 오염 방지)
 * - 느린 요청(기준 이상)과 5xx 응답만 INFO/ERROR로 기록, 나머지는 DEBUG (정상 트래픽 로그량 억제)
 */
@Slf4j
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RequestIdFilter extends OncePerRequestFilter {

    public static final String HEADER = "X-Request-Id";
    private static final Pattern VALID_ID = Pattern.compile("^[A-Za-z0-9-]{8,64}$");

    @Value("${app.logging.slow-request-ms:300}")
    private long slowRequestMs;

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String requestId = request.getHeader(HEADER);
        if (requestId == null || !VALID_ID.matcher(requestId).matches()) {
            requestId = UUID.randomUUID().toString();
        }
        MDC.put(LogKeys.REQUEST_ID, requestId);
        response.setHeader(HEADER, requestId);

        long start = System.nanoTime();
        try {
            chain.doFilter(request, response);
        } finally {
            long durationMs = (System.nanoTime() - start) / 1_000_000;
            int status = response.getStatus();
            Object[] fields = {kv(LogKeys.EVENT, "http.request"), kv("method", request.getMethod()),
                    kv("path", request.getRequestURI()), kv("status", status), kv("durationMs", durationMs)};
            if (status >= 500) {
                log.error("HTTP 요청 {} {} {} {} {}", fields);
            } else if (durationMs >= slowRequestMs) {
                log.info("HTTP 요청 {} {} {} {} {}", fields);
            } else if (log.isDebugEnabled()) {
                log.debug("HTTP 요청 {} {} {} {} {}", fields);
            }
            MDC.clear();
        }
    }

    // actuator(prometheus scrape)는 추적 대상 제외
    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return request.getRequestURI().startsWith("/actuator");
    }
}
