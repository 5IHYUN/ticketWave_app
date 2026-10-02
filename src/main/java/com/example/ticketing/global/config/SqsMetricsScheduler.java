package com.example.ticketing.global.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * SQS 큐 적체량을 2초마다 조회해 앱 메트릭으로 노출하는 스케줄러 (worker 프로필).
 *
 * 노출 메트릭: booking_sqs_messages{queue="main|dlq", queue_name="...", state="visible|in_flight"}
 *   - visible   : 처리 대기 중 메시지 수 (적체)
 *   - in_flight : Worker가 수신해 처리 중인 메시지 수 (visibility timeout 내)
 *   - 조회 실패 시 NaN (0으로 남기면 "적체 없음"으로 오인)
 *
 * 메트릭 이름 변경 (기존 aws_sqs_approximate_number_of_messages_visible_average)
 *   - 기존 이름은 YACE(CloudWatch) 메트릭과 동일 → 같은 쿼리에 두 출처가 섞임
 *   - Worker Pod마다 같은 큐 값을 노출 → sum() 시 Pod 수만큼 배로 집계 (KEDA 과대 스케일 위험)
 *   - 앱 메트릭은 실시간(2초), YACE는 CloudWatch 경유(1분 단위·수 분 지연) → 이름으로 출처 구분
 *   - 집계 시 sum 대신 max 사용 (Pod별 값이 같은 큐의 같은 값)
 */
@Component
@Profile("worker")
@Slf4j
public class SqsMetricsScheduler {

    private final SqsAsyncClient sqsAsyncClient;
    private final List<QueueTarget> targets = new ArrayList<>();

    public SqsMetricsScheduler(
            SqsAsyncClient sqsAsyncClient,
            MeterRegistry meterRegistry,
            @Value("${app.sqs.booking-queue:booking-queue}") String bookingQueue,
            @Value("${app.sqs.booking-dlq:}") String bookingDlq) {
        this.sqsAsyncClient = sqsAsyncClient;
        targets.add(new QueueTarget("main", bookingQueue, meterRegistry));
        // DLQ 미설정 환경은 조회 생략 (설정값 없는 상태로 실패 로그 반복 방지)
        if (bookingDlq != null && !bookingDlq.isBlank()) {
            targets.add(new QueueTarget("dlq", bookingDlq, meterRegistry));
        }
    }

    @Scheduled(fixedRate = 2000)
    public void pollQueueDepth() {
        targets.forEach(this::poll);
    }

    private void poll(QueueTarget target) {
        try {
            resolveQueueUrl(target)
                    .thenCompose(queueUrl -> sqsAsyncClient.getQueueAttributes(GetQueueAttributesRequest.builder()
                            .queueUrl(queueUrl)
                            .attributeNames(
                                    QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                                    QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)
                            .build()))
                    .thenAccept(response -> target.update(response.attributes()))
                    .exceptionally(ex -> {
                        target.fail(ex);
                        return null;
                    });
        } catch (Exception e) {
            target.fail(e);
        }
    }

    private CompletableFuture<String> resolveQueueUrl(QueueTarget target) {
        if (target.queueUrl != null) {
            return CompletableFuture.completedFuture(target.queueUrl);
        }
        if (target.urlOrName.startsWith("http://") || target.urlOrName.startsWith("https://")) {
            target.queueUrl = target.urlOrName;
            return CompletableFuture.completedFuture(target.queueUrl);
        }
        // 이름만 주어진 경우 URL 1회 조회 후 캐시 (2초마다 GetQueueUrl 호출 방지)
        return sqsAsyncClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(target.urlOrName).build())
                .thenApply(response -> {
                    target.queueUrl = response.queueUrl();
                    return target.queueUrl;
                });
    }

    /** 큐 1개의 조회 대상·게이지 값 보관 */
    private static final class QueueTarget {
        private final String role;
        private final String urlOrName;
        private volatile String queueUrl;
        private volatile double visible = Double.NaN;
        private volatile double inFlight = Double.NaN;
        private volatile boolean failing = false;

        QueueTarget(String role, String urlOrName, MeterRegistry registry) {
            this.role = role;
            this.urlOrName = urlOrName;
            String queueName = urlOrName.substring(urlOrName.lastIndexOf('/') + 1);   // URL이면 마지막 segment
            Gauge.builder("booking.sqs.messages", this, t -> t.visible)
                    .tags("queue", role, "queue_name", queueName, "state", "visible")
                    .description("SQS 처리 대기 메시지 수 (앱 실시간 조회)")
                    .register(registry);
            Gauge.builder("booking.sqs.messages", this, t -> t.inFlight)
                    .tags("queue", role, "queue_name", queueName, "state", "in_flight")
                    .description("SQS 처리 중 메시지 수 (앱 실시간 조회)")
                    .register(registry);
        }

        void update(Map<QueueAttributeName, String> attributes) {
            visible = parse(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES));
            inFlight = parse(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE));
            if (failing) {
                failing = false;
                log.info("SQS 적체량 조회 복구 queue={}", role);
            }
        }

        void fail(Throwable ex) {
            visible = Double.NaN;
            inFlight = Double.NaN;
            queueUrl = null;   // 큐 재생성(LocalStack 재기동 등) 대비 URL 재조회
            // 실패 시작 시점에만 WARN (2초마다 반복 로그 방지)
            if (!failing) {
                failing = true;
                log.warn("SQS 적체량 조회 실패 queue={} cause={}", role, ex.getMessage());
            }
        }

        private static double parse(String value) {
            return value == null ? Double.NaN : Double.parseDouble(value);
        }
    }
}
