package com.nodev.notification.bl.scheduler;

import com.nodev.notification.bl.entity.DeliveryAttempt;
import com.nodev.notification.bl.repository.DeliveryAttemptRepository;
import com.nodev.notification.bl.service.RetryExecutor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Sibling to OutboxPoller, not a modification of it - same background-job
 * shape (scan a table for due work, act on it), different table, and
 * calls DispatchService directly instead of publishing to Kafka. A retry
 * is purely internal to this service; the event already made its one trip
 * through Kafka via the original attempt, so there's nothing left for
 * Kafka to decouple here.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class RetryScheduler {

    private static final int BATCH_SIZE = 50;

    private final DeliveryAttemptRepository deliveryAttemptRepository;
    private final RetryExecutor retryExecutor;

    @Scheduled(fixedDelay = 10000)
    @SchedulerLock(name = "retryDueAttempts", lockAtMostFor = "2m", lockAtLeastFor = "9s")
    public void retryDueAttempts() {
        Page<DeliveryAttempt> due = deliveryAttemptRepository.findRetryDueAttempts(
                Instant.now(), PageRequest.of(0, BATCH_SIZE));

        if (due.isEmpty()) {
            return;
        }

        log.info("Retry scheduler picked up {} due attempt(s)", due.getNumberOfElements());

        for (DeliveryAttempt failedAttempt : due) {
            retryExecutor.retryOne(failedAttempt);

        }

    }
}