package com.nodev.notification.bl.scheduler;

import com.nodev.notification.bl.RetryPolicy;
import com.nodev.notification.bl.entity.DeliveryAttempt;
import com.nodev.notification.bl.entity.NotificationRequest;
import com.nodev.notification.bl.entity.enums.Channel;
import com.nodev.notification.bl.repository.DeliveryAttemptRepository;
import com.nodev.notification.bl.service.DispatchService;
import com.nodev.notification.bl.service.RetryOutcomeRecorder;
import com.nodev.notification.bl.service.SubscriberResolver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
    private final SubscriberResolver subscriberResolver;
    private final RetryPolicy retryPolicy;
    private final DispatchService dispatchService;
    private final RetryOutcomeRecorder retryOutcomeRecorder;

    @Scheduled(fixedDelay = 10000)
    public void retryDueAttempts() {
        Page<DeliveryAttempt> due = deliveryAttemptRepository.findRetryDueAttempts(
                Instant.now(), PageRequest.of(0, BATCH_SIZE));

        if (due.isEmpty()) {
            return;
        }

        log.info("Retry scheduler picked up {} due attempt(s)", due.getNumberOfElements());

        for (DeliveryAttempt failedAttempt : due) {
            retryOne(failedAttempt);
        }
    }

    private void retryOne(DeliveryAttempt failedAttempt) {
        NotificationRequest notification = failedAttempt.getNotification();
        Channel channel = failedAttempt.getChannel();
        int newAttemptNumber = failedAttempt.getAttemptNumber() + 1;

        // Resolved fresh, right now - not reused from the original attempt,
        // in case contact info changed since then.
        String resolvedAddress = subscriberResolver.resolve(notification.getRecipientId(), channel);

        // Dispatch happens OUTSIDE any transaction - same reasoning as the
        // outbox poller: never hold a DB transaction open across external I/O.
        boolean success = resolvedAddress != null
                && dispatchService.dispatch(channel, resolvedAddress, notification.getPayload());

        DeliveryAttempt retryAttempt = new DeliveryAttempt(
                notification, failedAttempt, channel, newAttemptNumber);

        if (success) {
            retryAttempt.markSuccess();
            notification.markSent();
        } else if (retryPolicy.isExhausted(newAttemptNumber)) {
            retryAttempt.markFailure(null);
            notification.markFailed();
            log.warn("Notification id={} exhausted retries after {} attempts, dead-lettered",
                    notification.getId(), newAttemptNumber);
        } else {
            retryAttempt.markFailure(retryPolicy.nextRetryAt(newAttemptNumber));
        }

        // Parent is done either way - it produced this child, so it must
        // stop matching the retry-due query going forward.
        failedAttempt.markRetryScheduled();

        retryOutcomeRecorder.record(failedAttempt, retryAttempt, notification);

        log.info("Notification id={} retry attempt #{} on channel={} result={}",
                notification.getId(), newAttemptNumber, channel, success ? "SUCCESS" : "FAILURE");
    }
}
