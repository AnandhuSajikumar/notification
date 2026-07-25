package com.nodev.notification.bl.service;

import com.nodev.notification.bl.RetryPolicy;
import com.nodev.notification.bl.entity.DeliveryAttempt;
import com.nodev.notification.bl.entity.NotificationRequest;
import com.nodev.notification.bl.entity.enums.Channel;
import com.nodev.notification.model.NotificationEventMessage;
import com.nodev.notification.bl.repository.NotificationRequestRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Reads notifications.outbound and creates the first DeliveryAttempt.

 * FIX (architectural review point A): this method is deliberately NOT
 * (@Transactional) anymore. The old version wrapped the whole thing -
 * including the external dispatch call - in one transaction, which would
 * hold a DB connection open for however long a slow/hanging SMTP call
 * takes. Now: read (short internal transaction via the repository call,
 * entity is detached afterward) -> dispatch (real I/O, no transaction
 * open at all) -> persist (a separate, real transaction, via
 * NotificationOutcomeRecorder). Same principle already applied in
 * RetryExecutor/RetryScheduler, now applied here too.

 * Redis dedup (via NotificationDedupService) guards against Kafka's
 * at-least-once redelivery causing this to run twice for the same event.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class NotificationConsumer {

    private final NotificationRequestRepository notificationRequestRepository;
    private final SubscriberResolver subscriberResolver;
    private final RetryPolicy retryPolicy;
    private final DispatchService dispatchService;
    private final NotificationDedupService dedupService;
    private final NotificationOutcomeRecorder notificationOutcomeRecorder;

    @KafkaListener(topics = "${notification.kafka.outbound-topic}")
    public void onNotificationCreated(NotificationEventMessage event) {
        if (dedupService.isAlreadyProcessed(event.getNotificationId())) {
            log.info("notificationId={} already processed (Kafka redelivery), skipping", event.getNotificationId());
            return;
        }

        log.info("Consumed event for notificationId={}", event.getNotificationId());

        NotificationRequest notification = notificationRequestRepository
                .findById(event.getNotificationId())
                .orElseThrow(() -> new IllegalStateException(
                        "Received Kafka event for notificationId=" + event.getNotificationId()
                                + " but no matching row exists in Postgres"));

        notification.markDispatched();

        Channel channel = event.getChannel();

        DeliveryAttempt attempt = new DeliveryAttempt(
                notification,
                null,   // parentAttempt: null - this is the first try, not a retry/fallback
                channel,
                1       // attemptNumber
        );

        String resolvedAddress = subscriberResolver.resolve(event.getRecipientId(), channel);
        boolean success = resolvedAddress != null
                && dispatchService.dispatch(channel, resolvedAddress, event.getPayload());

        if (success) {
            attempt.markSuccess();
            notification.markSent();
        } else if (retryPolicy.isExhausted(attempt.getAttemptNumber())) {
            attempt.markFailure(null);
            notification.markFailed();
        } else {
            attempt.markFailure(retryPolicy.nextRetryAt(attempt.getAttemptNumber()));
        }


        notificationOutcomeRecorder.record(attempt, notification);

        log.info("Notification id={} attempt #{} on channel={} result={}",
                notification.getId(), attempt.getAttemptNumber(), channel, success ? "SUCCESS" : "FAILURE");
    }
}
