package com.nodev.notification.bl.service;

import com.nodev.notification.bl.RetryPolicy;
import com.nodev.notification.bl.entity.DeliveryAttempt;
import com.nodev.notification.bl.entity.NotificationRequest;
import com.nodev.notification.bl.entity.enums.Channel;
import com.nodev.notification.model.NotificationEventMessage;
import com.nodev.notification.model.NotificationProcessedEvent;
import com.nodev.notification.bl.repository.DeliveryAttemptRepository;
import com.nodev.notification.bl.repository.NotificationRequestRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Reads notifications.outbound and creates the first DeliveryAttempt,
 * calling DispatchService.
 *
 * Redis dedup (via NotificationDedupService) guards against Kafka's
 * at-least-once redelivery causing this to run twice for the same event.
 * Checked at the very start (read-only); the "mark processed" write only
 * happens AFTER the DB transaction genuinely commits (see
 * DedupMarkerListener) - never inside this method itself, which would
 * mark it before commit is guaranteed.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class NotificationConsumer {

    private final NotificationRequestRepository notificationRequestRepository;
    private final DeliveryAttemptRepository deliveryAttemptRepository;
    private final SubscriberResolver subscriberResolver;
    private final RetryPolicy retryPolicy;
    private final DispatchService dispatchService;
    private final NotificationDedupService dedupService;
    private final ApplicationEventPublisher eventPublisher;

    @KafkaListener(topics = "${notification.kafka.outbound-topic}")
    @Transactional
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

        deliveryAttemptRepository.save(attempt);
        notificationRequestRepository.save(notification);

        // Published now, inside the transaction - but DedupMarkerListener
        // only actually touches Redis AFTER this transaction commits.
        eventPublisher.publishEvent(new NotificationProcessedEvent(notification.getId()));

        log.info("Notification id={} attempt #{} on channel={} result={}",
                notification.getId(), attempt.getAttemptNumber(), channel, success ? "SUCCESS" : "FAILURE");
    }
}
