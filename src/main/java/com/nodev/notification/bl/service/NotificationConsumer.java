package com.nodev.notification.bl.service;

import com.nodev.notification.bl.RetryPolicy;
import com.nodev.notification.bl.entity.DeliveryAttempt;
import com.nodev.notification.bl.entity.NotificationRequest;
import com.nodev.notification.bl.entity.enums.Channel;
import com.nodev.notification.model.NotificationEventMessage;
import com.nodev.notification.bl.repository.DeliveryAttemptRepository;
import com.nodev.notification.bl.repository.NotificationRequestRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;


@Service
@Slf4j
@RequiredArgsConstructor
public class NotificationConsumer {

    private final NotificationRequestRepository notificationRequestRepository;
    private final DeliveryAttemptRepository deliveryAttemptRepository;
    private final SubscriberResolver subscriberResolver;
    private final RetryPolicy retryPolicy;
    private final DispatchService dispatchService;

    @KafkaListener(topics = "${notification.kafka.outbound-topic}")
    @Transactional
    public void onNotificationCreated(NotificationEventMessage event) {
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
            // MAX_ATTEMPTS would somehow be 1 - no retry even gets a chance.
            // Not the normal case with MAX_ATTEMPTS=3, but the branch must
            // still exist and be correct.
            attempt.markFailure(null);
            notification.markFailed();
        } else {
            attempt.markFailure(retryPolicy.nextRetryAt(attempt.getAttemptNumber()));
            // notification.status stays DISPATCHED - it's not FAILED yet,
            // a retry is still pending.
        }

        deliveryAttemptRepository.save(attempt);
        notificationRequestRepository.save(notification);

        log.info("Notification id={} attempt #{} on channel={} result={}",
                notification.getId(), attempt.getAttemptNumber(), channel, success ? "SUCCESS" : "FAILURE");
    }
}
