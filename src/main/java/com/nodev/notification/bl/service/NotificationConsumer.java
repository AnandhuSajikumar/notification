package com.nodev.notification.bl.service;

import com.nodev.notification.bl.entity.DeliveryAttempt;
import com.nodev.notification.bl.entity.NotificationRequest;
import com.nodev.notification.bl.entity.Subscriber;
import com.nodev.notification.bl.entity.enums.Channel;
import com.nodev.notification.model.NotificationEventMessage;
import com.nodev.notification.bl.repository.DeliveryAttemptRepository;
import com.nodev.notification.bl.repository.NotificationRequestRepository;
import com.nodev.notification.bl.repository.SubscriberRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Reads notifications.outbound and, for Week 1 (happy path only), creates
 * the first DeliveryAttempt and calls the (stub) dispatch step.
 *
 * KNOWN GAP, deliberate: Kafka's at-least-once delivery means this method
 * can run more than once for the same message (e.g. crash after finishing
 * but before Kafka records it as handled). Nothing here yet stops a
 * duplicate DeliveryAttempt from being created on redelivery - that's
 * exactly the gap Week 2's Redis idempotency dedup closes. Left open
 * intentionally for now, not silently ignored.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class NotificationConsumer {

    private final NotificationRequestRepository notificationRequestRepository;
    private final DeliveryAttemptRepository deliveryAttemptRepository;
    private final SubscriberRepository subscriberRepository;
    private final DispatchService dispatchService;

    @KafkaListener(topics = "${notification.kafka.outbound-topic}")
    @Transactional
    public void onNotificationCreated(NotificationEventMessage event) {
        log.info("Consumed event for notificationId={}", event.getNotificationId());

        NotificationRequest notification = notificationRequestRepository
                .findById((event.getNotificationId()))
                .orElseThrow(() -> new IllegalStateException("Received Kafka event for notificationId=" + event.getNotificationId()
                        + " but no matching row exists in Postgres"));

        notification.markDispatched();

        Channel channel = event.getChannel();

        DeliveryAttempt attempt = new DeliveryAttempt(
                notification,
                null,
                channel,
                1
        );

        String resolvedAddress = resolveAddress(event.getRecipientId(), channel);


        boolean success = resolvedAddress != null
                && dispatchService.dispatch(channel, resolvedAddress, event.getPayload());

        if (success) {
            attempt.markSuccess();
            notification.markSent();
        } else {
            // Either dispatch genuinely failed (Week 1 stub always succeeds,
            // so this won't fire yet), or there was no usable contact info
            // for this channel - both are real failure cases the shape
            // needs to handle even though only the second can happen today.
            attempt.markFailure(null);
            notification.markFailed();
        }

        deliveryAttemptRepository.save(attempt);
    }


    private String resolveAddress(String recipientId, Channel channel) {
        Subscriber subscriber = subscriberRepository.findByRecipientId(recipientId).orElse(null);

        if (subscriber == null) {
            log.warn("No subscriber record for recipientId={}, cannot resolve address", recipientId);
            return null;
        }

        String address = switch (channel) {
            case EMAIL -> subscriber.getEmail();
            case SMS -> subscriber.getPhone();
            case PUSH -> subscriber.getPushToken();
        };
        if (address == null) {
            log.warn("Subscriber recipientId={} has no {} contact info on file", recipientId, channel);
        }

        return address;
    }
}
