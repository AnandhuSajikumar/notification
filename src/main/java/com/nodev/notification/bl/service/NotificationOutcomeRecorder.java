package com.nodev.notification.bl.service;

import com.nodev.notification.bl.entity.DeliveryAttempt;
import com.nodev.notification.bl.entity.NotificationRequest;
import com.nodev.notification.model.NotificationProcessedEvent;
import com.nodev.notification.bl.repository.DeliveryAttemptRepository;
import com.nodev.notification.bl.repository.NotificationRequestRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Separate bean, same reason as RetryOutcomeRecorder: called via Spring's
 * proxy from NotificationConsumer (a real bean-to-bean call), not as a
 * private method on the consumer itself, so @Transactional actually applies.
 *
 * The event MUST be published from inside this transactional method, not
 * from the (now non-transactional) consumer - @TransactionalEventListener
 * with AFTER_COMMIT only fires if the publish happens within an active
 * transaction; published outside one, it's silently dropped.
 */
@Service
@RequiredArgsConstructor
public class NotificationOutcomeRecorder {

    private final DeliveryAttemptRepository deliveryAttemptRepository;
    private final NotificationRequestRepository notificationRequestRepository;
    private final ApplicationEventPublisher eventPublisher;

    @Transactional
    public void record(DeliveryAttempt attempt, NotificationRequest notification) {
        deliveryAttemptRepository.save(attempt);
        notificationRequestRepository.save(notification);
        eventPublisher.publishEvent(new NotificationProcessedEvent(notification.getId()));
    }
}
