package com.nodev.notification.bl.service;

import com.nodev.notification.bl.entity.DeliveryAttempt;
import com.nodev.notification.bl.entity.NotificationRequest;
import com.nodev.notification.bl.repository.DeliveryAttemptRepository;
import com.nodev.notification.bl.repository.NotificationRequestRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Separate bean, deliberately - RetryScheduler calls this method through
 * Spring's proxy (a real bean-to-bean call), not as a private method on
 * itself. @Transactional on a method called via self-invocation
 * (this.someMethod() from inside the same class) is silently ignored,
 * since the proxy that implements the transaction is bypassed entirely.
 */
@Service
@RequiredArgsConstructor
public class RetryOutcomeRecorder {

    private final DeliveryAttemptRepository deliveryAttemptRepository;
    private final NotificationRequestRepository notificationRequestRepository;

    @Transactional
    public void record(DeliveryAttempt parentAttempt, DeliveryAttempt retryAttempt, NotificationRequest notification) {
        deliveryAttemptRepository.save(parentAttempt);
        deliveryAttemptRepository.save(retryAttempt);
        notificationRequestRepository.save(notification);
    }
}
