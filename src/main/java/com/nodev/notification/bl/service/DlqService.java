package com.nodev.notification.bl.service;

import com.nodev.notification.bl.entity.DeliveryAttempt;
import com.nodev.notification.bl.entity.NotificationRequest;
import com.nodev.notification.bl.entity.enums.NotificationStatus;
import com.nodev.notification.exceptionHandler.InvalidReplayStateException;
import com.nodev.notification.bl.repository.DeliveryAttemptRepository;
import com.nodev.notification.bl.repository.NotificationRequestRepository;
import com.nodev.notification.exceptionHandler.NotificationNotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class DlqService {

    private final NotificationRequestRepository notificationRequestRepository;
    private final DeliveryAttemptRepository deliveryAttemptRepository;
    private final RetryExecutor retryExecutor;

    public Page<NotificationRequest> listByStatus(NotificationStatus status, Pageable pageable) {
        return notificationRequestRepository.findByStatus(status, pageable);
    }

    public void replay(Long notificationId) {
        NotificationRequest notification = notificationRequestRepository.findById(notificationId)
                .orElseThrow(() -> new NotificationNotFoundException(notificationId));

        if (notification.getStatus() != NotificationStatus.FAILED) {
            throw new InvalidReplayStateException(notificationId, notification.getStatus());
        }

        DeliveryAttempt lastAttempt = deliveryAttemptRepository
                .findTopByNotification_IdOrderByAttemptNumberDesc(notificationId)
                .orElseThrow(() -> new IllegalStateException(
                        "Notification " + notificationId + " is FAILED but has no delivery attempts - data integrity issue"));

        retryExecutor.retryOne(lastAttempt);
    }
}
