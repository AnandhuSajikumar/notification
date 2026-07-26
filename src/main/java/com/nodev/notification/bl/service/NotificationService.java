package com.nodev.notification.bl.service;

import com.nodev.notification.bl.entity.NotificationRequest;
import com.nodev.notification.bl.entity.OutboxEvent;
import com.nodev.notification.exceptionHandler.DuplicateNotificationException;
import com.nodev.notification.exceptionHandler.RateLimitExceededException;
import com.nodev.notification.model.CreateNotificationRequest;
import com.nodev.notification.bl.repository.NotificationRequestRepository;
import com.nodev.notification.bl.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class NotificationService {

    private final NotificationRequestRepository notificationRequestRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final RateLimiter rateLimiter;

    @Transactional
    public NotificationRequest createNotification(CreateNotificationRequest request) {

        if (!rateLimiter.allow(request.getRecipientId())) {
            throw new RateLimitExceededException(request.getRecipientId());
        }

        // Fast-path check: NOT the real safety net (that's the DB unique
        // constraint below) - lets us fail cleanly and cheaply in the
        // common, non-racing case.
        notificationRequestRepository.findByIdempotencyKey(request.getIdempotencyKey())
                .ifPresent(existing -> {
                    throw new DuplicateNotificationException(request.getIdempotencyKey());
                });

        NotificationRequest notification = new NotificationRequest(
                request.getRecipientId(),
                request.getTemplateId(),
                request.getChannel(),
                request.getPayload(),
                request.getIdempotencyKey()
        );

        try {
            notification = notificationRequestRepository.save(notification);
        } catch (DataIntegrityViolationException e) {
            // The real safety net - the DB's UNIQUE constraint (V1
            // migration) rejects a raced duplicate insert here.
            log.warn("Duplicate idempotencyKey rejected at DB level: {}", request.getIdempotencyKey());
            throw new DuplicateNotificationException(request.getIdempotencyKey());
        }

        OutboxEvent outboxEvent = new OutboxEvent(
                notification.getId(),
                "NOTIFICATION_CREATED",
                buildEventPayload(notification)
        );
        outboxEventRepository.save(outboxEvent);

        log.info("Created notification id={} idempotencyKey={} status={}",
                notification.getId(), notification.getIdempotencyKey(), notification.getStatus());

        return notification;
    }

    private Map<String, Object> buildEventPayload(NotificationRequest notification) {
        return Map.of(
                "notificationId", notification.getId(),
                "recipientId", notification.getRecipientId(),
                "templateId", notification.getTemplateId(),
                "channel", notification.getChannel(),
                "payload", notification.getPayload()
        );
    }
}
