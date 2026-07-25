package com.nodev.notification.bl.service;

import com.nodev.notification.bl.entity.NotificationRequest;
import com.nodev.notification.bl.entity.OutboxEvent;
import com.nodev.notification.exceptionHandler.DuplicateNotificationException;
import com.nodev.notification.bl.repository.NotificationRequestRepository;
import com.nodev.notification.bl.repository.OutboxEventRepository;
import com.nodev.notification.model.CreateNotificationRequest;
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

    @Transactional
    public NotificationRequest createNotification(CreateNotificationRequest request) {

        // Fast-path check: NOT the real safety net (that's the DB unique
        // constraint below) - this just lets us fail cleanly and cheaply
        // in the common, non-racing case, without provoking a raw SQL
        // exception for something that's actually a normal, expected event.
        notificationRequestRepository.findByIdempotencyKey(request.getIdempotencyKey())
                .ifPresent(existing -> {
                    throw new DuplicateNotificationException(request.getIdempotencyKey());
                });

        // Built through the entity's own constructor, not new + setters -
        // there is no moment where an invalid, half-built NotificationRequest exists.
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
            // The real safety net. If two requests with the same
            // idempotencyKey raced past the check above, the DB's UNIQUE
            // constraint (V1 migration) rejects the second insert here -
            // this is what actually guarantees no duplicate, not the
            // check above.
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

    // The event body a consumer will eventually read off Kafka - deliberately
    // small and generic, not the whole entity. It carries just enough for
    // the poller/consumer to act without re-fetching from the DB immediately.
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