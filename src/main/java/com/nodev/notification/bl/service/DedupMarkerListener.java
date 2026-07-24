package com.nodev.notification.bl.service;

import com.nodev.notification.model.NotificationProcessedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * Marks the Redis dedup key only once the DB write is genuinely durable -
 * not inside the same @Transactional method, where the Redis call would
 * execute BEFORE the proxy actually commits. Same pattern as
 * TaskAssignedNotificationListener in the task-assignment app.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class DedupMarkerListener {

    private final NotificationDedupService dedupService;

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onNotificationProcessed(NotificationProcessedEvent event) {
        dedupService.markProcessed(event.notificationId());
        log.debug("Marked notificationId={} as processed in Redis (post-commit)", event.notificationId());
    }
}
