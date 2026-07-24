package com.nodev.notification.bl.service;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Guards against Kafka's at-least-once redelivery causing the SAME
 * notification-created event to be processed twice by NotificationConsumer.
 *
 * Deliberately marks a message as "done" only AFTER processing succeeds,
 * not before (a "claim first" approach would risk a notification getting
 * permanently stuck with zero DeliveryAttempt rows if the consumer crashed
 * mid-processing - RetryScheduler can't rescue something that was never
 * attempted at all). The narrow tradeoff: a crash in the brief window
 * between DB commit and this Redis SET could still allow one duplicate -
 * same "prefer a duplicate over a loss" philosophy already used by the
 * outbox poller.
 */
@Component
@RequiredArgsConstructor
public class NotificationDedupService {

    private static final String KEY_PREFIX = "notification:processed:";
    private static final Duration TTL = Duration.ofHours(24);

    private final StringRedisTemplate redisTemplate;

    public boolean isAlreadyProcessed(Long notificationId) {
        return Boolean.TRUE.equals(redisTemplate.hasKey(key(notificationId)));
    }

    public void markProcessed(Long notificationId) {
        redisTemplate.opsForValue().set(key(notificationId), "1", TTL);
    }

    private String key(Long notificationId) {
        return KEY_PREFIX + notificationId;
    }
}
