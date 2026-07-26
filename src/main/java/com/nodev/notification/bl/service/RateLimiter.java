package com.nodev.notification.bl.service;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Fixed-window rate limiter: caps how many DISTINCT notification requests
 * a recipient can generate per minute, across all channels combined.

 * Deliberately checked at REQUEST time (when a notification is created),
 * not at dispatch time - this limits how often someone gets ASKED to be
 * notified, not how many delivery attempts happen. A single notification's
 * own retries are the same ask being retried, not new asks, so they must
 * not count against this budget.

 * Known limitation, accepted deliberately: fixed windows allow a burst
 * right at a window boundary (e.g. 5 at 12:05:59, 5 more at 12:06:00) to
 * slip through above the nominal rate. A sliding-window or token-bucket
 * approach would close that gap at the cost of more complexity - not
 * needed here.
 */
@Component
@RequiredArgsConstructor
public class RateLimiter {

    private static final int MAX_PER_WINDOW = 5;
    private static final Duration WINDOW = Duration.ofMinutes(1);

    private final StringRedisTemplate redisTemplate;

    public boolean allow(String recipientId) {
        long windowBucket = Instant.now().getEpochSecond() / WINDOW.getSeconds();
        String key = "ratelimit:" + recipientId + ":" + windowBucket;

        Long count = redisTemplate.opsForValue().increment(key);

        // Only the request that creates the key sets its expiry - every
        // subsequent increment within the same window just bumps the count.
        if (count != null && count == 1L) {
            redisTemplate.expire(key, WINDOW);
        }

        return count != null && count <= MAX_PER_WINDOW;
    }
}
