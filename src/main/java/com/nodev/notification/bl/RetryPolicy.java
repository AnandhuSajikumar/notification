package com.nodev.notification.bl;

import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

@Component
public class RetryPolicy {
    private static final int MAX_RETRY = 3;
    private static final Duration[] BACKOFF_SCHEDULE = {
            Duration.ofMinutes(1),
            Duration.ofMinutes(3),
            Duration.ofMinutes(5)
    };

    public Instant nextRetryAt(int failedAttemptNumber){
        if(isExhausted(failedAttemptNumber)) return null;

        int index = Math.min(failedAttemptNumber - 1, BACKOFF_SCHEDULE.length - 1);
        return Instant.now().plus(BACKOFF_SCHEDULE[index]);
    }

    public boolean isExhausted(int failedAttemptNumber){
        return failedAttemptNumber >= MAX_RETRY;
    }
}
