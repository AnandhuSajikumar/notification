package com.nodev.notification.exceptionHandler;

public class RateLimitExceededException extends RuntimeException {
    public RateLimitExceededException(String recipientId) {
        super("Rate limit exceeded for recipientId '" + recipientId + "' - max 5 notifications per minute");
    }
}
