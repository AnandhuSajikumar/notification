package com.nodev.notification.exceptionHandler;

public class NotificationNotFoundException extends RuntimeException {
    public NotificationNotFoundException(Long notificationId) {
        super("No notification found with id " + notificationId);
    }
}
