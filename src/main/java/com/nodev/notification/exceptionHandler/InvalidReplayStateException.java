package com.nodev.notification.exceptionHandler;

import com.nodev.notification.bl.entity.enums.NotificationStatus;

public class InvalidReplayStateException extends RuntimeException {
    public InvalidReplayStateException(Long notificationId, NotificationStatus currentStatus) {
        super("Notification " + notificationId + " cannot be replayed - status is "
                + currentStatus + ", only FAILED notifications can be replayed");
    }
}
