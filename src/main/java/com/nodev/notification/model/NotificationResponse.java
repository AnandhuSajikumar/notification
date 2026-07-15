package com.nodev.notification.model;

import com.nodev.notification.bl.entity.NotificationRequest;
import com.nodev.notification.bl.entity.enums.NotificationStatus;
import lombok.Getter;

@Getter
public class NotificationResponse {
    private final Long id;
    private final String idempotencyKey;
    private final NotificationStatus status;

    public NotificationResponse(NotificationRequest request) {
        this.id = request.getId();
        this.idempotencyKey = request.getIdempotencyKey();
        this.status = request.getStatus();
    }
}
