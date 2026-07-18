package com.nodev.notification.model;

import com.nodev.notification.bl.entity.enums.Channel;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class CreateNotificationRequest {

    @NotBlank(message = "recipientId is required")
    private String recipientId;

    @NotBlank(message = "templateId is required")
    private String templateId;

    @NotBlank(message = "channel is required")
    private Channel channel;

    @NotEmpty(message = "payload must not be empty")
    private Map<String, Object> payload;

    @NotBlank(message = "idempotencyKey is required")
    private String idempotencyKey;
}