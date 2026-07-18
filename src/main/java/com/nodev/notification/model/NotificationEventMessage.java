package com.nodev.notification.model;

import com.nodev.notification.bl.entity.enums.Channel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

/**
 * The consumer's own typed view of the JSON on notifications.outbound.
 * This is NOT the wire contract itself - the wire format is plain JSON
 * (see add.type.headers=false on the producer side), so a consumer in any
 * language could read the same message without knowing this class exists.
 * This class just saves this consumer from manually picking a Map apart.
 *
 * Needs a no-args constructor + setters (unlike our entities) because
 * Jackson builds this via reflection during deserialization, the same way
 * it builds any incoming request DTO.
 */
@Getter
@Setter
@NoArgsConstructor
public class NotificationEventMessage {

    private Long notificationId;
    private String recipientId;
    private String templateId;
    private Channel channel;
    private Map<String, Object> payload;
}
