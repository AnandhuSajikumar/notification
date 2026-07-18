package com.nodev.notification.bl.entity;
import com.nodev.notification.bl.entity.enums.Channel;
import com.nodev.notification.bl.entity.enums.NotificationStatus;
import io.hypersistence.utils.hibernate.type.json.JsonType;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import lombok.Getter;
import org.hibernate.annotations.Type;

import java.time.Instant;
import java.util.Map;

@Entity
@Table(name = "notification_requests")
@Getter
public class NotificationRequest {

    public NotificationRequest(String recipientId,String templateId, Channel channel, Map<String, Object> payload, String idempotencyKey) {
        this.recipientId = recipientId;
        this.templateId = templateId;
        this.channel = channel;
        this.payload = payload;
        this.idempotencyKey = idempotencyKey;
    }

    protected NotificationRequest() {
    }

    @Id
    @SequenceGenerator(name = "notif_req_seq_gen", sequenceName = "notification_requests_seq", allocationSize = 50)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "notif_req_seq_gen")
    private Long id;

    @Column(name = "idempotency_key", nullable = false, unique = true)
    private String idempotencyKey;

    @Column(name = "recipient_id", nullable = false)
    private String recipientId;

    @Column(name = "template_id", nullable = false)
    private String templateId;

    @Type(JsonType.class)  // hypersistence-utils, or convert manually — flag below
    @Column(name = "payload", columnDefinition = "jsonb", nullable = false)
    private Map<String, Object> payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "channel", nullable = false)
    private Channel channel;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private NotificationStatus status = NotificationStatus.PENDING;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;


    public void markDispatched() {
        this.status = NotificationStatus.DISPATCHED;
    }

    public void markSent() {
        this.status = NotificationStatus.SENT;
    }

    public void markDelivered() {
        this.status = NotificationStatus.DELIVERED;
    }

    public void markBounced() {
        this.status = NotificationStatus.BOUNCED;
    }

    public void markFailed() {
        this.status = NotificationStatus.FAILED;
    }


    @PrePersist
    void onCreate() {
        createdAt = Instant.now();
        updatedAt = Instant.now();
    }

    @PreUpdate
    void onUpdate() {
        updatedAt = Instant.now();
    }
}