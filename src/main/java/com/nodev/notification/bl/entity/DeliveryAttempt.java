package com.nodev.notification.bl.entity;
import com.nodev.notification.bl.entity.enums.AttemptResult;
import com.nodev.notification.bl.entity.enums.Channel;
import com.nodev.notification.bl.entity.enums.WebhookStatus;
import jakarta.persistence.*;
import lombok.Getter;

import java.time.Instant;

@Entity
@Table(name = "delivery_attempts")
@Getter
public class DeliveryAttempt {

    public DeliveryAttempt() {
    }

    public DeliveryAttempt(NotificationRequest notification, DeliveryAttempt parentAttempt,
                           Channel channel, int attemptNumber) {
        this.notification = notification;
        this.parentAttempt = parentAttempt;
        this.channel = channel;
        this.attemptNumber = attemptNumber;
    }

    @Id
    @SequenceGenerator(name = "delivery_attempt_seq_gen", sequenceName = "delivery_attempts_seq", allocationSize = 50)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "delivery_attempt_seq_gen")
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "notification_id", nullable = false)
    private NotificationRequest notification;

    // Self-reference: null if this is the first attempt on a channel,
    // set if this attempt exists BECAUSE a prior attempt (retry or fallback) failed
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "parent_attempt_id")
    private DeliveryAttempt parentAttempt;

    @Enumerated(EnumType.STRING)
    @Column(name = "channel", nullable = false)
    private Channel channel;

    @Column(name = "attempt_number", nullable = false)
    private Integer attemptNumber;

    @Enumerated(EnumType.STRING)
    @Column(name = "result")
    private AttemptResult result;   // null until the attempt completes

    @Column(name = "next_retry_at")
    private Instant nextRetryAt;    // lives HERE now, not on the notification

    @Column(name = "dispatched_at", nullable = false)
    private Instant dispatchedAt;

    @Column(name = "webhook_status")
    @Enumerated(EnumType.STRING)
    private WebhookStatus webhookStatus;

    @Column(name = "webhook_received_at")
    private Instant webhookReceivedAt;



    public void markSuccess() {
        this.result = AttemptResult.SUCCESS;
    }

    public void markFailure(Instant nextRetryAt) {
        this.result = AttemptResult.FAILURE;
        this.nextRetryAt = nextRetryAt;
    }

    public void markRetryScheduled() {
        this.nextRetryAt = null;
    }

    public void recordWebhook(WebhookStatus webhookStatus) {
        this.webhookStatus = webhookStatus;
        this.webhookReceivedAt = Instant.now();
    }

    @PrePersist
    void onCreate() {
        if (dispatchedAt == null) {
            dispatchedAt = Instant.now();
        }

    }
}