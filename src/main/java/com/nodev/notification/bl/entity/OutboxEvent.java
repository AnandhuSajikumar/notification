package com.nodev.notification.bl.entity;

import io.hypersistence.utils.hibernate.type.json.JsonType;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Type;

import java.time.Instant;
import java.util.Map;

@Entity
@Table(name = "outbox_events")
@Getter
public class OutboxEvent {
    protected OutboxEvent() {
    }

    public OutboxEvent(Long aggregateId, String eventType, Map<String, Object> payload) {
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
    }

    @Id
    @SequenceGenerator(name = "outbox_seq_gen", sequenceName = "outbox_events_seq", allocationSize = 50)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "outbox_seq_gen")
    private Long id;

    @Column(name = "aggregate_id", nullable = false)
    private Long aggregateId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Type(JsonType.class)
    @Column(name = "payload", columnDefinition = "jsonb", nullable = false)
    private Map<String,Object> payload;   // the full event body, already serialized

    @Column(name = "published", nullable = false)
    private boolean published = false;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    public void markPublished() {
        this.published = true;
        this.publishedAt = Instant.now();
    }


    @PrePersist
    void onCreate() {
        createdAt = Instant.now();
    }
}