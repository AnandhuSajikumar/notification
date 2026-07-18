package com.nodev.notification.bl.entity;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Synced recipient contact info - the "identify" pattern (Novu/Knock/Courier
 * all use this). The calling service remains the source of truth for who
 * the user actually is; this is just enough to deliver to them, kept
 * current via upsertContactInfo() whenever the caller syncs.
 */
@Entity
@Table(name = "subscribers")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Subscriber {

    @Id
    @SequenceGenerator(name = "subscriber_seq_gen", sequenceName = "subscribers_seq", allocationSize = 50)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "subscriber_seq_gen")
    private Long id;

    @Column(name = "recipient_id", nullable = false, unique = true)
    private String recipientId;

    @Column(name = "email")
    private String email;

    @Column(name = "phone")
    private String phone;

    @Column(name = "push_token")
    private String pushToken;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public Subscriber(String recipientId, String email, String phone, String pushToken) {
        this.recipientId = recipientId;
        this.email = email;
        this.phone = phone;
        this.pushToken = pushToken;
    }

    // The upsert target - called on an EXISTING subscriber when the caller
    // re-syncs (e.g. the engineer's email changed in the task-assignment app).
    public void upsertContactInfo(String email, String phone, String pushToken) {
        this.email = email;
        this.phone = phone;
        this.pushToken = pushToken;
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
