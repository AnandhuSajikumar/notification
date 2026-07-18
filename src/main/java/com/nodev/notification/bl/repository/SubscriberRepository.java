package com.nodev.notification.bl.repository;

import com.nodev.notification.bl.entity.Subscriber;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface SubscriberRepository extends JpaRepository<Subscriber, Long> {
    Optional<Subscriber> findByRecipientId(String recipientId);
}
