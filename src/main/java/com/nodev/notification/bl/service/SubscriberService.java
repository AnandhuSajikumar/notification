package com.nodev.notification.bl.service;

import com.nodev.notification.bl.entity.Subscriber;
import com.nodev.notification.bl.model.UpsertSubscriberRequest;
import com.nodev.notification.bl.repository.SubscriberRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
@RequiredArgsConstructor
public class SubscriberService {

    private final SubscriberRepository subscriberRepository;

    @Transactional
    public Subscriber upsert(String recipientId, UpsertSubscriberRequest request) {
        Subscriber subscriber = subscriberRepository.findByRecipientId(recipientId)
                .orElse(null);

        if (subscriber == null) {
            subscriber = new Subscriber(recipientId, request.getEmail(), request.getPhone(), request.getPushToken());
            log.info("Created subscriber recipientId={}", recipientId);
        } else {
            subscriber.upsertContactInfo(request.getEmail(), request.getPhone(), request.getPushToken());
            log.info("Updated subscriber recipientId={}", recipientId);
        }

        return subscriberRepository.save(subscriber);
    }
}
