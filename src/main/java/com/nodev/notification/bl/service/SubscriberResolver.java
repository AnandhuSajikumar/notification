package com.nodev.notification.bl.service;

import com.nodev.notification.bl.entity.Subscriber;
import com.nodev.notification.bl.entity.enums.Channel;
import com.nodev.notification.bl.repository.SubscriberRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * recipientId -> real address for a given channel, resolved fresh every
 * time (never cached), so a retry hours later always uses current contact
 * info. Shared between NotificationConsumer (first attempt) and
 * RetryScheduler (subsequent attempts) - one implementation, not two.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class SubscriberResolver {

    private final SubscriberRepository subscriberRepository;

    // Returns null (not a thrown exception) if unresolvable - a missing
    // subscriber or missing channel-specific contact info is a normal,
    // expected failure case, not a data-integrity bug.
    public String resolve(String recipientId, Channel channel) {
        Subscriber subscriber = subscriberRepository.findByRecipientId(recipientId).orElse(null);

        if (subscriber == null) {
            log.warn("No subscriber record for recipientId={}, cannot resolve address", recipientId);
            return null;
        }

        String address = switch (channel) {
            case EMAIL -> subscriber.getEmail();
            case SMS -> subscriber.getPhone();
            case PUSH -> subscriber.getPushToken();
        };

        if (address == null) {
            log.warn("Subscriber recipientId={} has no {} contact info on file", recipientId, channel);
        }

        return address;
    }
}
