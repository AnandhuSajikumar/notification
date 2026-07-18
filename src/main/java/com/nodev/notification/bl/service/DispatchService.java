package com.nodev.notification.bl.service;

import com.nodev.notification.bl.entity.enums.Channel;

import java.util.Map;

/**
 * The actual "send" step, abstracted behind an interface on purpose - Week 2
 * swaps this implementation for a real email provider (+ induced failures
 * on simulated channels) WITHOUT the consumer needing to change at all.
 *
 * resolvedAddress is the real destination (email/phone/push token) -
 * already resolved from recipientId via Subscriber lookup by the caller,
 * NOT the opaque recipientId itself. This interface only deals with
 * actually reaching someone, never with looking up who they are.
 */
public interface DispatchService {
    boolean dispatch(Channel channel, String resolvedAddress, Map<String, Object> payload);
}
