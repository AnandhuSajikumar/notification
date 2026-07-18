package com.nodev.notification.bl.service;

import com.nodev.notification.bl.entity.enums.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Week 1 happy-path stub: always succeeds, no real provider call.
 * Replaced in Week 2 by real email + simulated SMS/PUSH with induced
 * failures - the point of this being an interface is that swap won't
 * touch NotificationConsumer at all.
 */
@Service
@Slf4j
public class StubDispatchService implements DispatchService {

    @Override
    public boolean dispatch(Channel channel, String resolvedAddress, Map<String, Object> payload) {
        log.info("[STUB] Dispatching via {} to address={} payload={}", channel, resolvedAddress, payload);
        return true;
    }
}