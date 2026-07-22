package com.nodev.notification.bl.service;

import com.nodev.notification.bl.entity.enums.Channel;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.MailException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * EMAIL: real SMTP send, actually reaches an inbox - the core "does this
 * system genuinely notify people" question, per the original design
 * decision to not fully simulate the core functionality.
 * SMS/PUSH: simulated with induced failures - real integrations there add
 * setup friction (carrier verification etc.) without teaching new concepts,
 * per that same original decision.
 *
 * Payload contract for EMAIL: optional "subject" and "body" string keys.
 * No template rendering here (explicitly out of scope) - the caller
 * supplies ready-to-send content in the payload; missing keys fall back
 * to generic defaults rather than failing.
 */
@Service
@Slf4j
public class EmailDispatchService implements DispatchService {

    private final JavaMailSender mailSender;
    private final double simulatedFailureRate;

    public EmailDispatchService(
            JavaMailSender mailSender,
            @Value("${notification.dispatch.simulated-failure-rate}") double simulatedFailureRate) {
        this.mailSender = mailSender;
        this.simulatedFailureRate = simulatedFailureRate;
    }

    @Override
    public boolean dispatch(Channel channel, String resolvedAddress, Map<String, Object> payload) {
        return switch (channel) {
            case EMAIL -> sendRealEmail(resolvedAddress, payload);
            case SMS, PUSH -> simulateDispatch(channel, resolvedAddress);
        };
    }

    private boolean sendRealEmail(String toAddress, Map<String, Object> payload) {
        String subject = String.valueOf(payload.getOrDefault("subject", "Notification"));
        String body = String.valueOf(payload.getOrDefault("body", payload));

        try {
            MimeMessage message = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message);
            helper.setTo(toAddress);
            helper.setSubject(subject);
            helper.setText(body);

            mailSender.send(message);

            log.info("Sent real email to={} subject={}", toAddress, subject);
            return true;

        } catch (MessagingException | MailException e) {
            // Never let this propagate - a provider failure is a normal,
            // expected outcome the retry mechanism handles, not a crash.
            log.error("Failed to send email to={}", toAddress, e);
            return false;
        }
    }

    private boolean simulateDispatch(Channel channel, String address) {
        boolean failed = ThreadLocalRandom.current().nextDouble() < simulatedFailureRate;

        if (failed) {
            log.warn("[SIMULATED] {} dispatch to {} FAILED (induced)", channel, address);
            return false;
        }

        log.info("[SIMULATED] {} dispatch to {} succeeded", channel, address);
        return true;
    }
}
