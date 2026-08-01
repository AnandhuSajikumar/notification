package com.nodev.notification;

import com.nodev.notification.bl.entity.NotificationRequest;
import com.nodev.notification.bl.entity.enums.Channel;
import com.nodev.notification.bl.repository.DeliveryAttemptRepository;
import com.nodev.notification.bl.repository.NotificationRequestRepository;
import com.nodev.notification.bl.service.NotificationConsumer;
import com.nodev.notification.model.NotificationEventMessage;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.TestPropertySource;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Scenario C: redelivery dedup proof at the consumer boundary.
 *
 * Calls NotificationConsumer.onNotificationCreated() twice with the same
 * NotificationEventMessage, simulating what happens when Kafka redelivers a
 * message due to at-least-once semantics (e.g. the consumer processes the
 * message but crashes before committing the offset).
 *
 * Scope of this test: it exercises the Redis dedup guard (NotificationDedupService)
 * at the consumer method boundary. It does NOT reproduce actual Kafka broker-level
 * offset or redelivery mechanics — for that, real offset manipulation or a
 * dedicated Kafka test harness (e.g. EmbeddedKafka with seekToBeginning) would
 * be needed. The correctness claim is limited to: given a second invocation with
 * the same notificationId after the first has committed, the consumer exits early
 * and creates no second DeliveryAttempt row.
 *
 * The @TransactionalEventListener(phase = AFTER_COMMIT) in DedupMarkerListener
 * fires synchronously in the same thread after NotificationOutcomeRecorder's
 * transaction commits. This means Redis is already marked by the time the first
 * onNotificationCreated() call returns, so the second call is guaranteed to be
 * caught at the dedupService.isAlreadyProcessed() check.
 *
 * Kept separate from NotificationConcurrencyTest because this test does not need
 * an HTTP client — it operates directly at the Spring bean level.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = "notification.dispatch.simulated-failure-rate=0.0")
@Slf4j
class NotificationRedeliveryDedupTest {

    @LocalServerPort
    @SuppressWarnings("unused")     // RANDOM_PORT annotation matches context used by NotificationConcurrencyTest,
    private int port;               // so Spring reuses the same cached context rather than starting a second one.

    @Autowired
    private NotificationConsumer notificationConsumer;

    @Autowired
    private NotificationRequestRepository notificationRequestRepository;

    @Autowired
    private DeliveryAttemptRepository deliveryAttemptRepository;

    @Test
    void scenarioC_kafkaRedelivery_secondInvocationDeduplicated() {
        // Create the NotificationRequest directly via the repository — deliberately
        // bypassing the API and outbox pipeline. This test is scoped to the consumer
        // boundary; if the row were created via POST /notifications, the OutboxPoller
        // would also publish it to Kafka, causing the consumer's @KafkaListener to
        // process it independently and confound the attempt count.
        NotificationRequest notification = notificationRequestRepository.save(
                new NotificationRequest(
                        "dedup-test-recipient-" + UUID.randomUUID(),
                        "template-dedup-test",
                        Channel.SMS,
                        Map.of("message", "redelivery dedup test"),
                        "dedup-test-key-" + UUID.randomUUID()
                )
        );

        // Build the event as the consumer would receive it from Kafka.
        NotificationEventMessage event = new NotificationEventMessage();
        event.setNotificationId(notification.getId());
        event.setRecipientId(notification.getRecipientId());
        event.setTemplateId(notification.getTemplateId());
        event.setChannel(notification.getChannel());
        event.setPayload(notification.getPayload());

        log.info("Scenario C: first invocation for notificationId={}", notification.getId());
        notificationConsumer.onNotificationCreated(event); // real processing: creates 1 DeliveryAttempt, marks Redis

        log.info("Scenario C: second invocation for notificationId={} (simulated Kafka redelivery)", notification.getId());
        notificationConsumer.onNotificationCreated(event); // must be caught by Redis dedup, no second attempt created

        var attempts = deliveryAttemptRepository
                .findByNotification_IdOrderByAttemptNumberAsc(notification.getId());

        assertThat(attempts)
                .as("Exactly 1 DeliveryAttempt should exist after a simulated redelivery. " +
                    "2+ rows would mean Redis dedup failed to short-circuit the second invocation.")
                .hasSize(1);
    }
}
