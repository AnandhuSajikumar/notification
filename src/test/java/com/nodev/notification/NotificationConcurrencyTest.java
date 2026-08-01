package com.nodev.notification;

import com.nodev.notification.bl.entity.enums.NotificationStatus;
import com.nodev.notification.bl.repository.DeliveryAttemptRepository;
import com.nodev.notification.bl.repository.NotificationRequestRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Scenarios A and B: concurrent idempotency enforcement and full-pipeline
 * no-loss verification.
 *
 * Both scenarios are HTTP-level integration tests and share the same
 * RANDOM_PORT Spring context, so they are co-located in this class.
 *
 * Runs against real local Postgres / Kafka / Redis (native Windows installs).
 * No Testcontainers — intentional: the goal is to prove behaviour under the
 * actual infrastructure this project runs on, not a simulated environment.
 *
 * simulated-failure-rate is forced to 0.0 for both scenarios. Without this,
 * induced random failures would schedule retries at 1-min+ intervals, which
 * would prevent Scenario B's 60-second terminal-status polling window from
 * completing. Subscribers are pre-registered so the SMS resolver finds a
 * real phone number — a missing subscriber is treated as a dispatch failure
 * (same retry-scheduling consequence).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = "notification.dispatch.simulated-failure-rate=0.0")
@Slf4j
class NotificationConcurrencyTest {

    private static final int    SCENARIO_A_THREADS         = 20;
    private static final int    SCENARIO_B_NOTIFICATIONS   = 50;
    private static final long   TERMINAL_POLL_TIMEOUT_SECS = 60;
    private static final long   TERMINAL_POLL_INTERVAL_MS  = 2_000;
    private static final Set<NotificationStatus> TERMINAL =
            Set.of(NotificationStatus.SENT, NotificationStatus.FAILED);

    @LocalServerPort
    private int port;

    @Autowired
    private NotificationRequestRepository notificationRequestRepository;

    @Autowired
    private DeliveryAttemptRepository deliveryAttemptRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private RestTemplate restTemplate;
    private String baseUrl;

    @BeforeEach
    void setUp() {
        restTemplate = new RestTemplate();
        baseUrl = "http://localhost:" + port;
    }

    // -------------------------------------------------------------------------
    // Scenario A — Concurrent duplicate idempotency key race
    // -------------------------------------------------------------------------

    /**
     * Fires 20 concurrent POST /notifications requests with the same
     * idempotency key using a CountDownLatch release-gate pattern:
     * all threads park on startLatch.await() before any HTTP call fires,
     * then a single startLatch.countDown() releases all simultaneously.
     * invokeAll() is not used because it does not guarantee true simultaneity.
     *
     * Asserts:
     * - Exactly 1 response is 201 Created; the remaining 19 are 409 Conflict.
     * - Exactly 1 notification_requests row exists for this idempotency key.
     * - Exactly 1 outbox_events row is tied to that notification.
     *   (OutboxEventRepository has no findByAggregateId method, so this query
     *   is issued via JdbcTemplate rather than adding a method to the repo.)
     */
    @Test
    void scenarioA_concurrentDuplicateIdempotencyKey_exactlyOneCreated() throws InterruptedException {
        String idempotencyKey  = "test-idem-concurrent-" + UUID.randomUUID();
        String baseRecipientId = "test-recipient-scA-"   + UUID.randomUUID();

        // Each thread gets its own recipientId so the per-recipient rate limiter
        // (5/min) does not fire before the idempotency check. The dedup check is
        // purely on idempotencyKey, so different recipients with the same key
        // still fully exercise the concurrent idempotency race.
        for (int i = 0; i < SCENARIO_A_THREADS; i++) {
            registerSmsSubscriber(baseRecipientId + "-t" + i);
        }

        CountDownLatch readyLatch   = new CountDownLatch(SCENARIO_A_THREADS);
        CountDownLatch startLatch   = new CountDownLatch(1);
        AtomicInteger  createdCount = new AtomicInteger(0);
        AtomicInteger  conflictCount = new AtomicInteger(0);
        List<Thread>   threads      = new ArrayList<>(SCENARIO_A_THREADS);

        for (int i = 0; i < SCENARIO_A_THREADS; i++) {
            final String threadRecipientId = baseRecipientId + "-t" + i;
            Thread t = new Thread(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await(); // park here until all threads are ready
                    ResponseEntity<String> response = restTemplate.postForEntity(
                            baseUrl + "/notifications",
                            new HttpEntity<>(buildNotificationPayload(threadRecipientId, idempotencyKey), jsonHeaders()),
                            String.class
                    );
                    if (response.getStatusCode() == HttpStatus.CREATED) {
                        createdCount.incrementAndGet();
                    }
                } catch (HttpClientErrorException e) {
                    if (e.getStatusCode() == HttpStatus.CONFLICT) {
                        conflictCount.incrementAndGet();
                    } else {
                        log.error("Unexpected HTTP {} in scenario A thread", e.getStatusCode());
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            });
            threads.add(t);
            t.start();
        }

        readyLatch.await(10, TimeUnit.SECONDS); // wait for all threads to be parked
        startLatch.countDown();                  // release all simultaneously

        for (Thread t : threads) {
            t.join(15_000);
        }

        assertThat(createdCount.get())
                .as("Exactly 1 thread should receive 201 Created")
                .isEqualTo(1);
        assertThat(conflictCount.get())
                .as("All other threads should receive 409 Conflict")
                .isEqualTo(SCENARIO_A_THREADS - 1);

        var saved = notificationRequestRepository.findByIdempotencyKey(idempotencyKey);
        assertThat(saved)
                .as("Exactly 1 notification_requests row should exist for this idempotency key")
                .isPresent();

        Long notificationId = saved.get().getId();

        Integer outboxCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ?",
                Integer.class,
                notificationId);
        assertThat(outboxCount)
                .as("Exactly 1 outbox_events row should exist for this notification")
                .isEqualTo(1);
    }

    // -------------------------------------------------------------------------
    // Scenario B — Full-pipeline volume, no-loss verification
    // -------------------------------------------------------------------------

    /**
     * Fires 50 concurrent POST /notifications requests with distinct
     * idempotency keys and distinct recipients. Distinct recipients are
     * required because the Redis rate limiter enforces 5 notifications per
     * recipient per minute across all channels — using the same recipient
     * for all 50 would block 45 of them with 429, exercising the rate limiter
     * rather than the pipeline itself.
     *
     * After firing, polls until every created notification reaches a terminal
     * status (SENT or FAILED) within 60 seconds, then asserts:
     *
     * 1. Every notification has at least 1 DeliveryAttempt row. A notification
     *    with zero attempt rows is the exact symptom of the previously-fixed
     *    constructor self-assignment bug (the field was assigned to itself,
     *    leaving attemptNumber null, causing Hibernate to throw at flush time
     *    inside the transaction, rolling back silently and leaving the
     *    notification permanently stuck). This check proves that bug cannot
     *    recur under load.
     *
     * 2. No duplicate attemptNumber values exist per notification. Duplicates
     *    would mean Redis dedup failed to prevent the same Kafka event from
     *    being processed twice, resulting in two DeliveryAttempt rows for
     *    attempt #1 on the same notification.
     */
    @Test
    void scenarioB_fullPipelineVolume_noLossNoSilentDuplicates() throws InterruptedException {
        List<String> recipientIds    = new ArrayList<>(SCENARIO_B_NOTIFICATIONS);
        List<String> idempotencyKeys = new ArrayList<>(SCENARIO_B_NOTIFICATIONS);

        for (int i = 0; i < SCENARIO_B_NOTIFICATIONS; i++) {
            recipientIds.add("test-recipient-scB-" + i + "-" + UUID.randomUUID());
            idempotencyKeys.add("test-idem-scB-" + i + "-" + UUID.randomUUID());
        }

        // Register all subscribers upfront so SMS dispatch can resolve a real
        // phone number. A missing subscriber is treated as a dispatch failure,
        // which schedules a retry at 1+ minute — outside the polling window.
        for (String recipientId : recipientIds) {
            registerSmsSubscriber(recipientId);
        }

        CountDownLatch    readyLatch  = new CountDownLatch(SCENARIO_B_NOTIFICATIONS);
        CountDownLatch    startLatch  = new CountDownLatch(1);
        List<Long>        createdIds  = Collections.synchronizedList(new ArrayList<>());
        List<Thread>      threads     = new ArrayList<>(SCENARIO_B_NOTIFICATIONS);

        for (int i = 0; i < SCENARIO_B_NOTIFICATIONS; i++) {
            final String recipientId    = recipientIds.get(i);
            final String idempotencyKey = idempotencyKeys.get(i);

            Thread t = new Thread(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await();
                    @SuppressWarnings("unchecked")
                    ResponseEntity<Map<String, Object>> response = restTemplate.exchange(
                            baseUrl + "/notifications",
                            HttpMethod.POST,
                            new HttpEntity<>(buildNotificationPayload(recipientId, idempotencyKey), jsonHeaders()),
                            (Class<Map<String, Object>>) (Class<?>) Map.class
                    );
                    if (response.getStatusCode() == HttpStatus.CREATED && response.getBody() != null) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> data = (Map<String, Object>) response.getBody().get("data");
                        if (data != null && data.get("id") instanceof Number id) {
                            createdIds.add(id.longValue());
                        }
                    }
                } catch (HttpClientErrorException e) {
                    log.error("Unexpected HTTP {} in scenario B thread: {}", e.getStatusCode(), e.getResponseBodyAsString());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            });
            threads.add(t);
            t.start();
        }

        readyLatch.await(10, TimeUnit.SECONDS);
        startLatch.countDown();

        for (Thread t : threads) {
            t.join(15_000);
        }

        assertThat(createdIds)
                .as("All %d notifications should have been created (no unexpected rejections)", SCENARIO_B_NOTIFICATIONS)
                .hasSize(SCENARIO_B_NOTIFICATIONS);

        log.info("Scenario B: {} notifications created, polling for terminal status (timeout={}s)...",
                createdIds.size(), TERMINAL_POLL_TIMEOUT_SECS);

        // Poll until every notification reaches SENT or FAILED, or timeout expires.
        long deadline = System.currentTimeMillis() + (TERMINAL_POLL_TIMEOUT_SECS * 1_000);
        List<Long> nonTerminal;

        do {
            Thread.sleep(TERMINAL_POLL_INTERVAL_MS);
            nonTerminal = createdIds.stream()
                    .filter(id -> notificationRequestRepository.findById(id)
                            .map(n -> !TERMINAL.contains(n.getStatus()))
                            .orElse(false))
                    .collect(Collectors.toList());

            log.info("Scenario B: {}/{} notifications still non-terminal",
                    nonTerminal.size(), createdIds.size());
        } while (!nonTerminal.isEmpty() && System.currentTimeMillis() < deadline);

        assertThat(nonTerminal)
                .as("Every notification must reach SENT or FAILED within %ds. " +
                    "Still non-terminal: %s", TERMINAL_POLL_TIMEOUT_SECS, nonTerminal)
                .isEmpty();

        // Every notification must have at least 1 DeliveryAttempt, with no
        // duplicate attempt numbers.
        for (Long notificationId : createdIds) {
            var attempts = deliveryAttemptRepository
                    .findByNotification_IdOrderByAttemptNumberAsc(notificationId);

            assertThat(attempts)
                    .as("notificationId=%d must have at least 1 DeliveryAttempt row — " +
                        "zero rows is the constructor self-assignment bug symptom", notificationId)
                    .isNotEmpty();

            List<Integer> attemptNumbers = attempts.stream()
                    .map(a -> a.getAttemptNumber())
                    .collect(Collectors.toList());

            assertThat(attemptNumbers)
                    .as("notificationId=%d has duplicate attempt numbers %s — " +
                        "Redis dedup failed to prevent duplicate processing", notificationId, attemptNumbers)
                    .doesNotHaveDuplicates();
        }

        log.info("Scenario B: all {} notifications verified — no loss, no silent duplicates", createdIds.size());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Map<String, Object> buildNotificationPayload(String recipientId, String idempotencyKey) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("recipientId",    recipientId);
        body.put("templateId",     "template-load-test");
        body.put("channel",        "SMS");
        body.put("payload",        Map.of("message", "load test notification"));
        body.put("idempotencyKey", idempotencyKey);
        return body;
    }

    private HttpHeaders jsonHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return headers;
    }

    /**
     * Registers a subscriber with a dummy phone number via PUT /subscribers.
     * Required for SMS dispatch: SubscriberResolver.resolve() returns null for
     * an unknown recipientId, which is treated as a dispatch failure — not an
     * exception — and schedules a retry. With pre-registered subscribers and
     * simulated-failure-rate=0.0, SMS dispatch always succeeds on the first try.
     */
    private void registerSmsSubscriber(String recipientId) {
        restTemplate.exchange(
                baseUrl + "/subscribers/" + recipientId,
                HttpMethod.PUT,
                new HttpEntity<>(Map.of("phone", "+11234567890"), jsonHeaders()),
                String.class
        );
    }
}
