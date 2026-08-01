package com.nodev.notification.load;

import io.gatling.javaapi.core.*;
import io.gatling.javaapi.http.*;

import java.util.UUID;

import static io.gatling.javaapi.core.CoreDsl.*;
import static io.gatling.javaapi.http.HttpDsl.*;

/**
 * Gatling throughput simulation for POST /notifications.
 *
 * Load profile: ramp from 1 to 50 virtual users/second over 30 seconds,
 * then hold at 50 req/s for 60 seconds. Each virtual user uses a unique
 * idempotency key + unique recipientId per request so that idempotency
 * rejection (409) and the Redis rate limiter (5/min per recipient) do not
 * interfere with throughput measurement.
 *
 * This simulation does NOT pre-register subscribers and does NOT assert
 * end-to-end pipeline correctness. Its purpose is to measure POST /notifications
 * acceptance latency and throughput at the HTTP layer. Pipeline correctness
 * under load is covered by NotificationConcurrencyTest (Scenario B).
 *
 * Expected status codes: 201 (accepted), 409 (duplicate key, unlikely with
 * random UUIDs), 429 (rate limit, also unlikely with distinct recipients).
 * All three are treated as non-error for the purposes of this simulation —
 * the meaningful failure signal is 5xx or connection refused.
 *
 * Run independently (service must be running locally first):
 *   mvn gatling:test
 *
 * Override the base URL:
 *   mvn gatling:test -Dgatling.baseUrl=http://your-host:8080
 *
 * Reports are written to target/gatling/ after each run.
 */
public class NotificationLoadSimulation extends Simulation {

    private static final String BASE_URL =
            System.getProperty("gatling.baseUrl", "http://localhost:8080");

    private final HttpProtocolBuilder httpProtocol = http
            .baseUrl(BASE_URL)
            .acceptHeader("application/json")
            .contentTypeHeader("application/json");

    private final ScenarioBuilder scenario = scenario("POST /notifications ramp load")
            .exec(
                    http("create notification")
                            .post("/notifications")
                            .body(StringBody(session -> buildPayload()))
                            .check(status().in(201, 409, 429))
            );

    {
        setUp(
                scenario.injectOpen(
                        rampUsersPerSec(1).to(50).during(30),
                        constantUsersPerSec(50).during(60)
                )
        ).protocols(httpProtocol);
    }

    private static String buildPayload() {
        String uid = UUID.randomUUID().toString();
        return """
                {
                  "recipientId": "gatling-recipient-%s",
                  "templateId": "template-load-test",
                  "channel": "SMS",
                  "payload": {"message": "Gatling load test"},
                  "idempotencyKey": "gatling-idem-%s"
                }
                """.formatted(uid, uid);
    }
}
