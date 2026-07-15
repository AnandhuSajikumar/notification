-- delivery_attempts: one row per concrete channel try.
-- parent_attempt_id is the mechanism that unifies retry and fallback:
--   retry    = new attempt, parent_attempt_id set, SAME channel as parent
--   fallback = new attempt, parent_attempt_id set, DIFFERENT channel than parent
--   first try on a notification = parent_attempt_id is NULL

CREATE SEQUENCE delivery_attempts_seq
    INCREMENT BY 50
    START WITH 1;

CREATE TABLE delivery_attempts (
                                   id                  BIGINT PRIMARY KEY DEFAULT nextval('delivery_attempts_seq'),
                                   notification_id     BIGINT       NOT NULL REFERENCES notification_requests(id),
                                   parent_attempt_id   BIGINT       REFERENCES delivery_attempts(id),
                                   channel             VARCHAR(16)  NOT NULL,
                                   attempt_number      INT          NOT NULL,
                                   result              VARCHAR(16),                -- NULL until the attempt completes
                                   next_retry_at       TIMESTAMPTZ,                 -- set only when result = FAILURE and a retry is scheduled
                                   dispatched_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
                                   webhook_status      VARCHAR(16),                 -- DELIVERED | BOUNCED, set later by simulated webhook callback
                                   webhook_received_at TIMESTAMPTZ,

                                   CONSTRAINT chk_delivery_channel CHECK (channel IN ('EMAIL', 'SMS', 'PUSH')),
                                   CONSTRAINT chk_delivery_result CHECK (result IS NULL OR result IN ('SUCCESS', 'FAILURE')),
                                   CONSTRAINT chk_delivery_webhook_status CHECK (
                                       webhook_status IS NULL OR webhook_status IN ('DELIVERED', 'BOUNCED')
                                       )
);

-- The retry scheduler's core query: "which failed attempts are due for retry now".
-- Partial index (WHERE clause) keeps it small — only failed, not-yet-retried rows are indexed.
CREATE INDEX idx_delivery_attempts_retry_due
    ON delivery_attempts (next_retry_at)
    WHERE result = 'FAILURE' AND next_retry_at IS NOT NULL;

-- Lookup all attempts for a given notification, in order — used to build the
-- "history" view of a notification (first try, retries, any fallback).
CREATE INDEX idx_delivery_attempts_notification
    ON delivery_attempts (notification_id, attempt_number);