-- notification_requests: the durable record of "what was asked".
-- No channel or retry-timing columns here on purpose — those belong to
-- delivery_attempts (see V2), since a single request can produce many
-- attempts across channels (retry = same channel, fallback = different channel).

CREATE SEQUENCE notification_requests_seq
    INCREMENT BY 50   -- must match @SequenceGenerator(allocationSize = 50) in Java
    START WITH 1;

CREATE TABLE notification_requests (
    id                  BIGINT PRIMARY KEY DEFAULT nextval('notification_requests_seq'),
    idempotency_key     VARCHAR(128) NOT NULL,
    recipient_id        VARCHAR(64)  NOT NULL,     -- opaque identifier, no meaning to this service
    template_id         VARCHAR(64)  NOT NULL,
    payload             JSONB        NOT NULL,     -- template variables, producer-defined shape
    status              VARCHAR(16)  NOT NULL DEFAULT 'PENDING',
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT uq_notification_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT chk_notification_status CHECK (
        status IN ('PENDING', 'DISPATCHED', 'SENT', 'DELIVERED', 'BOUNCED', 'FAILED')
    )
);

-- Supports the Week-2 "inspect failed notifications" admin endpoint (paginated).
CREATE INDEX idx_notification_requests_status ON notification_requests (status);