-- outbox_events: written in the SAME transaction as a notification_requests insert.
-- The poller (separate process) reads unpublished rows here and publishes to Kafka,
-- then flips published = TRUE. This is what guarantees no lost events if the app
-- crashes between the DB commit and the Kafka publish.

CREATE SEQUENCE outbox_events_seq
    INCREMENT BY 50
    START WITH 1;

CREATE TABLE outbox_events (
    id             BIGINT PRIMARY KEY DEFAULT nextval('outbox_events_seq'),
    aggregate_id   BIGINT       NOT NULL REFERENCES notification_requests(id),
    event_type     VARCHAR(32)  NOT NULL,   -- e.g. NOTIFICATION_CREATED
    payload        JSONB        NOT NULL,   -- full event body as published to Kafka
    published      BOOLEAN      NOT NULL DEFAULT FALSE,
    published_at   TIMESTAMPTZ,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- The poller's main query: "give me unpublished rows, oldest first".
-- Partial index keeps this cheap even as the table grows, since published
-- rows (the vast majority over time) are never touched by this index.
CREATE INDEX idx_outbox_events_unpublished
    ON outbox_events (created_at)
    WHERE published = FALSE;