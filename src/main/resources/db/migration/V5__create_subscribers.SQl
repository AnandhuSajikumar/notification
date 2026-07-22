-- Synced copy of recipient contact info, following the "identify" pattern
-- used by Novu/Knock/Courier: the calling service remains the source of
-- truth for who a user is; this table holds just enough to actually
-- deliver to them, kept current via upsert calls from the caller.
CREATE SEQUENCE subscribers_seq INCREMENT BY 50 START WITH 1;

CREATE TABLE subscribers (
    id           BIGINT PRIMARY KEY DEFAULT nextval('subscribers_seq'),
    recipient_id VARCHAR(64)  NOT NULL,
    email        VARCHAR(255),
    phone        VARCHAR(32),
    push_token   VARCHAR(255),
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT uq_subscriber_recipient_id UNIQUE (recipient_id)
);
