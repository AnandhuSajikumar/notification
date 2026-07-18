-- Channel is the caller's stated intent for the first delivery attempt -
-- NOT execution history (that's still on delivery_attempts). The caller
-- (producer service) owns its own templates and knows which channel a
-- given template was authored for; it tells us explicitly rather than
-- this service trying to infer or look it up itself.
ALTER TABLE notification_requests
    ADD COLUMN channel VARCHAR(16) NOT NULL DEFAULT 'EMAIL';

ALTER TABLE notification_requests
    ADD CONSTRAINT chk_notification_channel CHECK (channel IN ('EMAIL', 'SMS', 'PUSH'));

-- DEFAULT 'EMAIL' above only exists to satisfy NOT NULL for any pre-existing
-- rows from earlier testing; all new inserts will always supply channel
-- explicitly from here on, so the default is really just a one-time
-- backfill convenience, not a real application default.
ALTER TABLE notification_requests
    ALTER COLUMN channel DROP DEFAULT;