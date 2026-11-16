-- Transactional outbox. Requires PostgreSQL >= 13 (gen_random_uuid is built in).
--
-- Contract columns (id, event_id, event_type, aggregate_id, payload, headers,
-- created_at) form the stable public contract consumed downstream.
-- Bookkeeping columns (processed_at, dead_at, attempts, next_attempt_at,
-- last_error) are relay-internal and may change freely.
CREATE TABLE IF NOT EXISTS outbox (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id        UUID        NOT NULL DEFAULT gen_random_uuid(),
    event_type      TEXT        NOT NULL,
    aggregate_id    TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    headers         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    processed_at    TIMESTAMPTZ,
    dead_at         TIMESTAMPTZ,
    attempts        INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error      TEXT
);

-- Hot index for the relay's claim query: only pending rows are interesting.
CREATE INDEX IF NOT EXISTS outbox_pending_idx
    ON outbox (id)
    WHERE processed_at IS NULL AND dead_at IS NULL;

-- Wake the relay after commit. Statement-level: one notification per INSERT
-- statement regardless of row count (the relay drains everything anyway, so
-- coalescing is desirable). PostgreSQL only delivers notifications if the
-- inserting transaction commits, so there are no spurious wakeups on rollback.
CREATE OR REPLACE FUNCTION outbox_notify() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('outbox_inserted', '');
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS outbox_notify_trg ON outbox;
CREATE TRIGGER outbox_notify_trg
    AFTER INSERT ON outbox
    FOR EACH STATEMENT EXECUTE FUNCTION outbox_notify();
