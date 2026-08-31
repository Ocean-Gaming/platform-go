-- Platform tables every service inherits from the template.
-- Rule 1: this schema lives in the service's OWN database. No shared tables.
-- Rule 2: every row carries tenant_id.

-- ---------------------------------------------------------------- idempotency
-- Rule 3: every command carries an idempotency key; the stored result is
-- replayed on retry. The fingerprint guards against the same key being reused
-- for a different request body.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    tenant_id     TEXT        NOT NULL,
    key           TEXT        NOT NULL,
    fingerprint   TEXT        NOT NULL,
    response      BYTEA,
    state         TEXT        NOT NULL CHECK (state IN ('in_flight', 'completed')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, key)
);

-- ---------------------------------------------------------------------- outbox
-- Rule 4: every state change writes an outbox row in the SAME transaction as
-- the change. A relay publishes committed rows; it never blocks the producer.
CREATE TABLE IF NOT EXISTS outbox (
    id            UUID        PRIMARY KEY,
    tenant_id     TEXT        NOT NULL,
    topic         TEXT        NOT NULL,
    partition_key TEXT        NOT NULL,
    payload       BYTEA       NOT NULL,
    headers       JSONB       NOT NULL DEFAULT '{}'::jsonb,
    occurred_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);

-- The relay's hot path: unpublished rows, oldest first.
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON outbox (occurred_at) WHERE published_at IS NULL;

-- ----------------------------------------------------------------------- inbox
-- Rule 5: consumers dedup on event ID with an inbox in their OWN transaction.
-- Delivery is at-least-once, so this is what makes handlers effectively-once.
CREATE TABLE IF NOT EXISTS inbox (
    event_id      UUID        NOT NULL,
    tenant_id     TEXT        NOT NULL,
    consumer      TEXT        NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Rule 2: tenant_id is part of the KEY, not just a column. Without it one
    -- tenant's delivery of an event id silently suppresses another's, and the
    -- handler is skipped with no error. Tenant-first also matches
    -- idempotency_keys and keeps the index prefix aligned with tenant scans.
    PRIMARY KEY (tenant_id, consumer, event_id)
);

-- ---------------------------------------------------------------- tenant config
-- Rule 7: tenant config is read from a LOCAL snapshot, invalidated by
-- ConfigChanged. Never a request-time call to tenant-config.
CREATE TABLE IF NOT EXISTS tenant_config_snapshot (
    tenant_id     TEXT        PRIMARY KEY,
    version       BIGINT      NOT NULL,
    document      JSONB       NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
