-- Bounds and shape checks on the platform tables (platform-go v0.6.0).
--
-- Every value below was unbounded TEXT, checked in Go for "not empty" at most. The rules here are
-- the same ones the Go stores now apply before they write, so a value either store refuses, the
-- other refuses too. Lengths are characters (char_length), as Go counts runes, not bytes.
--
--   idempotency_keys.key          1 to 255 characters
--   idempotency_keys.fingerprint  a lowercase hex SHA-256, exactly 64 characters
--   idempotency_keys              completed_at is set exactly when state is completed
--   outbox.topic                  a registry event type: PascalCase, at most 255 characters. The
--                                 column holds the event type, never a Kafka topic; event-bus
--                                 resolves the topic from the registry.
--   outbox.partition_key          at most 255 characters
--   inbox.consumer                1 to 255 characters
--   inbox.event_id                1 to 255 characters, any format (meta-repo ADR-0061: an event id
--                                 is an opaque string, never a UUID by type)
--
-- Services re-apply every platform migration at each boot, so everything here is guarded and a
-- second run changes nothing. Each constraint is added NOT VALID first, which enforces it on every
-- new or updated row without reading the table, and then validated against the existing rows. If
-- old rows break the rule, the constraint stays NOT VALID with a WARNING in the log and the boot
-- goes on: a check must never be what stops a money service from starting. Validation is retried
-- at every boot until it succeeds.
--
-- Down path: drop the seven constraints by name (ALTER TABLE ... DROP CONSTRAINT IF EXISTS ...).
-- Nothing else changes, so nothing else needs undoing. pgtest's down-path test does exactly that.

DO $$
DECLARE
    c record;
BEGIN
    FOR c IN SELECT * FROM (VALUES
        ('idempotency_keys', 'idempotency_keys_key_length',
            'CHECK (char_length(key) BETWEEN 1 AND 255)'),
        ('idempotency_keys', 'idempotency_keys_fingerprint_sha256',
            'CHECK (fingerprint ~ ''^[0-9a-f]{64}$'')'),
        ('idempotency_keys', 'idempotency_keys_completed_at_matches_state',
            'CHECK ((state = ''completed'') = (completed_at IS NOT NULL))'),
        ('outbox', 'outbox_topic_event_type',
            'CHECK (topic ~ ''^[A-Z][A-Za-z0-9]*$'' AND char_length(topic) <= 255)'),
        ('outbox', 'outbox_partition_key_length',
            'CHECK (char_length(partition_key) <= 255)'),
        ('inbox', 'inbox_consumer_length',
            'CHECK (char_length(consumer) BETWEEN 1 AND 255)'),
        ('inbox', 'inbox_event_id_length',
            'CHECK (char_length(event_id) BETWEEN 1 AND 255)')
    ) AS t(tbl, name, def)
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
             WHERE conname = c.name
               AND conrelid = format('%I.%I', current_schema(), c.tbl)::regclass
        ) THEN
            EXECUTE format('ALTER TABLE %I ADD CONSTRAINT %I %s NOT VALID', c.tbl, c.name, c.def);
        END IF;

        IF EXISTS (
            SELECT 1 FROM pg_constraint
             WHERE conname = c.name
               AND conrelid = format('%I.%I', current_schema(), c.tbl)::regclass
               AND NOT convalidated
        ) THEN
            BEGIN
                EXECUTE format('ALTER TABLE %I VALIDATE CONSTRAINT %I', c.tbl, c.name);
            EXCEPTION WHEN check_violation THEN
                RAISE WARNING 'platform-go 0003: existing rows of % break %; it is enforced on new rows and stays NOT VALID until they are fixed',
                    c.tbl, c.name;
            END;
        END IF;
    END LOOP;
END $$;
