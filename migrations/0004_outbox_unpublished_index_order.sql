-- The relay's index matched to its ORDER BY (platform-go v0.6.0).
--
-- The relay reads WHERE published_at IS NULL ORDER BY occurred_at, id. An index on occurred_at
-- alone leaves the tie-break to a sort. The replacement keeps the old index's name on purpose:
-- 0001 re-runs at every boot with CREATE INDEX IF NOT EXISTS outbox_unpublished_idx, and a new name
-- would have it rebuild the old index on every boot, only for this file to drop it again.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_index i
          JOIN pg_class ic ON ic.oid = i.indexrelid
          JOIN pg_namespace n ON n.oid = ic.relnamespace
         WHERE ic.relname = 'outbox_unpublished_idx'
           AND n.nspname = current_schema()
           AND i.indnatts = 1
    ) THEN
        DROP INDEX outbox_unpublished_idx;
    END IF;
    CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
        ON outbox (occurred_at, id) WHERE published_at IS NULL;
END $$;

-- Down path: DROP INDEX outbox_unpublished_idx; CREATE INDEX outbox_unpublished_idx ON outbox
-- (occurred_at) WHERE published_at IS NULL. Either index serves the relay; this one also covers
-- the id tie-break.
