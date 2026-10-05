-- The inbox's event_id is TEXT, not UUID (meta-repo ADR-0061).
--
-- The envelope's event_id is a string the PRODUCER's schema defines. Most are UUIDs, but the
-- registry (event-bus schemas/bonus-engine/*.schema.json) gives bonus-engine an `evt_`-prefixed
-- ULID. A UUID column refused every one of those with SQLSTATE 22P02, so each consumer of
-- ocean.bonus.v1 dead-lettered every bonus event it was sent. Dedup needs a stable, comparable
-- id, not a particular format, so the column takes any string.
--
-- Guarded, because services re-apply every platform migration at each boot: once the column is
-- TEXT this is a no-op and takes no lock. A UUID's text form is its canonical lowercase spelling,
-- which is what pgx already returned for it, so no existing row changes meaning.
--
-- Down path: none that keeps data. A non-UUID id cannot go back into a UUID column.
DO $$
BEGIN
    IF (SELECT data_type FROM information_schema.columns
         WHERE table_schema = current_schema() AND table_name = 'inbox' AND column_name = 'event_id') = 'uuid' THEN
        ALTER TABLE inbox ALTER COLUMN event_id TYPE TEXT;
    END IF;
END $$;
