-- idempotency_keys.state is an ENUM of its two states (platform-go v0.7.0).
--
-- The column was TEXT with CHECK (state IN ('in_flight', 'completed')), which already refused
-- every other value; the type says the same thing in the schema itself, and a dump or a query
-- plan shows a two-valued type rather than free text. The 2026-10-19 schema review asked for it.
--
-- Guarded, because services re-apply every platform migration at each boot: the type is created
-- once, the column converted once, and the old CHECK dropped once; a later run finds all three
-- done and changes nothing.
--
-- The Go stores write and compare the two values as literals and scan the column into a string,
-- which pgx does for an enum as it did for text, so no Go change rides with this file.
--
-- Down path, in this order (the completed_at check's stored expression is typed, so it is
-- dropped and re-added around any conversion of the column, this way or back):
--   ALTER TABLE idempotency_keys DROP CONSTRAINT idempotency_keys_completed_at_matches_state;
--   ALTER TABLE idempotency_keys ALTER COLUMN state TYPE TEXT USING state::text;
--   ALTER TABLE idempotency_keys ADD CONSTRAINT idempotency_keys_completed_at_matches_state
--       CHECK ((state = 'completed') = (completed_at IS NOT NULL));
--   ALTER TABLE idempotency_keys ADD CONSTRAINT idempotency_keys_state_check
--       CHECK (state IN ('in_flight', 'completed'));
--   DROP TYPE idempotency_state;
-- migrations_integration_test.go runs it. Adding a third state later is
-- ALTER TYPE idempotency_state ADD VALUE, in a new file.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
         WHERE t.typname = 'idempotency_state' AND n.nspname = current_schema()
    ) THEN
        CREATE TYPE idempotency_state AS ENUM ('in_flight', 'completed');
    END IF;

    IF (SELECT data_type FROM information_schema.columns
         WHERE table_schema = current_schema() AND table_name = 'idempotency_keys' AND column_name = 'state') = 'text' THEN
        -- The TEXT-era CHECK first: it says nothing the type does not.
        ALTER TABLE idempotency_keys DROP CONSTRAINT IF EXISTS idempotency_keys_state_check;
        -- 0003's consistency check was stored comparing state to a TEXT literal; once the column
        -- is an enum that stored expression is "idempotency_state = text", which has no operator.
        -- It is re-created after the conversion, where the literal resolves to the enum. Every
        -- row passed it a moment ago, so adding it validated changes nothing and refuses nothing.
        ALTER TABLE idempotency_keys DROP CONSTRAINT IF EXISTS idempotency_keys_completed_at_matches_state;
        ALTER TABLE idempotency_keys ALTER COLUMN state TYPE idempotency_state USING state::idempotency_state;
        ALTER TABLE idempotency_keys ADD CONSTRAINT idempotency_keys_completed_at_matches_state
            CHECK ((state = 'completed') = (completed_at IS NOT NULL));
    END IF;
END $$;
