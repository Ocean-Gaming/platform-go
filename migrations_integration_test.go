//go:build integration

package platform_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	platform "github.com/Ocean-Gaming/platform-go"
	"github.com/Ocean-Gaming/platform-go/idempotency"
	pg2 "github.com/Ocean-Gaming/platform-go/pg"
	"github.com/Ocean-Gaming/platform-go/pgtest"
	"github.com/Ocean-Gaming/platform-go/tenant"
)

// The seven constraints migrations/0003 adds, by table.
var checks = map[string]string{
	"idempotency_keys_key_length":                 "idempotency_keys",
	"idempotency_keys_fingerprint_sha256":         "idempotency_keys",
	"idempotency_keys_completed_at_matches_state": "idempotency_keys",
	"outbox_topic_event_type":                     "outbox",
	"outbox_partition_key_length":                 "outbox",
	"inbox_consumer_length":                       "inbox",
	"inbox_event_id_length":                       "inbox",
}

// applyPlatform runs every platform migration in order, the way a service does at each boot.
func applyPlatform(t *testing.T, db *sql.DB) {
	t.Helper()
	src := platform.Migrations()
	files, err := fs.Glob(src, "migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(files)
	for _, f := range files {
		b, err := fs.ReadFile(src, f)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(string(b)); err != nil {
			t.Fatalf("apply %s: %v", f, err)
		}
	}
}

// validated reports whether a constraint exists, and whether it has been validated.
func validated(t *testing.T, db *sql.DB, name string) (exists, valid bool) {
	t.Helper()
	err := db.QueryRow(`SELECT convalidated FROM pg_constraint WHERE conname = $1`, name).Scan(&valid)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return true, valid
}

func unpublishedIndex(t *testing.T, db *sql.DB) string {
	t.Helper()
	var def string
	if err := db.QueryRow(`SELECT pg_get_indexdef('outbox_unpublished_idx'::regclass)`).Scan(&def); err != nil {
		t.Fatal(err)
	}
	return def
}

func isCheckViolation(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23514"
}

// Services apply every platform migration at every boot. A second and third run must change
// nothing and fail on nothing, and leave each check validated and the relay's index ordered.
func TestThePlatformMigrationsReapplyCleanly(t *testing.T) {
	db := pgtest.Open(t)
	applyPlatform(t, db)
	applyPlatform(t, db)

	for name := range checks {
		if exists, valid := validated(t, db, name); !exists || !valid {
			t.Errorf("%s: exists=%v validated=%v", name, exists, valid)
		}
	}
	if def := unpublishedIndex(t, db); !strings.Contains(def, "(occurred_at, id)") || !strings.Contains(def, "published_at IS NULL") {
		t.Fatalf("outbox_unpublished_idx is %q; want (occurred_at, id) WHERE published_at IS NULL", def)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM pg_indexes WHERE tablename = 'outbox' AND indexname <> 'outbox_pkey'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("outbox has %d secondary indexes; the old occurred_at index must be replaced, not kept beside", n)
	}
}

// The checks hold in SQL itself, for a writer that does not go through the Go stores.
func TestTheChecksRefuseBadRowsWrittenInSQL(t *testing.T) {
	db := pgtest.Open(t)
	fp := idempotency.Fingerprint([]byte("cmd"))
	long := strings.Repeat("x", 256)

	good := []struct {
		name, sql string
		args      []any
	}{
		{"an in-flight key", `INSERT INTO idempotency_keys (tenant_id, key, fingerprint, state) VALUES ('acme', 'k-ok', $1, 'in_flight')`, []any{fp}},
		{"a completed key", `INSERT INTO idempotency_keys (tenant_id, key, fingerprint, state, completed_at) VALUES ('acme', 'k-done', $1, 'completed', now())`, []any{fp}},
		{"an outbox row", `INSERT INTO outbox (id, tenant_id, topic, partition_key, payload) VALUES ('11111111-1111-4111-8111-111111111111', 'acme', 'BetPlaced', 'p', '\x7b7d')`, nil},
		{"an inbox row with an evt_ id", `INSERT INTO inbox (event_id, tenant_id, consumer) VALUES ('evt_01m3xv7mjv5tze0p0fx7c4dt49', 'acme', 'c')`, nil},
	}
	for _, g := range good {
		if _, err := db.Exec(g.sql, g.args...); err != nil {
			t.Fatalf("%s was refused: %v", g.name, err)
		}
	}

	bad := []struct {
		name, sql string
		args      []any
	}{
		{"an empty key", `INSERT INTO idempotency_keys (tenant_id, key, fingerprint, state) VALUES ('acme', '', $1, 'in_flight')`, []any{fp}},
		{"a 256-character key", `INSERT INTO idempotency_keys (tenant_id, key, fingerprint, state) VALUES ('acme', $1, $2, 'in_flight')`, []any{long, fp}},
		{"a placeholder fingerprint", `INSERT INTO idempotency_keys (tenant_id, key, fingerprint, state) VALUES ('acme', 'k-fp', 'fp', 'in_flight')`, nil},
		{"an uppercase fingerprint", `INSERT INTO idempotency_keys (tenant_id, key, fingerprint, state) VALUES ('acme', 'k-up', $1, 'in_flight')`, []any{strings.ToUpper(fp)}},
		{"completed with no completed_at", `INSERT INTO idempotency_keys (tenant_id, key, fingerprint, state) VALUES ('acme', 'k-c', $1, 'completed')`, []any{fp}},
		{"in flight with a completed_at", `INSERT INTO idempotency_keys (tenant_id, key, fingerprint, state, completed_at) VALUES ('acme', 'k-i', $1, 'in_flight', now())`, []any{fp}},
		{"a Kafka topic name as topic", `INSERT INTO outbox (id, tenant_id, topic, partition_key, payload) VALUES ('22222222-2222-4222-8222-222222222222', 'acme', 'ocean.wallet.v1', 'p', '\x7b7d')`, nil},
		{"a 256-character partition key", `INSERT INTO outbox (id, tenant_id, topic, partition_key, payload) VALUES ('33333333-3333-4333-8333-333333333333', 'acme', 'BetPlaced', $1, '\x7b7d')`, []any{long}},
		{"an empty consumer", `INSERT INTO inbox (event_id, tenant_id, consumer) VALUES ('e-1', 'acme', '')`, nil},
		{"an empty event id", `INSERT INTO inbox (event_id, tenant_id, consumer) VALUES ('', 'acme', 'c')`, nil},
		{"a 256-character event id", `INSERT INTO inbox (event_id, tenant_id, consumer) VALUES ($1, 'acme', 'c')`, []any{long}},
	}
	for _, b := range bad {
		if _, err := db.Exec(b.sql, b.args...); !isCheckViolation(err) {
			t.Errorf("%s: want a check violation (23514), got %v", b.name, err)
		}
	}
}

// A database that already holds a row breaking a new rule must still boot. The check is enforced
// on new rows at once, stays NOT VALID while the old row is there, and validates on the first
// boot after the row is fixed.
func TestOldRowsThatBreakACheckDoNotStopTheBoot(t *testing.T) {
	db := pgtest.Open(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `ALTER TABLE idempotency_keys DROP CONSTRAINT idempotency_keys_fingerprint_sha256`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO idempotency_keys (tenant_id, key, fingerprint, state) VALUES ('acme', 'k-old', 'fp', 'in_flight')`); err != nil {
		t.Fatal(err)
	}

	applyPlatform(t, db) // the boot after the upgrade
	if exists, valid := validated(t, db, "idempotency_keys_fingerprint_sha256"); !exists || valid {
		t.Fatalf("with an old bad row: exists=%v validated=%v; want present and NOT VALID", exists, valid)
	}
	_, err := db.ExecContext(ctx, `INSERT INTO idempotency_keys (tenant_id, key, fingerprint, state) VALUES ('acme', 'k-new', 'fp', 'in_flight')`)
	if !isCheckViolation(err) {
		t.Fatalf("a NOT VALID check must still refuse a new bad row; got %v", err)
	}

	if _, err := db.ExecContext(ctx, `DELETE FROM idempotency_keys WHERE key = 'k-old'`); err != nil {
		t.Fatal(err)
	}
	applyPlatform(t, db) // the next boot
	if _, valid := validated(t, db, "idempotency_keys_fingerprint_sha256"); !valid {
		t.Fatal("the check did not validate on the first boot after the old row was gone")
	}
}

// 0005: state is an ENUM, so a third value is refused by the type itself, and the migration
// re-applies without touching a converted column.
func TestStateIsAnEnumOfItsTwoValues(t *testing.T) {
	db := pgtest.Open(t)
	applyPlatform(t, db) // a second boot
	var typ string
	if err := db.QueryRow(`SELECT udt_name FROM information_schema.columns WHERE table_name = 'idempotency_keys' AND column_name = 'state'`).Scan(&typ); err != nil {
		t.Fatal(err)
	}
	if typ != "idempotency_state" {
		t.Fatalf("state is %s, want idempotency_state", typ)
	}
	if exists, _ := validated(t, db, "idempotency_keys_state_check"); exists {
		t.Fatal("the TEXT-era CHECK on state survived the conversion")
	}
	fp := idempotency.Fingerprint([]byte("cmd"))
	_, err := db.Exec(`INSERT INTO idempotency_keys (tenant_id, key, fingerprint, state) VALUES ('acme', 'k-x', $1, 'done')`, fp)
	var pg *pgconn.PgError
	if !errors.As(err, &pg) || pg.Code != "22P02" {
		t.Fatalf("a third state: want invalid enum input (22P02), got %v", err)
	}
	// The Go stores still write and read the two values as strings.
	ctx := tenant.NewContext(context.Background(), "acme")
	if err := pg2.InTx(ctx, db, func(tx *sql.Tx) error {
		st := idempotency.NewPostgresStore(tx)
		if _, _, err := st.Claim(ctx, "k-enum", fp); err != nil {
			return err
		}
		return st.Complete(ctx, "k-enum", []byte(`{}`))
	}); err != nil {
		t.Fatal(err)
	}
	if err := pg2.InTx(ctx, db, func(tx *sql.Tx) error {
		rec, replay, err := idempotency.NewPostgresStore(tx).Claim(ctx, "k-enum", fp)
		if err != nil || !replay || rec.State != idempotency.StateCompleted {
			return fmt.Errorf("replay=%v state=%q err=%v", replay, rec.State, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// The down path the migrations describe, and the way back up from it.
func TestTheDownPathRestoresTheOldSchemaAndTheUpPathReturns(t *testing.T) {
	db := pgtest.Open(t)
	// 0005's down path first, as the file states it: the completed_at check dropped and re-added
	// around the conversion (its stored expression is typed), the column back to TEXT with its
	// old CHECK, and the type dropped.
	if _, err := db.Exec(`ALTER TABLE idempotency_keys DROP CONSTRAINT idempotency_keys_completed_at_matches_state;
		ALTER TABLE idempotency_keys ALTER COLUMN state TYPE TEXT USING state::text;
		ALTER TABLE idempotency_keys ADD CONSTRAINT idempotency_keys_completed_at_matches_state CHECK ((state = 'completed') = (completed_at IS NOT NULL));
		ALTER TABLE idempotency_keys ADD CONSTRAINT idempotency_keys_state_check CHECK (state IN ('in_flight', 'completed'));
		DROP TYPE idempotency_state`); err != nil {
		t.Fatalf("0005 down path: %v", err)
	}
	for name, table := range checks {
		if _, err := db.Exec(`ALTER TABLE ` + table + ` DROP CONSTRAINT IF EXISTS ` + name); err != nil {
			t.Fatalf("drop %s: %v", name, err)
		}
	}
	if _, err := db.Exec(`DROP INDEX outbox_unpublished_idx;
		CREATE INDEX outbox_unpublished_idx ON outbox (occurred_at) WHERE published_at IS NULL`); err != nil {
		t.Fatal(err)
	}
	for name := range checks {
		if exists, _ := validated(t, db, name); exists {
			t.Fatalf("%s survived the down path", name)
		}
	}
	if def := unpublishedIndex(t, db); strings.Contains(def, "id)") {
		t.Fatalf("down path left %q", def)
	}

	applyPlatform(t, db)
	for name := range checks {
		if exists, valid := validated(t, db, name); !exists || !valid {
			t.Errorf("after re-applying, %s: exists=%v validated=%v", name, exists, valid)
		}
	}
	if def := unpublishedIndex(t, db); !strings.Contains(def, "(occurred_at, id)") {
		t.Fatalf("after re-applying, outbox_unpublished_idx is %q", def)
	}
	var typ string
	if err := db.QueryRow(`SELECT udt_name FROM information_schema.columns WHERE table_name = 'idempotency_keys' AND column_name = 'state'`).Scan(&typ); err != nil {
		t.Fatal(err)
	}
	if typ != "idempotency_state" {
		t.Fatalf("after re-applying, state is %s", typ)
	}
}
