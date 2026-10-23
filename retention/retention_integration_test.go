//go:build integration

package retention_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Ocean-Gaming/platform-go/idempotency"
	"github.com/Ocean-Gaming/platform-go/pgtest"
	"github.com/Ocean-Gaming/platform-go/retention"
	"github.com/Ocean-Gaming/platform-go/tenant"
)

var (
	now = time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	day = 24 * time.Hour
	fp  = idempotency.Fingerprint([]byte("cmd"))
)

func exec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func count(t *testing.T, db *sql.DB, q string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// seed writes, for one tenant, one row of every kind the policies must and must not touch.
func seed(t *testing.T, db *sql.DB, tid string, i int) {
	t.Helper()
	old, fresh := now.Add(-40*day), now.Add(-day)
	uid := func(k int) string { return fmt.Sprintf("%08d-0000-4000-8000-%012d", i, k) }
	exec(t, db, `INSERT INTO outbox (id, tenant_id, topic, partition_key, payload, occurred_at, published_at) VALUES
		($1, $5, 'BetPlaced', 'p', '\x7b7d', $6, $6),      -- published long ago: goes
		($2, $5, 'BetPlaced', 'p', '\x7b7d', $7, $7),      -- published yesterday: stays
		($3, $5, 'BetPlaced', 'p', '\x7b7d', $6, NULL),    -- old and never published: stays, always
		($4, $5, 'BetPlaced', 'p', '\x7b7d', $7, NULL)`, // fresh and unpublished: stays
		uid(1), uid(2), uid(3), uid(4), tid, old, fresh)
	exec(t, db, `INSERT INTO inbox (event_id, tenant_id, consumer, processed_at) VALUES
		($1, $3, 'c', $4), ($2, $3, 'c', $5)`, uid(5), uid(6), tid, old, fresh)
	exec(t, db, `INSERT INTO idempotency_keys (tenant_id, key, fingerprint, response, state, created_at, completed_at) VALUES
		($1, 'k-old-done',   $2, '\x7b7d', 'completed', $3, $3),   -- response cleared, then key deleted
		($1, 'k-fresh-done', $2, '\x7b7d', 'completed', $4, $4),   -- stays whole
		($1, 'k-old-flight', $2, NULL,     'in_flight', $3, NULL), -- never deleted; reported
		($1, 'k-new-flight', $2, NULL,     'in_flight', $4, NULL)`, // never deleted; not stale
		tid, fp, old, fresh)
}

func sweeper(db *sql.DB, p retention.Policies) *retention.Sweeper {
	return &retention.Sweeper{DB: db, Policies: p, Batch: 1, Now: func() time.Time { return now }}
}

// Every policy on, both tenants seeded: exactly the rows the policies name go, and the sweep is
// scoped to the tenants it is given. Batch is 1 so the batching loop itself is exercised.
func TestEachPolicyRemovesExactlyItsRowsAndNeverWorkInProgress(t *testing.T) {
	db := pgtest.Open(t)
	seed(t, db, "acme", 1)
	seed(t, db, "globex", 2)

	s := sweeper(db, retention.Policies{
		PublishedOutbox:     &retention.PublishedOutbox{After: 30 * day},
		Inbox:               &retention.Inbox{After: 30 * day},
		IdempotencyResponse: &retention.IdempotencyResponse{ClearAfter: 30 * day},
		IdempotencyKey:      &retention.IdempotencyKey{DeleteAfter: 30 * day},
	})
	res, err := s.Run(context.Background(), []tenant.ID{"acme"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 {
		t.Fatalf("want one result, got %d", len(res))
	}
	r := res[0]
	if r.OutboxDeleted != 1 || r.InboxDeleted != 1 || r.ResponsesCleared != 1 || r.KeysDeleted != 1 || r.StaleInFlight != 1 {
		t.Fatalf("acme: %+v; want one of each and one stale in-flight key reported", r)
	}

	// acme: the published-old outbox row is gone, the unpublished-old one is not.
	if n := count(t, db, `SELECT count(*) FROM outbox WHERE tenant_id = 'acme'`); n != 3 {
		t.Fatalf("acme outbox has %d rows, want 3", n)
	}
	if n := count(t, db, `SELECT count(*) FROM outbox WHERE tenant_id = 'acme' AND published_at IS NULL`); n != 2 {
		t.Fatalf("an unpublished outbox row was deleted: %d left, want 2", n)
	}
	if n := count(t, db, `SELECT count(*) FROM inbox WHERE tenant_id = 'acme'`); n != 1 {
		t.Fatalf("acme inbox has %d rows, want 1", n)
	}
	if n := count(t, db, `SELECT count(*) FROM idempotency_keys WHERE tenant_id = 'acme' AND state = 'in_flight'`); n != 2 {
		t.Fatalf("an in-flight key was deleted: %d left, want 2", n)
	}
	if n := count(t, db, `SELECT count(*) FROM idempotency_keys WHERE tenant_id = 'acme' AND key = 'k-fresh-done' AND response IS NOT NULL`); n != 1 {
		t.Fatal("a fresh completed key lost its response or was deleted")
	}

	// globex was not in the list: untouched.
	if n := count(t, db, `SELECT count(*) FROM outbox WHERE tenant_id = 'globex'`); n != 4 {
		t.Fatalf("globex outbox has %d rows, want 4: the sweep leaked across tenants", n)
	}
	if n := count(t, db, `SELECT count(*) FROM idempotency_keys WHERE tenant_id = 'globex'`); n != 4 {
		t.Fatalf("globex keys %d, want 4", n)
	}
}

// Clearing a response keeps the key, and a later claim is a replay with no response, never a
// second execution.
func TestAClearedResponseStillRefusesTheRetry(t *testing.T) {
	db := pgtest.Open(t)
	seed(t, db, "acme", 1)
	s := sweeper(db, retention.Policies{IdempotencyResponse: &retention.IdempotencyResponse{ClearAfter: 30 * day}})
	if _, err := s.Run(context.Background(), []tenant.ID{"acme"}); err != nil {
		t.Fatal(err)
	}
	if n := count(t, db, `SELECT count(*) FROM idempotency_keys WHERE tenant_id = 'acme'`); n != 4 {
		t.Fatalf("clearing responses deleted keys: %d left, want 4", n)
	}
	ctx := tenant.NewContext(context.Background(), "acme")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	rec, replay, err := idempotency.NewPostgresStore(tx).Claim(ctx, "k-old-done", fp)
	if err != nil || !replay {
		t.Fatalf("replay=%v err=%v; want a replay", replay, err)
	}
	if len(rec.Response) != 0 {
		t.Fatalf("response %q should have been cleared", rec.Response)
	}
}

// A policy that is off touches nothing, even with rows that another policy would remove.
func TestADisabledPolicyTouchesNothing(t *testing.T) {
	db := pgtest.Open(t)
	seed(t, db, "acme", 1)
	s := sweeper(db, retention.Policies{Inbox: &retention.Inbox{After: 30 * day}})
	if _, err := s.Run(context.Background(), []tenant.ID{"acme"}); err != nil {
		t.Fatal(err)
	}
	if n := count(t, db, `SELECT count(*) FROM outbox`); n != 4 {
		t.Fatalf("outbox %d, want 4", n)
	}
	if n := count(t, db, `SELECT count(*) FROM idempotency_keys WHERE response IS NOT NULL`); n != 2 {
		t.Fatalf("responses %d, want 2", n)
	}
}

// BeforeBatch runs in every batch's transaction, once per batch, with the tenant being swept.
func TestBeforeBatchRunsInEveryBatchWithTheTenant(t *testing.T) {
	db := pgtest.Open(t)
	seed(t, db, "acme", 1)
	var seen []string
	s := sweeper(db, retention.Policies{PublishedOutbox: &retention.PublishedOutbox{After: 30 * day}})
	s.BeforeBatch = func(ctx context.Context, tx *sql.Tx, tid tenant.ID) error {
		seen = append(seen, string(tid))
		_, err := tx.ExecContext(ctx, `SELECT set_config('app.tenant_id', $1, true)`, string(tid))
		return err
	}
	if _, err := s.Run(context.Background(), []tenant.ID{"acme"}); err != nil {
		t.Fatal(err)
	}
	// Batch 1 and one deletable row: one batch deletes it, a second finds nothing and stops.
	if len(seen) != 2 || seen[0] != "acme" || seen[1] != "acme" {
		t.Fatalf("BeforeBatch saw %v; want acme twice", seen)
	}
}

// A hook that fails stops the sweep with the error and leaves the rows.
func TestAFailingHookStopsTheSweepAndDeletesNothing(t *testing.T) {
	db := pgtest.Open(t)
	seed(t, db, "acme", 1)
	boom := errors.New("rls setting unavailable")
	s := sweeper(db, retention.Policies{PublishedOutbox: &retention.PublishedOutbox{After: 30 * day}})
	s.BeforeBatch = func(context.Context, *sql.Tx, tenant.ID) error { return boom }
	if _, err := s.Run(context.Background(), []tenant.ID{"acme"}); !errors.Is(err, boom) {
		t.Fatalf("want the hook's error, got %v", err)
	}
	if n := count(t, db, `SELECT count(*) FROM outbox`); n != 4 {
		t.Fatalf("outbox %d, want 4", n)
	}
}
