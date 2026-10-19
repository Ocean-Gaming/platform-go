//go:build integration

package pg_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/Ocean-Gaming/platform-go/idempotency"
	"github.com/Ocean-Gaming/platform-go/inbox"
	"github.com/Ocean-Gaming/platform-go/outbox"
	"github.com/Ocean-Gaming/platform-go/pg"
	"github.com/Ocean-Gaming/platform-go/pgtest"
	"github.com/Ocean-Gaming/platform-go/tenant"
)

func acme() context.Context {
	return tenant.NewContext(context.Background(), "acme")
}

// fp is a well-formed fingerprint: migrations/0003 refuses any other shape.
var fp = idempotency.Fingerprint([]byte("cmd"))

func msg(id string) outbox.Message {
	return outbox.Message{ID: id, Topic: "ActionRecorded", PartitionKey: "player-1", Payload: []byte(`{}`)}
}

// THE test. Rule 4 says the state change and its outbox row commit together or
// not at all. If the transaction fails after both writes, the database must
// contain neither - no idempotency record claiming the command ran, and no
// event announcing something that did not happen.
func TestRollbackLeavesNoIdempotencyRecordAndNoOutboxRow(t *testing.T) {
	db := pgtest.Open(t)
	ctx := acme()
	boom := errors.New("domain failure after both writes")

	err := pg.InTx(ctx, db, func(tx *sql.Tx) error {
		idem := idempotency.NewPostgresStore(tx)
		ob := outbox.NewPostgresWriter(tx)

		if _, replay, err := idem.Claim(ctx, "k-1", fp); err != nil || replay {
			t.Fatalf("claim: replay=%v err=%v", replay, err)
		}
		if err := ob.Write(ctx, msg("11111111-1111-1111-1111-111111111111")); err != nil {
			t.Fatal(err)
		}
		if err := idem.Complete(ctx, "k-1", []byte(`{"ok":true}`)); err != nil {
			t.Fatal(err)
		}
		return boom // the command fails after everything was written
	})
	if !errors.Is(err, boom) {
		t.Fatalf("want the domain error, got %v", err)
	}

	if n := pgtest.Count(t, db, "idempotency_keys"); n != 0 {
		t.Fatalf("rollback left %d idempotency records - a retry would wrongly replay", n)
	}
	if n := pgtest.Count(t, db, "outbox"); n != 0 {
		t.Fatalf("rollback left %d outbox rows - an event announcing work that never happened", n)
	}
}

// The positive case: a committed command leaves exactly one of each.
func TestCommitLeavesExactlyOneOfEach(t *testing.T) {
	db := pgtest.Open(t)
	ctx := acme()

	if err := pg.InTx(ctx, db, func(tx *sql.Tx) error {
		idem := idempotency.NewPostgresStore(tx)
		ob := outbox.NewPostgresWriter(tx)
		if _, _, err := idem.Claim(ctx, "k-1", fp); err != nil {
			return err
		}
		if err := ob.Write(ctx, msg("22222222-2222-2222-2222-222222222222")); err != nil {
			return err
		}
		return idem.Complete(ctx, "k-1", []byte(`{"ok":true}`))
	}); err != nil {
		t.Fatal(err)
	}

	if n := pgtest.Count(t, db, "idempotency_keys"); n != 1 {
		t.Fatalf("idempotency_keys=%d want 1", n)
	}
	if n := pgtest.Count(t, db, "outbox"); n != 1 {
		t.Fatalf("outbox=%d want 1", n)
	}
}

// A panic must roll back too. A command that panics halfway must not leave a
// record claiming it succeeded.
func TestPanicRollsBack(t *testing.T) {
	db := pgtest.Open(t)
	ctx := acme()

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected the panic to propagate")
			}
		}()
		_ = pg.InTx(ctx, db, func(tx *sql.Tx) error {
			idem := idempotency.NewPostgresStore(tx)
			if _, _, err := idem.Claim(ctx, "k-1", fp); err != nil {
				return err
			}
			panic("nil map write")
		})
	}()

	if n := pgtest.Count(t, db, "idempotency_keys"); n != 0 {
		t.Fatalf("panic left %d idempotency records", n)
	}
}

// Rule 3 under real concurrency: of N simultaneous commands sharing one key,
// exactly one may execute. The rest must be rejected or replay - never execute.
//
// Every worker's outcome is recorded, including the commit. Discarding them
// counted a database error as a rejection, so a run in which every claim failed
// with 22P02 or a dropped connection passed as "dedup worked".
func TestConcurrentSameKeyExecutesExactlyOnce(t *testing.T) {
	db := pgtest.Open(t)
	ctx := acme()
	const n = 16
	stored := []byte(`{"action_id":"a-1"}`)

	type outcome struct {
		executed bool
		replayed []byte // the replayed response, when the claim replayed
		conflict bool   // ErrInFlight: the expected, retryable refusal
		err      error  // anything else, the commit included
	}
	outcomes := make([]outcome, n)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			o := &outcomes[i]
			err := pg.InTx(ctx, db, func(tx *sql.Tx) error {
				idem := idempotency.NewPostgresStore(tx)
				rec, replay, err := idem.Claim(ctx, "shared-key", idempotency.Fingerprint([]byte("cmd")))
				switch {
				case errors.Is(err, idempotency.ErrInFlight):
					o.conflict = true
					return nil
				case err != nil:
					return err
				case replay:
					o.replayed = rec.Response
					return nil
				}
				if err := outbox.NewPostgresWriter(tx).Write(ctx, msg(fmt.Sprintf("aaaaaaaa-0000-4000-8000-%012d", i))); err != nil {
					return err
				}
				if err := idem.Complete(ctx, "shared-key", stored); err != nil {
					return err
				}
				o.executed = true
				return nil
			})
			if err != nil {
				// A failed commit means the work did not happen, whatever fn reported.
				o.executed = false
				o.err = err
			}
		}(i)
	}
	wg.Wait()

	var executed, replayed, conflicts int
	for i, o := range outcomes {
		switch {
		case o.err != nil:
			t.Errorf("worker %d: %v (a database error is not a dedup)", i, o.err)
		case o.executed:
			executed++
		case o.replayed != nil:
			replayed++
			if !bytes.Equal(o.replayed, stored) {
				t.Errorf("worker %d replayed %q, want %q", i, o.replayed, stored)
			}
		case o.conflict:
			conflicts++
		default:
			t.Errorf("worker %d neither executed, replayed nor conflicted", i)
		}
	}
	if executed != 1 {
		t.Fatalf("%d of %d concurrent commands executed; exactly 1 may", executed, n)
	}
	if replayed+conflicts != n-1 {
		t.Fatalf("replayed=%d conflicts=%d, want %d between them", replayed, conflicts, n-1)
	}

	// The business effect, counted after every commit.
	if got := pgtest.Count(t, db, "outbox"); got != 1 {
		t.Fatalf("outbox has %d rows; a duplicate event is a duplicated bet", got)
	}
	var state string
	var response []byte
	if err := db.QueryRow(`SELECT state, response FROM idempotency_keys WHERE tenant_id = 'acme' AND key = 'shared-key'`).
		Scan(&state, &response); err != nil {
		t.Fatal(err)
	}
	if state != "completed" || !bytes.Equal(response, stored) {
		t.Fatalf("key is %s with %q; want completed with %q", state, response, stored)
	}
}

// Rule 5 under real concurrency: exactly one delivery may win.
func TestConcurrentInboxAdmitsExactlyOneWinner(t *testing.T) {
	db := pgtest.Open(t)
	ctx := acme()
	const n = 16

	// A real UUID: the inbox.event_id column is typed UUID, so a placeholder
	// like "evt-1" makes every insert error rather than dedup.
	const eventID = "33333333-3333-3333-3333-333333333333"

	var (
		mu      sync.Mutex
		handled int
		errs    []error
	)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Collect errors rather than discarding them. Swallowing them here
			// makes a hard failure look identical to "dedup worked", which is
			// exactly the bug this test is supposed to catch.
			if err := pg.InTx(ctx, db, func(tx *sql.Tx) error {
				return inbox.Handle(ctx, inbox.NewPostgresStore(tx), "consumer-a", eventID,
					func(context.Context) error {
						mu.Lock()
						handled++
						mu.Unlock()
						return nil
					})
			}); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(errs) != 0 {
		t.Fatalf("%d of %d deliveries errored, first: %v", len(errs), n, errs[0])
	}
	if handled != 1 {
		t.Fatalf("handler ran %d times for one event id; want exactly 1", handled)
	}
	if got := pgtest.Count(t, db, "inbox"); got != 1 {
		t.Fatalf("inbox has %d rows for one event; want 1", got)
	}
}

// Cross-tenant isolation, against a real unique constraint rather than a map.
func TestSameKeyDifferentTenantsBothExecute(t *testing.T) {
	db := pgtest.Open(t)

	for _, tid := range []tenant.ID{"acme", "globex"} {
		ctx := tenant.NewContext(context.Background(), tid)
		if err := pg.InTx(ctx, db, func(tx *sql.Tx) error {
			idem := idempotency.NewPostgresStore(tx)
			_, replay, err := idem.Claim(ctx, "shared-key", fp)
			if err != nil {
				return err
			}
			if replay {
				t.Fatalf("tenant %s replayed another tenant's result", tid)
			}
			return idem.Complete(ctx, "shared-key", []byte(`{}`))
		}); err != nil {
			t.Fatalf("tenant %s: %v", tid, err)
		}
	}
	if n := pgtest.Count(t, db, "idempotency_keys"); n != 2 {
		t.Fatalf("want one record per tenant, got %d", n)
	}
}

// The inbox twin of the test above, and the one whose absence let a real bug
// ship: inbox.TestDedupIsScopedPerTenant asserts this invariant but can only
// ever exercise MemoryStore, which keyed by tenant while the SQL did not. Only
// a real Postgres proves the constraint and the ON CONFLICT arbiter agree.
func TestSameEventIdDifferentTenantsBothRun(t *testing.T) {
	db := pgtest.Open(t)

	const eventID = "8f14e45f-ea18-4a4b-8f3a-1c2d3e4f5a6b" // the SAME id for both
	runs := 0
	for _, tid := range []tenant.ID{"acme", "globex"} {
		ctx := tenant.NewContext(context.Background(), tid)
		if err := pg.InTx(ctx, db, func(tx *sql.Tx) error {
			return inbox.Handle(ctx, inbox.NewPostgresStore(tx), "consumer-a", eventID, func(context.Context) error {
				runs++
				return nil
			})
		}); err != nil {
			t.Fatalf("tenant %s: %v", tid, err)
		}
	}
	// A tenant-blind dedup key makes the second Handle a silent no-op: no
	// error, no row, no handler — the event is simply lost for that tenant.
	if runs != 2 {
		t.Fatalf("one tenant suppressed another's event: handler ran %d times, want 2", runs)
	}
	if n := pgtest.Count(t, db, "inbox"); n != 2 {
		t.Fatalf("want one inbox row per tenant, got %d", n)
	}

	// And dedup must still work WITHIN a tenant, or the fix traded one bug for
	// a worse one: a redelivered BetSettled crediting a player twice.
	ctx := tenant.NewContext(context.Background(), tenant.ID("acme"))
	if err := pg.InTx(ctx, db, func(tx *sql.Tx) error {
		return inbox.Handle(ctx, inbox.NewPostgresStore(tx), "consumer-a", eventID, func(context.Context) error {
			runs++
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	if runs != 2 {
		t.Fatalf("redelivery to the same tenant re-ran the handler: %d runs", runs)
	}
}

// Racing Completes of one committed in-flight key, each in its own transaction.
// They serialise on the row lock; the one that waits must find the key already
// completed and refuse, not overwrite the answer the first caller was given.
func TestConcurrentCompletesKeepTheFirstResponse(t *testing.T) {
	db := pgtest.Open(t)
	ctx := acme()
	if err := pg.InTx(ctx, db, func(tx *sql.Tx) error {
		_, _, err := idempotency.NewPostgresStore(tx).Claim(ctx, "k-race", idempotency.Fingerprint([]byte("cmd")))
		return err
	}); err != nil {
		t.Fatal(err)
	}

	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = pg.InTx(ctx, db, func(tx *sql.Tx) error {
				return idempotency.NewPostgresStore(tx).Complete(ctx, "k-race", []byte(fmt.Sprintf(`{"n":%d}`, i)))
			})
		}(i)
	}
	wg.Wait()

	winner := -1
	for i, err := range errs {
		switch {
		case err == nil:
			if winner != -1 {
				t.Fatalf("workers %d and %d both completed the key", winner, i)
			}
			winner = i
		case !errors.Is(err, idempotency.ErrAlreadyCompleted):
			t.Fatalf("worker %d: want ErrAlreadyCompleted, got %v", i, err)
		}
	}
	if winner == -1 {
		t.Fatal("no Complete succeeded")
	}
	var response []byte
	if err := db.QueryRow(`SELECT response FROM idempotency_keys WHERE tenant_id = 'acme' AND key = 'k-race'`).Scan(&response); err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf(`{"n":%d}`, winner); string(response) != want {
		t.Fatalf("stored %q, want the winner's %q", response, want)
	}
}
