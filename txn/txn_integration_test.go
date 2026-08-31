//go:build integration

package txn_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Ocean-Gaming/platform-go/idempotency"
	"github.com/Ocean-Gaming/platform-go/outbox"
	"github.com/Ocean-Gaming/platform-go/pgtest"
	"github.com/Ocean-Gaming/platform-go/tenant"
	"github.com/Ocean-Gaming/platform-go/txn"
)

// The property the seam exists for, and the one a service could previously
// break by wiring the stores against *sql.DB: a command that fails partway
// leaves NO idempotency record and NO outbox row. A retry must not find a claim
// saying the work happened, and no event may announce work that was rolled back.
func TestFailureLeavesNothingBehind(t *testing.T) {
	db := pgtest.Open(t)
	ctx := tenant.NewContext(context.Background(), "acme")
	r := txn.Postgres(db)

	boom := errors.New("domain exploded after both writes")
	err := r.Do(ctx, func(s txn.Stores) error {
		if _, _, err := s.Idempotency.Claim(ctx, "k", idempotency.Fingerprint([]byte("{}"))); err != nil {
			return err
		}
		if err := s.Outbox.Write(ctx, outbox.Message{
			ID: "44444444-4444-4444-8444-444444444444", Tenant: "acme",
			Topic: "ActionRecorded", PartitionKey: "p", Payload: []byte(`{}`),
			OccurredAt: time.Now().UTC(),
		}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("want the domain error back, got %v", err)
	}
	if n := pgtest.Count(t, db, "idempotency_keys"); n != 0 {
		t.Fatalf("a failed unit left %d idempotency records", n)
	}
	if n := pgtest.Count(t, db, "outbox"); n != 0 {
		t.Fatalf("a failed unit left %d outbox rows — an event for work that was rolled back", n)
	}
}

// A panic must roll back the same way; pg.InTx re-panics after rollback so the
// recovery interceptor still turns it into a scrubbed INTERNAL.
func TestPanicRollsBackTheWholeUnit(t *testing.T) {
	db := pgtest.Open(t)
	ctx := tenant.NewContext(context.Background(), "acme")
	r := txn.Postgres(db)

	func() {
		defer func() {
			if recover() == nil {
				t.Error("the panic was swallowed instead of re-raised")
			}
		}()
		_ = r.Do(ctx, func(s txn.Stores) error {
			_, _, _ = s.Idempotency.Claim(ctx, "k-panic", idempotency.Fingerprint([]byte("{}")))
			panic("mid-command")
		})
	}()

	if n := pgtest.Count(t, db, "idempotency_keys"); n != 0 {
		t.Fatalf("a panicking unit left %d idempotency records", n)
	}
}

// And the success path commits everything together.
func TestSuccessCommitsBothTogether(t *testing.T) {
	db := pgtest.Open(t)
	ctx := tenant.NewContext(context.Background(), "acme")

	if err := txn.Postgres(db).Do(ctx, func(s txn.Stores) error {
		if _, _, err := s.Idempotency.Claim(ctx, "k-ok", idempotency.Fingerprint([]byte("{}"))); err != nil {
			return err
		}
		if err := s.Outbox.Write(ctx, outbox.Message{
			ID: "55555555-5555-4555-8555-555555555555", Tenant: "acme",
			Topic: "ActionRecorded", PartitionKey: "p", Payload: []byte(`{}`),
			OccurredAt: time.Now().UTC(),
		}); err != nil {
			return err
		}
		return s.Idempotency.Complete(ctx, "k-ok", []byte(`{"ok":true}`))
	}); err != nil {
		t.Fatal(err)
	}
	if n := pgtest.Count(t, db, "idempotency_keys"); n != 1 {
		t.Fatalf("want 1 idempotency record, got %d", n)
	}
	if n := pgtest.Count(t, db, "outbox"); n != 1 {
		t.Fatalf("want exactly 1 outbox row, got %d", n)
	}
}
