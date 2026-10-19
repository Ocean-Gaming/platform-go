//go:build integration

package outbox_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/Ocean-Gaming/platform-go/outbox"
	"github.com/Ocean-Gaming/platform-go/pg"
	"github.com/Ocean-Gaming/platform-go/pgtest"
	"github.com/Ocean-Gaming/platform-go/tenant"
)

// The delivery half of rule 4. The WRITE half was proven; PostgresReader was
// constructed by nothing and executed by no test in any repo, so the relay's
// actual SQL — the ORDER BY, the published_at filter, the batch UPDATE — had
// never run outside production.
func TestRelayRoundTripOverRealSQL(t *testing.T) {
	db := pgtest.Open(t)
	ctx := tenant.NewContext(context.Background(), "acme")

	base := time.Now().UTC().Add(-time.Hour)
	// Written newest-first on purpose: the reader must re-order them.
	for i, id := range []string{
		"11111111-1111-4111-8111-111111111111",
		"22222222-2222-4222-8222-222222222222",
		"33333333-3333-4333-8333-333333333333",
	} {
		occurred := base.Add(time.Duration(3-i) * time.Minute)
		if err := pg.InTx(ctx, db, func(tx *sql.Tx) error {
			return outbox.NewPostgresWriter(tx).Write(ctx, outbox.Message{
				ID: id, Tenant: "acme", Topic: "ActionRecorded",
				PartitionKey: id, Payload: []byte(`{}`), OccurredAt: occurred,
				Headers: map[string]string{"tenant_id": "acme"},
			})
		}); err != nil {
			t.Fatal(err)
		}
	}

	r := outbox.NewPostgresReader(db)
	msgs, err := r.Unpublished(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 {
		t.Fatalf("want 3 unpublished, got %d", len(msgs))
	}
	// Oldest first, or a consumer sees a settle before its bet.
	for i := 1; i < len(msgs); i++ {
		if msgs[i].OccurredAt.Before(msgs[i-1].OccurredAt) {
			t.Fatalf("relay returned events out of order: %v before %v",
				msgs[i-1].OccurredAt, msgs[i].OccurredAt)
		}
	}
	// Rule 2 — the header survives the round trip, not just the payload.
	if msgs[0].Headers["tenant_id"] != "acme" {
		t.Fatalf("tenant_id header lost in the round trip: %+v", msgs[0].Headers)
	}

	ids := []string{msgs[0].ID, msgs[1].ID}
	if err := r.MarkPublished(ctx, ids); err != nil {
		t.Fatal(err)
	}
	left, err := r.Unpublished(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 1 || left[0].ID != msgs[2].ID {
		t.Fatalf("MarkPublished did not mark exactly the batch: %d left", len(left))
	}

	// Marking is idempotent: a relay that crashes between publish and mark
	// re-marks on restart, and must not error.
	if err := r.MarkPublished(ctx, ids); err != nil {
		t.Fatalf("re-marking an already published batch failed: %v", err)
	}
	if err := r.MarkPublished(ctx, nil); err != nil {
		t.Fatalf("marking an empty batch failed: %v", err)
	}

	// A mark that misses a row is an error: the relay follows a full batch at
	// once, and a silent miss would republish the same rows in a loop.
	err = r.MarkPublished(ctx, []string{msgs[2].ID, "99999999-9999-4999-8999-999999999999"})
	if !errors.Is(err, outbox.ErrNotMarked) {
		t.Fatalf("marking an id with no row: want ErrNotMarked, got %v", err)
	}
}

// The same three rules as the memory outbox, against real SQL: another
// tenant's message is refused, a bad batch inserts nothing even when the
// caller commits anyway, and the caller's headers are left alone.
func TestPostgresWriterChecksTheWholeBatchFirst(t *testing.T) {
	db := pgtest.Open(t)
	ctx := tenant.NewContext(context.Background(), "acme")
	good := outbox.Message{ID: "44444444-4444-4444-8444-444444444444", Topic: "ActionRecorded", PartitionKey: "p", Payload: []byte(`{}`)}

	other := good
	other.Tenant = "globex"
	shared := map[string]string{"source": "test"}
	good.Headers = shared

	var mismatch, batch error
	if err := pg.InTx(ctx, db, func(tx *sql.Tx) error {
		w := outbox.NewPostgresWriter(tx)
		mismatch = w.Write(ctx, other)
		batch = w.Write(ctx, good, outbox.Message{ID: "55555555-5555-4555-8555-555555555555"})
		return nil // commit regardless: nothing may have been inserted
	}); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(mismatch, outbox.ErrTenantMismatch) {
		t.Fatalf("want ErrTenantMismatch, got %v", mismatch)
	}
	if !errors.Is(batch, outbox.ErrNoTopic) {
		t.Fatalf("want ErrNoTopic, got %v", batch)
	}
	if n := pgtest.Count(t, db, "outbox"); n != 0 {
		t.Fatalf("%d rows inserted from refused writes", n)
	}
	if _, ok := shared["tenant_id"]; ok {
		t.Fatalf("the caller's headers were modified: %v", shared)
	}
}
