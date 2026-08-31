// Package platformtest exports one set of test bodies that every implementation
// of the platform's storage interfaces must satisfy.
//
// It exists because of a bug that actually shipped. inbox.MemoryStore keyed its
// dedup on tenant+consumer+event; the SQL PRIMARY KEY omitted tenant_id. The
// unit test asserting cross-tenant isolation passed — against the map — while
// production silently dropped one tenant's event. Two implementations of one
// interface disagreed, and nothing could see it, because each was tested alone.
//
// Run this against BOTH: the fake unconditionally, the real store under the
// integration tag. A conformance failure is the divergence, caught before merge.
package platformtest

import (
	"context"
	"testing"

	"github.com/Ocean-Gaming/platform-go/idempotency"
	"github.com/Ocean-Gaming/platform-go/inbox"
	"github.com/Ocean-Gaming/platform-go/tenant"
)

// Harness binds one implementation into a unit of work. For Postgres, Do opens
// a transaction and commits it; for the in-memory fakes it simply calls fn. The
// stores are constructed INSIDE Do so the Postgres pair share one tx, which is
// the property rules 3 and 4 depend on.
type Harness struct {
	Name string
	Do   func(ctx context.Context, fn func(idempotency.Store, inbox.Store) error) error
}

const (
	acme   = tenant.ID("acme")
	globex = tenant.ID("globex") // rule 8 — never fewer than two, including here

	// Event ids are UUIDs. The Store interface types them as string, but the
	// inbox.event_id column is UUID, so a non-UUID id passes every unit test
	// against the fake and fails in production with SQLSTATE 22P02. This suite
	// found that; the constraint is documented on inbox.Store.
	evtDup    = "3f2504e0-4f89-41d3-9a0c-0305e82c3301"
	evtShared = "3f2504e0-4f89-41d3-9a0c-0305e82c3302"
	evtOther  = "3f2504e0-4f89-41d3-9a0c-0305e82c3303"
)

// RunConformance runs every invariant both implementations must uphold.
func RunConformance(t *testing.T, h Harness) {
	t.Helper()
	t.Run(h.Name+"/idempotency", func(t *testing.T) { idempotencyConformance(t, h) })
	t.Run(h.Name+"/inbox", func(t *testing.T) { inboxConformance(t, h) })
}

func ctxFor(id tenant.ID) context.Context {
	return tenant.NewContext(context.Background(), id)
}

func idempotencyConformance(t *testing.T, h Harness) {
	fp := idempotency.Fingerprint([]byte(`{"amount":100}`))
	stored := []byte(`{"action_id":"a-1"}`)

	// Rule 3 — first claim executes, and the completed result replays verbatim.
	t.Run("claim then replay returns the stored answer", func(t *testing.T) {
		ctx := ctxFor(acme)
		if err := h.Do(ctx, func(idem idempotency.Store, _ inbox.Store) error {
			_, replay, err := idem.Claim(ctx, "k-replay", fp)
			if err != nil {
				return err
			}
			if replay {
				t.Fatal("a first claim reported itself as a replay")
			}
			return idem.Complete(ctx, "k-replay", stored)
		}); err != nil {
			t.Fatal(err)
		}
		if err := h.Do(ctx, func(idem idempotency.Store, _ inbox.Store) error {
			rec, replay, err := idem.Claim(ctx, "k-replay", fp)
			if err != nil {
				return err
			}
			if !replay {
				t.Fatal("a retry re-executed instead of replaying")
			}
			if string(rec.Response) != string(stored) {
				t.Fatalf("replayed answer differs: %q want %q", rec.Response, stored)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	// The same key with a DIFFERENT body is a conflict, never a replay of the
	// other command's answer — that would return success for work never done.
	t.Run("same key different fingerprint conflicts", func(t *testing.T) {
		ctx := ctxFor(acme)
		if err := h.Do(ctx, func(idem idempotency.Store, _ inbox.Store) error {
			_, _, err := idem.Claim(ctx, "k-fp", fp)
			if err != nil {
				return err
			}
			return idem.Complete(ctx, "k-fp", stored)
		}); err != nil {
			t.Fatal(err)
		}
		err := h.Do(ctx, func(idem idempotency.Store, _ inbox.Store) error {
			_, _, err := idem.Claim(ctx, "k-fp", idempotency.Fingerprint([]byte(`{"amount":999}`)))
			return err
		})
		if err == nil {
			t.Fatal("a reused key with a different body was accepted")
		}
	})

	// Rule 2 — one tenant must never replay another's result.
	t.Run("same key across tenants stays independent", func(t *testing.T) {
		for _, tid := range []tenant.ID{acme, globex} {
			ctx := ctxFor(tid)
			if err := h.Do(ctx, func(idem idempotency.Store, _ inbox.Store) error {
				_, replay, err := idem.Claim(ctx, "k-shared", fp)
				if err != nil {
					return err
				}
				if replay {
					t.Fatalf("tenant %s replayed another tenant's result", tid)
				}
				return idem.Complete(ctx, "k-shared", stored)
			}); err != nil {
				t.Fatalf("tenant %s: %v", tid, err)
			}
		}
	})

	// Rule 2 — no tenant, no command.
	t.Run("a claim with no tenant is refused", func(t *testing.T) {
		err := h.Do(context.Background(), func(idem idempotency.Store, _ inbox.Store) error {
			_, _, err := idem.Claim(context.Background(), "k-no-tenant", fp)
			return err
		})
		if err == nil {
			t.Fatal("a claim with no tenant in context was accepted")
		}
	})
}

func inboxConformance(t *testing.T, h Harness) {
	// Rule 5 — a redelivery must not run the handler twice.
	t.Run("redelivery to the same tenant runs the handler once", func(t *testing.T) {
		ctx := ctxFor(acme)
		runs := 0
		for i := 0; i < 2; i++ {
			if err := h.Do(ctx, func(_ idempotency.Store, in inbox.Store) error {
				return inbox.Handle(ctx, in, "consumer-a", evtDup, func(context.Context) error {
					runs++
					return nil
				})
			}); err != nil {
				t.Fatal(err)
			}
		}
		if runs != 1 {
			t.Fatalf("handler ran %d times for one event; want 1", runs)
		}
	})

	// THE regression. A tenant-blind dedup key makes the second delivery a
	// silent no-op: no error, no row, no handler, and the event is simply lost
	// for that tenant. This is the body the fake passed and the SQL failed.
	t.Run("the same event id across tenants runs both handlers", func(t *testing.T) {
		runs := 0
		for _, tid := range []tenant.ID{acme, globex} {
			ctx := ctxFor(tid)
			if err := h.Do(ctx, func(_ idempotency.Store, in inbox.Store) error {
				return inbox.Handle(ctx, in, "consumer-b", evtShared, func(context.Context) error {
					runs++
					return nil
				})
			}); err != nil {
				t.Fatalf("tenant %s: %v", tid, err)
			}
		}
		if runs != 2 {
			t.Fatalf("one tenant suppressed another's event: handler ran %d times, want 2", runs)
		}
	})

	// Rule 2 — an event with no tenant is not dedupable, so it does not run.
	t.Run("handling with no tenant is refused", func(t *testing.T) {
		err := h.Do(context.Background(), func(_ idempotency.Store, in inbox.Store) error {
			return inbox.Handle(context.Background(), in, "c", evtOther, func(context.Context) error {
				t.Error("handler ran with no tenant in context")
				return nil
			})
		})
		if err == nil {
			t.Fatal("an event with no tenant was accepted")
		}
	})
}
