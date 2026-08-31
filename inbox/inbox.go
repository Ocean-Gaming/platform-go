// Package inbox implements platform rule 5: consumers dedup on event ID with an
// inbox in their own transaction.
//
// Delivery from the bus is at-least-once. Without this, a redelivered
// BetSettled credits a player twice. The dedup insert and the handler's own
// writes must share one transaction, so "we handled it" and "the handling took
// effect" are a single atomic fact.
package inbox

import (
	"context"
	"sync"

	"github.com/Ocean-Gaming/platform-go/tenant"
)

// Store records which events a consumer has handled.
type Store interface {
	// MarkProcessed records (tenant, consumer, eventID) and reports whether this
	// is the first time. Implementations must make the check-and-insert atomic;
	// a read-then-write race here reintroduces double processing.
	//
	// eventID must be a UUID. It is typed as string because that is what the
	// event envelope carries, but the Postgres store writes it to a UUID column
	// and a non-UUID value fails at runtime with SQLSTATE 22P02 — after passing
	// every test against the in-memory store, which accepts any string.
	MarkProcessed(ctx context.Context, consumer, eventID string) (first bool, err error)
}

// Handle runs fn exactly once per (consumer, eventID).
//
// If the event was already processed it returns nil without running fn — a
// redelivery is a no-op, not an error the consumer has to reason about.
func Handle(ctx context.Context, s Store, consumer, eventID string, fn func(context.Context) error) error {
	if _, err := tenant.Require(ctx); err != nil {
		return err
	}
	first, err := s.MarkProcessed(ctx, consumer, eventID)
	if err != nil {
		return err
	}
	if !first {
		return nil
	}
	return fn(ctx)
}

// MemoryStore is an in-process Store for unit tests.
type MemoryStore struct {
	mu   sync.Mutex
	seen map[string]bool
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{seen: map[string]bool{}} }

func (m *MemoryStore) MarkProcessed(ctx context.Context, consumer, eventID string) (bool, error) {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return false, err
	}
	k := string(tid) + "\x00" + consumer + "\x00" + eventID
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.seen[k] {
		return false, nil
	}
	m.seen[k] = true
	return true, nil
}
