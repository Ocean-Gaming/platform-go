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
	"errors"
	"sync"
	"unicode/utf8"

	"github.com/Ocean-Gaming/platform-go/tenant"
)

var (
	// ErrInvalidEventID means the event id is empty or longer than MaxLength characters. An
	// empty id would dedup every id-less event of a consumer into one, so the second is lost.
	ErrInvalidEventID = errors.New("inbox: event id is empty or longer than 255 characters")

	// ErrInvalidConsumer means the consumer name is empty or longer than MaxLength characters.
	ErrInvalidConsumer = errors.New("inbox: consumer is empty or longer than 255 characters")
)

// MaxLength is the longest consumer name and event id accepted, in characters. migrations/0003
// holds both columns to it. The id's format is never checked (meta-repo ADR-0061).
const MaxLength = 255

// check applies the rules migrations/0003 enforces in Postgres, so both stores refuse the same
// deliveries.
func check(consumer, eventID string) error {
	if n := utf8.RuneCountInString(consumer); n == 0 || n > MaxLength {
		return ErrInvalidConsumer
	}
	if n := utf8.RuneCountInString(eventID); n == 0 || n > MaxLength {
		return ErrInvalidEventID
	}
	return nil
}

// Store records which events a consumer has handled.
type Store interface {
	// MarkProcessed records (tenant, consumer, eventID) and reports whether this
	// is the first time. Implementations must make the check-and-insert atomic;
	// a read-then-write race here reintroduces double processing.
	//
	// eventID is the envelope's event_id as the producer's schema defines it:
	// a UUID for most, an `evt_`-prefixed ULID for bonus-engine. Any stable
	// string dedups; the Postgres column is TEXT since migration 0002 (meta-repo
	// ADR-0061). Before that it was UUID, and every non-UUID id failed with
	// SQLSTATE 22P02 after passing every test against the in-memory store.
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
	if err := check(consumer, eventID); err != nil {
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
