// Package txn is the seam that makes rules 3, 4 and 5 compose.
//
// Each of those rules is individually free: idempotency.Store claims a key,
// outbox.Writer writes the event, inbox.Store dedups the delivery. What is NOT
// free is that they must all happen in ONE transaction — "we handled it" and
// "the handling took effect" have to be a single atomic fact, or a crash
// between them either double-credits a player or loses the event announcing a
// credit that did happen.
//
// Nothing in the type system used to enforce that. The Postgres stores take a
// pg.DBTX, and *sql.DB satisfies pg.DBTX, so wiring
//
//	Idempotency: idempotency.NewPostgresStore(db)   // <- compiles, and is wrong
//
// produced three autocommit statements that look exactly like one transaction
// until the process dies between them. A service could hold every rule and
// still lose money.
//
// A Runner closes that: stores exist only INSIDE the unit of work, bound to its
// transaction, so there is no way to hold one across a commit boundary.
package txn

import (
	"context"
	"database/sql"

	"github.com/Ocean-Gaming/platform-go/idempotency"
	"github.com/Ocean-Gaming/platform-go/inbox"
	"github.com/Ocean-Gaming/platform-go/outbox"
	"github.com/Ocean-Gaming/platform-go/pg"
)

// Stores are the platform stores for one unit of work. They share its
// transaction and must not outlive it.
type Stores struct {
	Idempotency idempotency.Store
	Outbox      outbox.Writer
	Inbox       inbox.Store

	// Tx is the same transaction, for the service's own domain writes. Nil for
	// the in-memory runner, which is why a test that needs real domain writes
	// belongs in the integration suite.
	Tx *sql.Tx
}

// Runner executes fn as one atomic unit. Everything fn writes commits together
// or not at all.
type Runner interface {
	Do(ctx context.Context, fn func(Stores) error) error
}

// Postgres binds every store to a single transaction, committed when fn returns
// nil and rolled back on error or panic.
func Postgres(db *sql.DB) Runner { return pgRunner{db: db} }

type pgRunner struct{ db *sql.DB }

func (r pgRunner) Do(ctx context.Context, fn func(Stores) error) error {
	return pg.InTx(ctx, r.db, func(tx *sql.Tx) error {
		return fn(Stores{
			Idempotency: idempotency.NewPostgresStore(tx),
			Outbox:      outbox.NewPostgresWriter(tx),
			Inbox:       inbox.NewPostgresStore(tx),
			Tx:          tx,
		})
	})
}

// Memory is the unit-test runner. The stores persist across calls, so a retry
// in a test sees the previous call's claim — otherwise every test would
// trivially "pass" idempotency by forgetting.
//
// It does NOT roll back on error: the fakes have no transaction to undo. A test
// that needs to prove atomicity needs a real database, which is the point of
// the integration suite, and is why the inbox bug could ship green.
func Memory() Runner {
	return &memRunner{
		idem: idempotency.NewMemoryStore(),
		ob:   outbox.NewMemoryOutbox(),
		in:   inbox.NewMemoryStore(),
	}
}

type memRunner struct {
	idem *idempotency.MemoryStore
	ob   *outbox.MemoryOutbox
	in   *inbox.MemoryStore
}

func (r *memRunner) Do(_ context.Context, fn func(Stores) error) error {
	return fn(Stores{Idempotency: r.idem, Outbox: r.ob, Inbox: r.in})
}

// Outbox exposes the in-memory outbox so tests can assert on what was emitted.
// Present only on the memory runner, deliberately: production code that reaches
// for it will not compile against a Runner.
func (r *memRunner) Outbox() *outbox.MemoryOutbox { return r.ob }
