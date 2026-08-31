// Package pg holds the tiny database surface the platform stores share.
package pg

import (
	"context"
	"database/sql"
)

// DBTX is satisfied by both *sql.DB and *sql.Tx.
//
// Every platform store is constructed around one of these rather than opening
// its own connection. That is what lets a store participate in the caller's
// transaction — a store that dials out on its own defeats the outbox pattern
// entirely, because its write would commit independently of the state change
// it is supposed to be atomic with.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// InTx runs fn inside a transaction, rolling back on error or panic.
//
// The rollback-on-panic matters: a panic midway through a command must not
// leave an idempotency record claiming the command ran.
func InTx(ctx context.Context, db *sql.DB, fn func(tx *sql.Tx) error) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r)
		}
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
