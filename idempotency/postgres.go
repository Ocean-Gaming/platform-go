package idempotency

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/Ocean-Gaming/platform-go/tenant"
)

// PostgresStore is an idempotency Store bound to one unit of work.
//
// It takes the caller's *sql.Tx, and only that, so the idempotency record and
// the state change it guards commit or roll back together. It used to take a
// pg.DBTX, which *sql.DB also satisfies: a store built on the pool compiled and
// wrote its record in autocommit, apart from the work it guarded.
type PostgresStore struct{ db *sql.Tx }

func NewPostgresStore(tx *sql.Tx) *PostgresStore { return &PostgresStore{db: tx} }

// Claim reserves the key. The INSERT ... ON CONFLICT DO NOTHING plus the
// follow-up SELECT is what makes the check-and-claim atomic: two concurrent
// requests cannot both see "no existing record".
func (s *PostgresStore) Claim(ctx context.Context, key Key, fingerprint string) (Record, bool, error) {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return Record{}, false, err
	}

	res, err := s.db.ExecContext(ctx, `
		INSERT INTO idempotency_keys (tenant_id, key, fingerprint, state, created_at)
		VALUES ($1, $2, $3, 'in_flight', now())
		ON CONFLICT (tenant_id, key) DO NOTHING`,
		string(tid), string(key), fingerprint)
	if err != nil {
		return Record{}, false, err
	}
	if n, err := res.RowsAffected(); err == nil && n == 1 {
		return Record{Tenant: tid, Key: key, Fingerprint: fingerprint,
			State: StateInFlight, CreatedAt: time.Now()}, false, nil
	}

	// Someone already holds the key. Decide whether this is a legitimate replay.
	var (
		fp        string
		state     string
		response  []byte
		created   time.Time
		completed sql.NullTime
	)
	err = s.db.QueryRowContext(ctx, `
		SELECT fingerprint, state, response, created_at, completed_at
		  FROM idempotency_keys WHERE tenant_id = $1 AND key = $2`,
		string(tid), string(key)).Scan(&fp, &state, &response, &created, &completed)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Record{}, false, ErrNotClaimed
		}
		return Record{}, false, err
	}

	if fp != fingerprint {
		return Record{}, false, ErrFingerprintMismatch
	}
	if State(state) == StateInFlight {
		return Record{}, false, ErrInFlight
	}
	rec := Record{
		Tenant: tid, Key: key, Fingerprint: fp, Response: response,
		State: StateCompleted, CreatedAt: created,
	}
	if completed.Valid {
		rec.CompletedAt = completed.Time
	}
	return rec, true, nil
}

// Complete moves an in-flight key to completed. The state guard is what keeps
// the first response: without it a second Complete overwrote the stored answer,
// and every later replay returned something the first caller was never told.
//
// Two transactions completing the same key serialise on its row lock. The one
// that waits re-evaluates the WHERE after the other commits, finds the key
// completed, and falls through to the comparison below.
func (s *PostgresStore) Complete(ctx context.Context, key Key, response []byte) error {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE idempotency_keys
		   SET response = $3, state = 'completed', completed_at = now()
		 WHERE tenant_id = $1 AND key = $2 AND state = 'in_flight'`,
		string(tid), string(key), response)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 1 {
		return nil
	}

	// Not in flight: either never claimed, or already completed.
	var (
		state  string
		stored []byte
	)
	err = s.db.QueryRowContext(ctx, `
		SELECT state, response FROM idempotency_keys WHERE tenant_id = $1 AND key = $2`,
		string(tid), string(key)).Scan(&state, &stored)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotClaimed
	}
	if err != nil {
		return err
	}
	if State(state) == StateCompleted && bytes.Equal(stored, response) {
		return nil
	}
	return ErrAlreadyCompleted
}
