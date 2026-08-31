package idempotency

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/Ocean-Gaming/platform-go/pg"
	"github.com/Ocean-Gaming/platform-go/tenant"
)

// PostgresStore is an idempotency Store bound to one unit of work.
//
// Construct it with the caller's *sql.Tx so the idempotency record and the
// state change it guards commit or roll back together.
type PostgresStore struct{ db pg.DBTX }

func NewPostgresStore(db pg.DBTX) *PostgresStore { return &PostgresStore{db: db} }

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

func (s *PostgresStore) Complete(ctx context.Context, key Key, response []byte) error {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE idempotency_keys
		   SET response = $3, state = 'completed', completed_at = now()
		 WHERE tenant_id = $1 AND key = $2`,
		string(tid), string(key), response)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotClaimed
	}
	return nil
}
