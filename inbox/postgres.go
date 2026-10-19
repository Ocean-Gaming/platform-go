package inbox

import (
	"context"
	"database/sql"

	"github.com/Ocean-Gaming/platform-go/tenant"
)

// PostgresStore records handled events using the caller's transaction, so the
// dedup row and the handler's own writes commit together. It takes a *sql.Tx
// and nothing else: on the pool the dedup row commits on its own, and a handler
// that then fails has its event marked handled and never retried.
type PostgresStore struct{ db *sql.Tx }

func NewPostgresStore(tx *sql.Tx) *PostgresStore { return &PostgresStore{db: tx} }

// MarkProcessed is a single INSERT ... ON CONFLICT DO NOTHING.
//
// Doing this as a SELECT followed by an INSERT would reintroduce the exact race
// the inbox exists to prevent: two concurrent deliveries both seeing "not
// processed" and both running the handler.
func (s *PostgresStore) MarkProcessed(ctx context.Context, consumer, eventID string) (bool, error) {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return false, err
	}
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO inbox (event_id, tenant_id, consumer, processed_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (tenant_id, consumer, event_id) DO NOTHING`,
		eventID, string(tid), consumer)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}
