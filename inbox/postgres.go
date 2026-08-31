package inbox

import (
	"context"

	"github.com/Ocean-Gaming/platform-go/pg"
	"github.com/Ocean-Gaming/platform-go/tenant"
)

// PostgresStore records handled events using the caller's transaction, so the
// dedup row and the handler's own writes commit together.
type PostgresStore struct{ db pg.DBTX }

func NewPostgresStore(db pg.DBTX) *PostgresStore { return &PostgresStore{db: db} }

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
