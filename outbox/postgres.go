package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Ocean-Gaming/platform-go/tenant"
)

// PostgresWriter appends outbox rows using the caller's transaction.
//
// It takes the same *sql.Tx as the state change, and only a transaction. That
// is the entire point: the row and the change commit together or not at all.
// A pg.DBTX let the pool in too, and an event written on the pool commits
// whether or not the change it announces does.
type PostgresWriter struct{ db *sql.Tx }

func NewPostgresWriter(tx *sql.Tx) *PostgresWriter { return &PostgresWriter{db: tx} }

func (w *PostgresWriter) Write(ctx context.Context, msgs ...Message) error {
	msgs, err := prepare(ctx, time.Now, msgs)
	if err != nil {
		return err
	}
	for _, m := range msgs {
		hj, err := json.Marshal(m.Headers)
		if err != nil {
			return err
		}
		if _, err := w.db.ExecContext(ctx, `
			INSERT INTO outbox (id, tenant_id, topic, partition_key, payload, headers, occurred_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			m.ID, string(m.Tenant), m.Topic, m.PartitionKey, m.Payload, hj, m.OccurredAt,
		); err != nil {
			return err
		}
	}
	return nil
}

// PostgresReader is the relay's view. It runs on the pool, not on a caller's
// transaction, because the relay is a separate process from any producer.
type PostgresReader struct{ db *sql.DB }

func NewPostgresReader(db *sql.DB) *PostgresReader { return &PostgresReader{db: db} }

func (r *PostgresReader) Unpublished(ctx context.Context, limit int) ([]Message, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, tenant_id, topic, partition_key, payload, headers, occurred_at
		  FROM outbox
		 WHERE published_at IS NULL
		 ORDER BY occurred_at ASC, id ASC
		 LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []Message
	for rows.Next() {
		var (
			m  Message
			t  string
			hj []byte
		)
		if err := rows.Scan(&m.ID, &t, &m.Topic, &m.PartitionKey, &m.Payload, &hj, &m.OccurredAt); err != nil {
			return nil, err
		}
		m.Tenant = tenant.ID(t)
		if len(hj) > 0 {
			_ = json.Unmarshal(hj, &m.Headers)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (r *PostgresReader) MarkPublished(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	// One statement, not one per id: the relay's default batch is 100, so a
	// loop here is 100 round trips per tick on the hot path, in every service.
	res, err := r.db.ExecContext(ctx,
		`UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, pq(ids))
	if err != nil {
		return err
	}
	// Already-published rows match too, so a re-mark after a crash still counts
	// every id. Fewer means rows the relay just published are not there to mark.
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if int(n) != len(ids) {
		return fmt.Errorf("%w: %d of %d", ErrNotMarked, n, len(ids))
	}
	return nil
}

// pq renders a string slice as a Postgres array literal. The driver handles
// []string for ANY() only with its own array type; this keeps the store free of
// a driver-specific dependency.
func pq(ids []string) string {
	var b strings.Builder
	b.WriteByte('{')
	for i, id := range ids {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('"')
		b.WriteString(strings.ReplaceAll(id, `"`, `\"`))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}
