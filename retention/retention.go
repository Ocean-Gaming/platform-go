// Package retention ages rows out of the four platform tables, one policy per
// table and class of data, every policy opt-in (meta-repo ADR-0130).
//
// Nothing here has a default period. A service names the policies it wants
// and their periods in its own configuration; a policy left nil deletes
// nothing. Two kinds of row are never aged out by any policy: an outbox row
// that has not been published, and an idempotency key still in flight. Both
// are work that has not finished, and the second is reported when it is old
// enough to be a crashed command.
//
// The sweeper deletes in bounded batches, each in its own short transaction,
// one tenant at a time, so it never holds a lock across a batch and a service
// with row-level security can scope each transaction with BeforeBatch.
//
// Indexes are the enabling service's. Each DELETE below filters on a timestamp
// the table has no index for (published_at, processed_at, completed_at); a
// service that enables a policy adds the index that DELETE needs, global or
// tenant-first as its own batch order and RLS dictate, in its own migration.
// This module creates none, and neither variant is created in advance.
package retention

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Ocean-Gaming/platform-go/tenant"
)

// MinInboxRetention is the shortest Inbox.After the sweeper accepts. An inbox
// row is what stops a redelivery from running a handler twice, so it has to
// outlive the topic's Kafka retention and any replay the service promises.
const MinInboxRetention = 7 * 24 * time.Hour

// StaleInFlight is the age at which an in-flight idempotency key is reported.
// A command that has not completed in a day did not complete; it crashed, and
// someone should look rather than have a sweeper hide it.
const StaleInFlight = 24 * time.Hour

var (
	ErrNoDB              = errors.New("retention: no database")
	ErrNoPolicy          = errors.New("retention: no policy is enabled")
	ErrInboxTooShort     = fmt.Errorf("retention: Inbox.After is under the %s minimum", MinInboxRetention)
	ErrDeleteBeforeClear = errors.New("retention: IdempotencyKey.DeleteAfter is shorter than IdempotencyResponse.ClearAfter")
	ErrNonPositive       = errors.New("retention: a period must be positive")
)

// PublishedOutbox deletes outbox rows published longer ago than After. Rows
// with published_at IS NULL are never touched, whatever their occurred_at.
type PublishedOutbox struct{ After time.Duration }

// Inbox deletes inbox rows processed longer ago than After, which must be at
// least MinInboxRetention.
type Inbox struct{ After time.Duration }

// IdempotencyResponse clears the stored response of completed keys older than
// ClearAfter. The key, its fingerprint and its state stay, so a retry after
// this is still refused as a replay, with an empty response, and is never
// executed again.
type IdempotencyResponse struct{ ClearAfter time.Duration }

// IdempotencyKey deletes completed keys older than DeleteAfter. A service whose
// commands move money sets this to its regulatory record period or leaves it
// nil; DeleteAfter is never shorter than IdempotencyResponse.ClearAfter.
// In-flight keys are never deleted.
type IdempotencyKey struct{ DeleteAfter time.Duration }

// Policies is the set a service enables. A nil policy is off.
type Policies struct {
	PublishedOutbox     *PublishedOutbox
	Inbox               *Inbox
	IdempotencyResponse *IdempotencyResponse
	IdempotencyKey      *IdempotencyKey
}

// Validate is what Sweeper.Run applies before touching a row.
func (p Policies) Validate() error {
	if p.PublishedOutbox == nil && p.Inbox == nil && p.IdempotencyResponse == nil && p.IdempotencyKey == nil {
		return ErrNoPolicy
	}
	for _, d := range []time.Duration{
		period(p.PublishedOutbox != nil, func() time.Duration { return p.PublishedOutbox.After }),
		period(p.Inbox != nil, func() time.Duration { return p.Inbox.After }),
		period(p.IdempotencyResponse != nil, func() time.Duration { return p.IdempotencyResponse.ClearAfter }),
		period(p.IdempotencyKey != nil, func() time.Duration { return p.IdempotencyKey.DeleteAfter }),
	} {
		if d < 0 {
			return ErrNonPositive
		}
	}
	if p.Inbox != nil && p.Inbox.After < MinInboxRetention {
		return ErrInboxTooShort
	}
	if p.IdempotencyKey != nil && p.IdempotencyResponse != nil && p.IdempotencyKey.DeleteAfter < p.IdempotencyResponse.ClearAfter {
		return ErrDeleteBeforeClear
	}
	return nil
}

// period returns -1 for an enabled policy with a non-positive period, so one
// loop can refuse all four.
func period(on bool, get func() time.Duration) time.Duration {
	if !on {
		return 0
	}
	if d := get(); d > 0 {
		return d
	}
	return -1
}

// Sweeper runs the enabled policies over the platform tables.
type Sweeper struct {
	DB       *sql.DB
	Policies Policies

	// Batch is the most rows one DELETE or UPDATE touches; default 1000.
	Batch int

	// BeforeBatch runs inside each batch's transaction before its statement,
	// for a service that scopes the connection to the tenant (SET LOCAL for
	// its RLS setting). Nil when there is nothing to set.
	BeforeBatch func(ctx context.Context, tx *sql.Tx, tid tenant.ID) error

	Log *slog.Logger
	Now func() time.Time
}

// Result is what one Run removed, per tenant and table.
type Result struct {
	Tenant           tenant.ID
	OutboxDeleted    int64
	InboxDeleted     int64
	ResponsesCleared int64
	KeysDeleted      int64

	// StaleInFlight counts in-flight keys older than StaleInFlight. They are
	// reported, never deleted.
	StaleInFlight int64
}

// Run applies the policies for each tenant in turn and reports what it removed.
// It stops at the first error and returns what it had done by then.
func (s *Sweeper) Run(ctx context.Context, tenants []tenant.ID) ([]Result, error) {
	if s.DB == nil {
		return nil, ErrNoDB
	}
	if err := s.Policies.Validate(); err != nil {
		return nil, err
	}
	if s.Batch <= 0 {
		s.Batch = 1000
	}
	if s.Log == nil {
		s.Log = slog.Default()
	}
	if s.Now == nil {
		s.Now = time.Now
	}

	var out []Result
	for _, tid := range tenants {
		if !tid.Valid() {
			return out, tenant.ErrMissing
		}
		r, err := s.sweep(ctx, tid)
		out = append(out, r)
		if err != nil {
			return out, fmt.Errorf("retention: tenant %s: %w", tid, err)
		}
	}
	return out, nil
}

func (s *Sweeper) sweep(ctx context.Context, tid tenant.ID) (Result, error) {
	r := Result{Tenant: tid}
	now := s.Now()
	p := s.Policies

	if p.PublishedOutbox != nil {
		n, err := s.drain(ctx, tid, `
			DELETE FROM outbox WHERE id IN (
				SELECT id FROM outbox
				 WHERE tenant_id = $1 AND published_at IS NOT NULL AND published_at < $2
				 LIMIT $3)`, now.Add(-p.PublishedOutbox.After))
		r.OutboxDeleted = n
		if err != nil {
			return r, err
		}
	}
	if p.Inbox != nil {
		n, err := s.drain(ctx, tid, `
			DELETE FROM inbox WHERE (tenant_id, consumer, event_id) IN (
				SELECT tenant_id, consumer, event_id FROM inbox
				 WHERE tenant_id = $1 AND processed_at < $2
				 LIMIT $3)`, now.Add(-p.Inbox.After))
		r.InboxDeleted = n
		if err != nil {
			return r, err
		}
	}
	if p.IdempotencyResponse != nil {
		n, err := s.drain(ctx, tid, `
			UPDATE idempotency_keys SET response = NULL WHERE (tenant_id, key) IN (
				SELECT tenant_id, key FROM idempotency_keys
				 WHERE tenant_id = $1 AND state = 'completed' AND response IS NOT NULL AND completed_at < $2
				 LIMIT $3)`, now.Add(-p.IdempotencyResponse.ClearAfter))
		r.ResponsesCleared = n
		if err != nil {
			return r, err
		}
	}
	if p.IdempotencyKey != nil {
		n, err := s.drain(ctx, tid, `
			DELETE FROM idempotency_keys WHERE (tenant_id, key) IN (
				SELECT tenant_id, key FROM idempotency_keys
				 WHERE tenant_id = $1 AND state = 'completed' AND completed_at < $2
				 LIMIT $3)`, now.Add(-p.IdempotencyKey.DeleteAfter))
		r.KeysDeleted = n
		if err != nil {
			return r, err
		}
	}

	// Never deleted, always reported: an in-flight key older than a day is a
	// command that crashed between Claim and Complete.
	if err := s.DB.QueryRowContext(ctx, `
		SELECT count(*) FROM idempotency_keys
		 WHERE tenant_id = $1 AND state = 'in_flight' AND created_at < $2`,
		string(tid), now.Add(-StaleInFlight)).Scan(&r.StaleInFlight); err != nil {
		return r, err
	}
	if r.StaleInFlight > 0 {
		s.Log.WarnContext(ctx, "retention: in-flight idempotency keys older than a day; these are crashed commands, not garbage",
			"tenant", string(tid), "count", r.StaleInFlight)
	}
	s.Log.InfoContext(ctx, "retention swept",
		"tenant", string(tid), "outbox_deleted", r.OutboxDeleted, "inbox_deleted", r.InboxDeleted,
		"responses_cleared", r.ResponsesCleared, "keys_deleted", r.KeysDeleted)
	return r, nil
}

// drain runs stmt in batches of s.Batch, each in its own transaction, until a
// batch touches fewer rows than the limit. Cancellation is honoured between
// batches.
func (s *Sweeper) drain(ctx context.Context, tid tenant.ID, stmt string, before time.Time) (int64, error) {
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, err := s.batch(ctx, tid, stmt, before)
		total += n
		if err != nil {
			return total, err
		}
		if n < int64(s.Batch) {
			return total, nil
		}
	}
}

func (s *Sweeper) batch(ctx context.Context, tid tenant.ID, stmt string, before time.Time) (n int64, err error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if s.BeforeBatch != nil {
		if err = s.BeforeBatch(ctx, tx, tid); err != nil {
			return 0, err
		}
	}
	res, err := tx.ExecContext(ctx, stmt, string(tid), before, s.Batch)
	if err != nil {
		return 0, err
	}
	if n, err = res.RowsAffected(); err != nil {
		return 0, err
	}
	return n, tx.Commit()
}
