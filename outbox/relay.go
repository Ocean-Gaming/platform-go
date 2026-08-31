package outbox

import (
	"context"
	"log/slog"
	"time"
)

// Relay moves committed outbox rows to the broker.
//
// It is deliberately dumb: poll, publish, mark. It never blocks a producer, and
// on failure it simply leaves rows unpublished for the next tick. Delivery is
// at-least-once by construction, which is why consumers dedup.
type Relay struct {
	Reader    Reader
	Publisher Publisher
	Batch     int
	Interval  time.Duration
	Log       *slog.Logger
}

// Run polls until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	if r.Batch <= 0 {
		r.Batch = 100
	}
	if r.Interval <= 0 {
		r.Interval = time.Second
	}
	if r.Log == nil {
		r.Log = slog.Default()
	}

	t := time.NewTicker(r.Interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if n, err := r.Tick(ctx); err != nil {
				// Log and keep going. The rows are still in the outbox, so the
				// next tick retries them; nothing is lost.
				r.Log.ErrorContext(ctx, "outbox relay tick failed", "error", err, "published", n)
			}
		}
	}
}

// Tick publishes at most one batch. Exposed so tests can drive the relay
// deterministically instead of sleeping.
func (r *Relay) Tick(ctx context.Context) (int, error) {
	msgs, err := r.Reader.Unpublished(ctx, r.Batch)
	if err != nil {
		return 0, err
	}
	if len(msgs) == 0 {
		return 0, nil
	}
	if err := r.Publisher.Publish(ctx, msgs); err != nil {
		return 0, err
	}
	ids := make([]string, 0, len(msgs))
	for _, m := range msgs {
		ids = append(ids, m.ID)
	}
	if err := r.Reader.MarkPublished(ctx, ids); err != nil {
		// Published but not marked: the next tick republishes. At-least-once is
		// the contract, so this is safe — consumers dedup on event id.
		return len(msgs), err
	}
	return len(msgs), nil
}
