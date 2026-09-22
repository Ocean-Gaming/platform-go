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

	// How long this relay has been failing, not just that it failed again.
	//
	// "Nothing is lost" below is true per tick and misleading over hours. A
	// relay that cannot publish is a service whose events have stopped
	// reaching every consumer downstream, and the old log said the same
	// sentence once a second with no way to tell the first failure from the
	// four thousandth. wallet-ledger emitted ~4,500 identical lines over 75
	// minutes in September 2026 and nothing escalated, because every line
	// looked exactly like a transient blip. These two fields are what makes
	// "this relay has been down for 75 minutes" a thing an alert can say.
	var fails int
	var since time.Time

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			n, err := r.Tick(ctx)
			if err != nil {
				// Log and keep going. The rows are still in the outbox, so the
				// next tick retries them; nothing is lost.
				if fails == 0 {
					since = time.Now()
				}
				fails++
				r.Log.ErrorContext(ctx, "outbox relay tick failed",
					"error", err, "published", n,
					"consecutive_failures", fails,
					"failing_for", time.Since(since).Round(time.Second).String())
				continue
			}
			if fails > 0 {
				// Recovery is reported at INFO because the outage was: an
				// operator watching the errors stop cannot otherwise tell a
				// relay that recovered from a relay that died.
				r.Log.InfoContext(ctx, "outbox relay recovered",
					"after_failures", fails,
					"was_failing_for", time.Since(since).Round(time.Second).String())
				fails = 0
			}
			if n > 0 {
				// The success path, at debug. A relay that has silently stopped
				// draining looks identical to an idle one in the logs, so
				// proving it is alive needed a SIGQUIT and a goroutine dump.
				// It stays at debug because a healthy relay ticks every second.
				r.Log.DebugContext(ctx, "outbox relay published", "count", n)
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
