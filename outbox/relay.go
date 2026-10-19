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
//
// A full batch is followed at once by the next one; the relay waits Interval
// only after a batch that was short, empty or failed. Waiting after every batch
// capped every service at Batch/Interval events a second, so a backlog of
// 100,000 rows took over a quarter of an hour to drain at the defaults.
type Relay struct {
	Reader    Reader
	Publisher Publisher
	Batch     int
	Interval  time.Duration

	// TickTimeout bounds one Tick: the read, the publish and the mark. Default
	// 30 s. Without it a publish that never returned stalled the relay for good,
	// and silently: Tick never failed, so the failure log below never fired.
	TickTimeout time.Duration

	Log *slog.Logger
}

// DefaultTickTimeout is TickTimeout's default.
const DefaultTickTimeout = 30 * time.Second

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

	// The first batch goes at once: a service that restarts with a backlog has
	// no reason to sit on it for an Interval.
	wait := time.NewTimer(0)
	defer wait.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wait.C:
		}

		// Drain: keep going while batches come back full. The cancellation
		// check sits between batches, so shutdown never waits on a backlog.
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			n, err := r.Tick(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err() // shutdown, not an outage
				}
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
				break
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
			// A short batch means the backlog is drained. A full one means there
			// may be more. This is not a busy loop: a full batch is only
			// followed at once when the previous one was published AND marked,
			// so the same rows cannot come back.
			if n < r.Batch {
				break
			}
		}
		wait.Reset(r.Interval)
	}
}

// Tick publishes at most one batch. Exposed so tests can drive the relay
// deterministically instead of sleeping.
//
// The whole tick runs under TickTimeout, the publish included, so a broker that
// accepts the connection and never answers surfaces as a failed tick.
func (r *Relay) Tick(ctx context.Context) (int, error) {
	timeout := r.TickTimeout
	if timeout <= 0 {
		timeout = DefaultTickTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

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
