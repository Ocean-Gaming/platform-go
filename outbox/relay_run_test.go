package outbox_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Ocean-Gaming/platform-go/outbox"
)

// capture collects records so a test can assert on what an operator would see.
type capture struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (c *capture) Enabled(context.Context, slog.Level) bool { return true }
func (c *capture) WithAttrs([]slog.Attr) slog.Handler       { return c }
func (c *capture) WithGroup(string) slog.Handler            { return c }

func (c *capture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recs = append(c.recs, r)
	return nil
}

// find returns the attributes of the most recent record with this message.
func (c *capture) find(msg string) (map[string]slog.Value, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := len(c.recs) - 1; i >= 0; i-- {
		if c.recs[i].Message != msg {
			continue
		}
		attrs := map[string]slog.Value{}
		c.recs[i].Attrs(func(a slog.Attr) bool {
			attrs[a.Key] = a.Value
			return true
		})
		return attrs, true
	}
	return nil, false
}

// flakyPublisher fails until it is told to stop.
type flakyPublisher struct {
	mu   sync.Mutex
	fail bool
}

func (p *flakyPublisher) setFail(v bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fail = v
}

func (p *flakyPublisher) Publish(context.Context, []outbox.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail {
		return errors.New("broker unavailable")
	}
	return nil
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// A relay that cannot publish must say how long it has been failing.
//
// Not a cosmetic field. wallet-ledger logged the same sentence once a second
// for 75 minutes in September 2026 and nothing escalated, because the four
// thousandth line was indistinguishable from the first. "Failed again" is not
// alertable; "has been failing for 75 minutes" is.
func TestAFailingRelayReportsHowLongItHasBeenFailing(t *testing.T) {
	store := outbox.NewMemoryOutbox()
	if err := store.Write(ctxFor("acme"), msg("e-1", "BetPlaced")); err != nil {
		t.Fatal(err)
	}
	pub := &flakyPublisher{fail: true}
	cap := &capture{}

	relay := &outbox.Relay{
		Reader: store, Publisher: pub, Batch: 10, Interval: time.Millisecond,
		Log: slog.New(cap),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go relay.Run(ctx) //nolint:errcheck // returns ctx.Err() on cancel

	waitFor(t, "the relay to fail three times", func() bool {
		a, ok := cap.find("outbox relay tick failed")
		return ok && a["consecutive_failures"].Int64() >= 3
	})

	a, _ := cap.find("outbox relay tick failed")
	if a["failing_for"].String() == "" {
		t.Error("no failing_for; the duration is what makes the outage alertable")
	}

	// And recovery must be announced, because an operator watching the errors
	// stop cannot otherwise tell a relay that recovered from one that died.
	pub.setFail(false)
	waitFor(t, "the relay to report recovery", func() bool {
		_, ok := cap.find("outbox relay recovered")
		return ok
	})

	rec, _ := cap.find("outbox relay recovered")
	if rec["after_failures"].Int64() < 3 {
		t.Errorf("after_failures = %v, want the failures it actually suffered", rec["after_failures"])
	}
}

// A healthy relay must leave a trace at debug.
//
// Draining normally and having silently stopped look identical in the logs
// otherwise, which is why proving the relay was alive needed a SIGQUIT and a
// goroutine dump.
func TestASuccessfulPublishIsVisibleAtDebug(t *testing.T) {
	store := outbox.NewMemoryOutbox()
	if err := store.Write(ctxFor("acme"), msg("e-1", "BetPlaced")); err != nil {
		t.Fatal(err)
	}
	cap := &capture{}
	relay := &outbox.Relay{
		Reader: store, Publisher: &flakyPublisher{}, Batch: 10, Interval: time.Millisecond,
		Log: slog.New(cap),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go relay.Run(ctx) //nolint:errcheck // returns ctx.Err() on cancel

	waitFor(t, "a published line", func() bool {
		_, ok := cap.find("outbox relay published")
		return ok
	})
	a, _ := cap.find("outbox relay published")
	if a["count"].Int64() != 1 {
		t.Errorf("count = %v, want 1", a["count"])
	}
}
