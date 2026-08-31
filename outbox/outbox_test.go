package outbox_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Ocean-Gaming/platform-go/outbox"
	"github.com/Ocean-Gaming/platform-go/tenant"
)

type stubPublisher struct {
	published [][]outbox.Message
	err       error
}

func (s *stubPublisher) Publish(_ context.Context, m []outbox.Message) error {
	if s.err != nil {
		return s.err
	}
	s.published = append(s.published, m)
	return nil
}

func ctxFor(id tenant.ID) context.Context {
	return tenant.NewContext(context.Background(), id)
}

func msg(id, topic string) outbox.Message {
	return outbox.Message{ID: id, Topic: topic, PartitionKey: "player-1", Payload: []byte("{}")}
}

func TestEnvelopeRulesAreEnforced(t *testing.T) {
	o := outbox.NewMemoryOutbox()
	ctx := ctxFor("acme")

	if err := o.Write(ctx, outbox.Message{Topic: "t"}); !errors.Is(err, outbox.ErrNoID) {
		t.Fatalf("event without id must be rejected, got %v", err)
	}
	if err := o.Write(ctx, outbox.Message{ID: "e1"}); !errors.Is(err, outbox.ErrNoTopic) {
		t.Fatalf("event without topic must be rejected, got %v", err)
	}
	if err := o.Write(context.Background(), msg("e1", "t")); !errors.Is(err, tenant.ErrMissing) {
		t.Fatalf("event without tenant must be rejected, got %v", err)
	}
}

func TestRelayPublishesThenMarks(t *testing.T) {
	o := outbox.NewMemoryOutbox()
	ctx := ctxFor("acme")
	if err := o.Write(ctx, msg("e1", "BetPlaced"), msg("e2", "BetSettled")); err != nil {
		t.Fatal(err)
	}

	pub := &stubPublisher{}
	r := &outbox.Relay{Reader: o, Publisher: pub, Batch: 10}

	n, err := r.Tick(context.Background())
	if err != nil || n != 2 {
		t.Fatalf("tick: n=%d err=%v", n, err)
	}

	// Second tick must publish nothing — the rows are marked.
	n, err = r.Tick(context.Background())
	if err != nil || n != 0 {
		t.Fatalf("second tick republished: n=%d err=%v", n, err)
	}
	if len(pub.published) != 1 {
		t.Fatalf("publisher called %d times, want 1", len(pub.published))
	}
}

// A broker outage must not lose messages: they stay unpublished and the next
// tick retries them. This is the property that lets a producer keep committing
// while the bus is down.
func TestBrokerFailureLeavesMessagesForRetry(t *testing.T) {
	o := outbox.NewMemoryOutbox()
	ctx := ctxFor("acme")
	_ = o.Write(ctx, msg("e1", "BetPlaced"))

	failing := &stubPublisher{err: errors.New("broker down")}
	r := &outbox.Relay{Reader: o, Publisher: failing, Batch: 10}

	if _, err := r.Tick(context.Background()); err == nil {
		t.Fatal("expected the broker error to surface")
	}

	// The bus recovers; the message is still there.
	ok := &stubPublisher{}
	r.Publisher = ok
	n, err := r.Tick(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("message lost across broker outage: n=%d err=%v", n, err)
	}
}

func TestTenantIsStampedFromContext(t *testing.T) {
	o := outbox.NewMemoryOutbox()
	if err := o.Write(ctxFor("acme"), msg("e1", "T")); err != nil {
		t.Fatal(err)
	}
	got, _ := o.Unpublished(context.Background(), 1)
	if got[0].Tenant != "acme" {
		t.Fatalf("tenant not stamped on event header, got %q", got[0].Tenant)
	}
}
