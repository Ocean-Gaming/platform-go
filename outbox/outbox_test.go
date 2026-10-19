package outbox_test

import (
	"context"
	"errors"
	"strings"
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

// Rule 2: a message may not name a tenant other than the request's. Writing it
// would announce one merchant's fact under another's id.
func TestAMessageForAnotherTenantIsRefused(t *testing.T) {
	o := outbox.NewMemoryOutbox()
	m := msg("e1", "BetPlaced")
	m.Tenant = "globex"
	if err := o.Write(ctxFor("acme"), m); !errors.Is(err, outbox.ErrTenantMismatch) {
		t.Fatalf("want ErrTenantMismatch, got %v", err)
	}
	m.Tenant = "acme"
	if err := o.Write(ctxFor("acme"), m); err != nil {
		t.Fatalf("a message naming the request's own tenant was refused: %v", err)
	}
}

// The whole batch is checked before any of it is written: a bad message
// part-way through leaves nothing behind.
func TestABatchWithOneBadMessageWritesNothing(t *testing.T) {
	o := outbox.NewMemoryOutbox()
	if err := o.Write(ctxFor("acme"), msg("e1", "BetPlaced"), outbox.Message{ID: "e2"}); !errors.Is(err, outbox.ErrNoTopic) {
		t.Fatalf("want ErrNoTopic, got %v", err)
	}
	if n := o.Len(); n != 0 {
		t.Fatalf("%d messages written from a batch that was refused", n)
	}
}

// The tenant header is stamped on a copy. Writing into the caller's map leaked
// the last tenant into every other message sharing it, and raced when the map
// was shared across goroutines.
func TestTheCallersHeadersAreNotModified(t *testing.T) {
	o := outbox.NewMemoryOutbox()
	shared := map[string]string{"source": "test"}
	m := msg("e1", "BetPlaced")
	m.Headers = shared
	if err := o.Write(ctxFor("acme"), m); err != nil {
		t.Fatal(err)
	}
	if _, ok := shared["tenant_id"]; ok || len(shared) != 1 {
		t.Fatalf("the caller's headers were modified: %v", shared)
	}
	got, _ := o.Unpublished(context.Background(), 1)
	if got[0].Headers["tenant_id"] != "acme" || got[0].Headers["source"] != "test" {
		t.Fatalf("stored headers %v; want source kept and tenant_id stamped", got[0].Headers)
	}
}

// Topic carries the registry's event type, never a Kafka topic: event-bus resolves the topic. The
// same rule is a CHECK on the column (migrations/0003).
func TestATopicMustBeAnEventTypeName(t *testing.T) {
	o := outbox.NewMemoryOutbox()
	for _, bad := range []string{"t", "ocean.wallet.v1", "Bet-Placed", "Bet Placed", "BetPlacéd", "B" + strings.Repeat("x", 255)} {
		if err := o.Write(ctxFor("acme"), msg("e1", bad)); !errors.Is(err, outbox.ErrInvalidTopic) {
			t.Errorf("%q: want ErrInvalidTopic, got %v", bad, err)
		}
	}
	for _, good := range []string{"BetPlaced", "T", "KycLevelChanged", "RiskFlagRaised2"} {
		if err := o.Write(ctxFor("acme"), msg("e-"+good, good)); err != nil {
			t.Errorf("%q: %v", good, err)
		}
	}
}

// A partition key is at most 255 characters, counted as characters.
func TestAPartitionKeyIsAtMost255Characters(t *testing.T) {
	o := outbox.NewMemoryOutbox()
	m := msg("e1", "BetPlaced")
	m.PartitionKey = strings.Repeat("é", 256)
	if err := o.Write(ctxFor("acme"), m); !errors.Is(err, outbox.ErrPartitionKeyTooLong) {
		t.Fatalf("want ErrPartitionKeyTooLong, got %v", err)
	}
	m.PartitionKey = strings.Repeat("é", 255)
	if err := o.Write(ctxFor("acme"), m); err != nil {
		t.Fatalf("a 255-character partition key was refused: %v", err)
	}
}
