// Package outbox implements platform rule 4: every state change writes to an
// outbox in the same transaction as the change itself.
//
// This is how a service hands off responsibility without a distributed
// transaction. The write and its announcement commit together or not at all;
// a relay publishes committed rows afterwards. A broker outage delays the
// announcement, it never loses it, and it never blocks the producer.
package outbox

import (
	"context"
	"errors"
	"time"

	"github.com/Ocean-Gaming/platform-go/tenant"
)

// Message is one event awaiting publication.
type Message struct {
	// ID is the event id. Consumers dedup on it (see the inbox package), so it
	// must be stable across republication of the same fact.
	ID string

	Tenant tenant.ID
	Topic  string

	// PartitionKey keeps a single player's events ordered. Default to the
	// player id; ordering across players is not guaranteed and must not be
	// relied on.
	PartitionKey string

	Payload    []byte
	Headers    map[string]string
	OccurredAt time.Time
}

var (
	ErrNoID    = errors.New("outbox: message has no id")
	ErrNoTopic = errors.New("outbox: message has no topic")
)

// Validate enforces the envelope rules every platform event must satisfy.
func (m Message) Validate() error {
	if m.ID == "" {
		return ErrNoID
	}
	if m.Topic == "" {
		return ErrNoTopic
	}
	if !m.Tenant.Valid() {
		return tenant.ErrMissing
	}
	return nil
}

// Writer appends messages to the outbox. Implementations MUST participate in
// the caller's transaction — a Writer that opens its own connection defeats the
// entire pattern.
type Writer interface {
	Write(ctx context.Context, msgs ...Message) error
}

// Publisher hands committed messages to the broker.
type Publisher interface {
	Publish(ctx context.Context, msgs []Message) error
}

// Reader is the relay's view of the outbox.
type Reader interface {
	// Unpublished returns up to limit committed, unpublished messages, oldest
	// first.
	Unpublished(ctx context.Context, limit int) ([]Message, error)
	// MarkPublished records successful publication.
	MarkPublished(ctx context.Context, ids []string) error
}
