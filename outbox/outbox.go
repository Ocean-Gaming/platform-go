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
	"fmt"
	"maps"
	"time"
	"unicode/utf8"

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

	// ErrInvalidTopic means Topic is not a registry event type: PascalCase letters and digits, at
	// most MaxLength characters. Topic carries the event type (BetSettled), never a Kafka topic;
	// event-bus resolves the topic from the registry.
	ErrInvalidTopic = errors.New("outbox: topic is not an event type name")

	// ErrPartitionKeyTooLong means PartitionKey is over MaxLength characters.
	ErrPartitionKeyTooLong = errors.New("outbox: partition key is longer than 255 characters")

	// ErrTenantMismatch means a message names a tenant other than the one in the
	// request's context. Writing it would announce one merchant's fact under
	// another's id (rule 2); a producer acting for several tenants sets each
	// message's tenant in the context it writes with.
	ErrTenantMismatch = errors.New("outbox: message tenant differs from the request's tenant")

	// ErrNotMarked means MarkPublished matched fewer rows than it was given.
	ErrNotMarked = errors.New("outbox: not every published message was marked")
)

// MaxLength is the longest topic and partition key accepted, in characters. migrations/0003
// holds both columns to it.
const MaxLength = 255

// Validate enforces the envelope rules every platform event must satisfy. The topic and
// partition-key rules are the ones migrations/0003 enforces in Postgres.
func (m Message) Validate() error {
	if m.ID == "" {
		return ErrNoID
	}
	if m.Topic == "" {
		return ErrNoTopic
	}
	if !eventType(m.Topic) {
		return fmt.Errorf("%w: %q", ErrInvalidTopic, m.Topic)
	}
	if utf8.RuneCountInString(m.PartitionKey) > MaxLength {
		return ErrPartitionKeyTooLong
	}
	if !m.Tenant.Valid() {
		return tenant.ErrMissing
	}
	return nil
}

// eventType reports whether s has the shape of a registry event type: an uppercase ASCII letter,
// then ASCII letters and digits, MaxLength characters at most. Every registered type has it.
func eventType(s string) bool {
	if len(s) == 0 || len(s) > MaxLength || s[0] < 'A' || s[0] > 'Z' {
		return false
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

// prepare stamps and checks a whole batch before any of it is written, so a bad
// message part-way through leaves nothing behind in either store. It returns
// copies: the caller's Headers map is never written to, because a map shared
// across messages or goroutines would otherwise carry the last tenant stamped
// into it.
func prepare(ctx context.Context, now func() time.Time, msgs []Message) ([]Message, error) {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Message, len(msgs))
	for i, m := range msgs {
		switch {
		case m.Tenant == "":
			m.Tenant = tid
		case m.Tenant != tid:
			return nil, fmt.Errorf("%w: message %q is for %q, the request for %q", ErrTenantMismatch, m.ID, m.Tenant, tid)
		}
		if m.OccurredAt.IsZero() {
			m.OccurredAt = now()
		}
		if err := m.Validate(); err != nil {
			return nil, err
		}
		headers := make(map[string]string, len(m.Headers)+1)
		maps.Copy(headers, m.Headers)
		// Rule 2 - tenant_id travels in the event header, not only the payload.
		headers["tenant_id"] = string(m.Tenant)
		m.Headers = headers
		out[i] = m
	}
	return out, nil
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
	// MarkPublished records successful publication. It returns an error when it
	// could not mark every id: the relay follows a full batch at once, so a mark
	// that silently matched nothing would republish the same rows in a loop.
	MarkPublished(ctx context.Context, ids []string) error
}
