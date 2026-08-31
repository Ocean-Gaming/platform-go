package outbox

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/Ocean-Gaming/platform-go/tenant"
)

// MemoryOutbox is an in-process Writer + Reader for unit tests.
type MemoryOutbox struct {
	mu   sync.Mutex
	msgs []Message
	pub  map[string]bool
	now  func() time.Time
}

func NewMemoryOutbox() *MemoryOutbox {
	return &MemoryOutbox{pub: map[string]bool{}, now: time.Now}
}

func (o *MemoryOutbox) Write(ctx context.Context, msgs ...Message) error {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, m := range msgs {
		if m.Tenant == "" {
			m.Tenant = tid
		}
		if m.OccurredAt.IsZero() {
			m.OccurredAt = o.now()
		}
		if err := m.Validate(); err != nil {
			return err
		}
		o.msgs = append(o.msgs, m)
	}
	return nil
}

func (o *MemoryOutbox) Unpublished(ctx context.Context, limit int) ([]Message, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []Message
	for _, m := range o.msgs {
		if !o.pub[m.ID] {
			out = append(out, m)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].OccurredAt.Before(out[j].OccurredAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (o *MemoryOutbox) MarkPublished(ctx context.Context, ids []string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, id := range ids {
		o.pub[id] = true
	}
	return nil
}

// Len reports how many messages have been written, for assertions.
func (o *MemoryOutbox) Len() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.msgs)
}
