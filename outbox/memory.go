package outbox

import (
	"context"
	"sort"
	"sync"
	"time"
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
	msgs, err := prepare(ctx, o.now, msgs)
	if err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.msgs = append(o.msgs, msgs...)
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
	// The Postgres reader's order: occurred_at, then id.
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].OccurredAt.Equal(out[j].OccurredAt) {
			return out[i].OccurredAt.Before(out[j].OccurredAt)
		}
		return out[i].ID < out[j].ID
	})
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
