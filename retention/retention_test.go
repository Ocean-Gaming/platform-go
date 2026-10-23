package retention_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Ocean-Gaming/platform-go/retention"
)

// ADR-0130: nothing is enabled by default, and an enabled policy needs a period.
func TestNoPolicyAndNoPeriodAreBothRefused(t *testing.T) {
	if err := (retention.Policies{}).Validate(); !errors.Is(err, retention.ErrNoPolicy) {
		t.Fatalf("empty policies: want ErrNoPolicy, got %v", err)
	}
	for name, p := range map[string]retention.Policies{
		"outbox zero":   {PublishedOutbox: &retention.PublishedOutbox{}},
		"inbox zero":    {Inbox: &retention.Inbox{}},
		"response zero": {IdempotencyResponse: &retention.IdempotencyResponse{}},
		"key negative":  {IdempotencyKey: &retention.IdempotencyKey{DeleteAfter: -time.Hour}},
	} {
		if err := p.Validate(); !errors.Is(err, retention.ErrNonPositive) {
			t.Errorf("%s: want ErrNonPositive, got %v", name, err)
		}
	}
}

// An inbox row must outlive Kafka retention and any replay, so a short period is refused.
func TestAnInboxPeriodUnderSevenDaysIsRefused(t *testing.T) {
	p := retention.Policies{Inbox: &retention.Inbox{After: retention.MinInboxRetention - time.Second}}
	if err := p.Validate(); !errors.Is(err, retention.ErrInboxTooShort) {
		t.Fatalf("want ErrInboxTooShort, got %v", err)
	}
	p.Inbox.After = retention.MinInboxRetention
	if err := p.Validate(); err != nil {
		t.Fatalf("exactly the minimum was refused: %v", err)
	}
}

// The key is the protection, the response the convenience: a key is never deleted before its
// response would have been cleared.
func TestAKeyIsNeverDeletedBeforeItsResponseIsCleared(t *testing.T) {
	p := retention.Policies{
		IdempotencyResponse: &retention.IdempotencyResponse{ClearAfter: 30 * 24 * time.Hour},
		IdempotencyKey:      &retention.IdempotencyKey{DeleteAfter: 29 * 24 * time.Hour},
	}
	if err := p.Validate(); !errors.Is(err, retention.ErrDeleteBeforeClear) {
		t.Fatalf("want ErrDeleteBeforeClear, got %v", err)
	}
	p.IdempotencyKey.DeleteAfter = p.IdempotencyResponse.ClearAfter
	if err := p.Validate(); err != nil {
		t.Fatalf("equal periods were refused: %v", err)
	}
}

func TestRunRefusesToStartWithoutADatabaseOrAValidPolicy(t *testing.T) {
	s := &retention.Sweeper{}
	if _, err := s.Run(context.Background(), nil); !errors.Is(err, retention.ErrNoDB) {
		t.Fatalf("want ErrNoDB, got %v", err)
	}
}
