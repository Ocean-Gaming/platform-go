package inbox_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Ocean-Gaming/platform-go/inbox"
	"github.com/Ocean-Gaming/platform-go/tenant"
)

// Rule 5: at-least-once delivery must become effectively-once handling.
func TestRedeliveryDoesNotRunHandlerTwice(t *testing.T) {
	s := inbox.NewMemoryStore()
	ctx := tenant.NewContext(context.Background(), "acme")

	runs := 0
	handler := func(context.Context) error { runs++; return nil }

	for i := 0; i < 5; i++ {
		if err := inbox.Handle(ctx, s, "wallet-consumer", "evt-1", handler); err != nil {
			t.Fatal(err)
		}
	}
	if runs != 1 {
		t.Fatalf("handler ran %d times for one event id; want 1", runs)
	}
}

func TestDifferentConsumersEachProcessTheSameEvent(t *testing.T) {
	s := inbox.NewMemoryStore()
	ctx := tenant.NewContext(context.Background(), "acme")

	a, b := 0, 0
	_ = inbox.Handle(ctx, s, "consumer-a", "evt-1", func(context.Context) error { a++; return nil })
	_ = inbox.Handle(ctx, s, "consumer-b", "evt-1", func(context.Context) error { b++; return nil })

	if a != 1 || b != 1 {
		t.Fatalf("each consumer must see the event once; got a=%d b=%d", a, b)
	}
}

func TestDedupIsScopedPerTenant(t *testing.T) {
	s := inbox.NewMemoryStore()
	acme := tenant.NewContext(context.Background(), "acme")
	globex := tenant.NewContext(context.Background(), "globex")

	runs := 0
	fn := func(context.Context) error { runs++; return nil }

	_ = inbox.Handle(acme, s, "c", "evt-1", fn)
	_ = inbox.Handle(globex, s, "c", "evt-1", fn)

	if runs != 2 {
		t.Fatalf("tenants must not suppress each other's events; runs=%d want 2", runs)
	}
}

func TestHandlerErrorPropagates(t *testing.T) {
	s := inbox.NewMemoryStore()
	ctx := tenant.NewContext(context.Background(), "acme")
	boom := errors.New("boom")

	err := inbox.Handle(ctx, s, "c", "evt-1", func(context.Context) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
}

func TestRequiresTenant(t *testing.T) {
	s := inbox.NewMemoryStore()
	err := inbox.Handle(context.Background(), s, "c", "evt-1", func(context.Context) error { return nil })
	if !errors.Is(err, tenant.ErrMissing) {
		t.Fatalf("want ErrMissing, got %v", err)
	}
}
