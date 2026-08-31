package platform_test

import (
	"context"
	"testing"

	"github.com/Ocean-Gaming/platform-go/idempotency"
	"github.com/Ocean-Gaming/platform-go/inbox"
	"github.com/Ocean-Gaming/platform-go/platformtest"
)

// The fakes run unconditionally — no Docker, no database.
func TestMemoryConformance(t *testing.T) {
	idem := idempotency.NewMemoryStore()
	in := inbox.NewMemoryStore()
	platformtest.RunConformance(t, platformtest.Harness{
		Name: "memory",
		Do: func(_ context.Context, fn func(idempotency.Store, inbox.Store) error) error {
			return fn(idem, in)
		},
	})
}
