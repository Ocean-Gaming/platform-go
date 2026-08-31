package tenant_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Ocean-Gaming/platform-go/tenant"
)

func TestRequire_missingIsAnError(t *testing.T) {
	// Rule 2: no tenant must never silently become a default tenant.
	if _, err := tenant.Require(context.Background()); !errors.Is(err, tenant.ErrMissing) {
		t.Fatalf("want ErrMissing, got %v", err)
	}
}

func TestRequire_blankTenantRejected(t *testing.T) {
	for _, blank := range []tenant.ID{"", "   ", "\t"} {
		ctx := tenant.NewContext(context.Background(), blank)
		if _, err := tenant.Require(ctx); !errors.Is(err, tenant.ErrMissing) {
			t.Fatalf("blank tenant %q was accepted", blank)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	ctx := tenant.NewContext(context.Background(), "acme")
	got, err := tenant.Require(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got != "acme" {
		t.Fatalf("want acme, got %q", got)
	}
}
