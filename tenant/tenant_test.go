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

// ADR-0127: the UUID form is told apart from the slug it replaces, canonically
// spelled only. Both stay Valid until the cut-over's last phase.
func TestIsUUIDAcceptsOnlyTheCanonicalLowercaseForm(t *testing.T) {
	yes := []tenant.ID{"3f2504e0-4f89-41d3-9a0c-0305e82c3301", "00000000-0000-0000-0000-000000000000"}
	no := []tenant.ID{
		"acme", "",
		"3F2504E0-4F89-41D3-9A0C-0305E82C3301",   // uppercase
		"3f2504e04f8941d39a0c0305e82c3301",       // no dashes
		"{3f2504e0-4f89-41d3-9a0c-0305e82c3301}", // braced
		"3f2504e0-4f89-41d3-9a0c-0305e82c330",    // 35 chars
		"3f2504e0-4f89-41d3-9a0c-0305e82c33011",  // 37 chars
		"3f2504e0-4f89-41d3-9a0c-0305e82c330g",   // non-hex
		"3f2504e0-4f89-41d3-9a0c0-305e82c3301",   // dash misplaced
		"urn:uuid:3f2504e0-4f89-41d3-9a0c-0305e82c3301",
	}
	for _, id := range yes {
		if !id.IsUUID() || !id.Valid() {
			t.Errorf("%q: want IsUUID and Valid", id)
		}
	}
	for _, id := range no {
		if id.IsUUID() {
			t.Errorf("%q: IsUUID must be false", id)
		}
	}
	if !tenant.ID("acme").Valid() {
		t.Fatal("the slug form must stay Valid during the cut-over")
	}
}
