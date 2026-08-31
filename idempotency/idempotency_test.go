package idempotency_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Ocean-Gaming/platform-go/idempotency"
	"github.com/Ocean-Gaming/platform-go/tenant"
)

func ctxFor(id tenant.ID) context.Context {
	return tenant.NewContext(context.Background(), id)
}

// The core rule: a retry replays the stored result and does NOT re-execute.
func TestReplayReturnsStoredResponse(t *testing.T) {
	s := idempotency.NewMemoryStore()
	ctx := ctxFor("acme")
	fp := idempotency.Fingerprint([]byte(`{"amount":100}`))

	if _, replay, err := s.Claim(ctx, "k1", fp); err != nil || replay {
		t.Fatalf("first claim: replay=%v err=%v", replay, err)
	}
	if err := s.Complete(ctx, "k1", []byte(`{"tx":"abc"}`)); err != nil {
		t.Fatal(err)
	}

	rec, replay, err := s.Claim(ctx, "k1", fp)
	if err != nil {
		t.Fatal(err)
	}
	if !replay {
		t.Fatal("second claim must replay, not re-execute")
	}
	if string(rec.Response) != `{"tx":"abc"}` {
		t.Fatalf("stored response not replayed, got %q", rec.Response)
	}
}

// Same key, different body is a client bug and must be surfaced, not replayed.
func TestFingerprintMismatchIsRejected(t *testing.T) {
	s := idempotency.NewMemoryStore()
	ctx := ctxFor("acme")

	if _, _, err := s.Claim(ctx, "k1", idempotency.Fingerprint([]byte("A"))); err != nil {
		t.Fatal(err)
	}
	_ = s.Complete(ctx, "k1", []byte("done"))

	_, _, err := s.Claim(ctx, "k1", idempotency.Fingerprint([]byte("B")))
	if !errors.Is(err, idempotency.ErrFingerprintMismatch) {
		t.Fatalf("want ErrFingerprintMismatch, got %v", err)
	}
}

// A concurrent retry while the first is still running must not run in parallel.
func TestInFlightIsRejected(t *testing.T) {
	s := idempotency.NewMemoryStore()
	ctx := ctxFor("acme")
	fp := idempotency.Fingerprint([]byte("A"))

	if _, _, err := s.Claim(ctx, "k1", fp); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Claim(ctx, "k1", fp); !errors.Is(err, idempotency.ErrInFlight) {
		t.Fatalf("want ErrInFlight, got %v", err)
	}
}

// Rule 2: two merchants may use the same key value and must never collide.
func TestKeysAreScopedPerTenant(t *testing.T) {
	s := idempotency.NewMemoryStore()
	fp := idempotency.Fingerprint([]byte("A"))

	if _, _, err := s.Claim(ctxFor("acme"), "shared-key", fp); err != nil {
		t.Fatal(err)
	}
	_ = s.Complete(ctxFor("acme"), "shared-key", []byte("acme-result"))

	// The same key for a different tenant must be a fresh claim.
	_, replay, err := s.Claim(ctxFor("globex"), "shared-key", fp)
	if err != nil {
		t.Fatal(err)
	}
	if replay {
		t.Fatal("tenant globex replayed tenant acme's result — cross-tenant leak")
	}
}

func TestClaimRequiresTenant(t *testing.T) {
	s := idempotency.NewMemoryStore()
	if _, _, err := s.Claim(context.Background(), "k", "fp"); !errors.Is(err, tenant.ErrMissing) {
		t.Fatalf("want ErrMissing, got %v", err)
	}
}
