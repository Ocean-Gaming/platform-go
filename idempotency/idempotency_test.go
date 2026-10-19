package idempotency_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
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

// Racing Completes of one key: exactly one response is stored, every other
// caller is told so, and the stored answer is the winner's.
func TestConcurrentCompletesKeepExactlyOneResponse(t *testing.T) {
	s := idempotency.NewMemoryStore()
	ctx := ctxFor("acme")
	if _, _, err := s.Claim(ctx, "k1", idempotency.Fingerprint([]byte("A"))); err != nil {
		t.Fatal(err)
	}

	const n = 16
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = s.Complete(ctx, "k1", []byte(fmt.Sprintf(`{"n":%d}`, i)))
		}(i)
	}
	wg.Wait()

	winner := -1
	for i, err := range errs {
		switch {
		case err == nil:
			if winner != -1 {
				t.Fatalf("workers %d and %d both completed the key", winner, i)
			}
			winner = i
		case !errors.Is(err, idempotency.ErrAlreadyCompleted):
			t.Fatalf("worker %d: want ErrAlreadyCompleted, got %v", i, err)
		}
	}
	if winner == -1 {
		t.Fatal("no Complete succeeded")
	}
	rec, replay, err := s.Claim(ctx, "k1", idempotency.Fingerprint([]byte("A")))
	if err != nil || !replay {
		t.Fatalf("replay=%v err=%v", replay, err)
	}
	if want := fmt.Sprintf(`{"n":%d}`, winner); string(rec.Response) != want {
		t.Fatalf("stored %q, want the winner's %q", rec.Response, want)
	}
}
