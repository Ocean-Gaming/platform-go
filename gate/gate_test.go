package gate_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Ocean-Gaming/platform-go/gate"
)

// Rule 6, the whole point of this package: if the answer does not arrive, the
// answer is no.
func TestTimeoutDenies(t *testing.T) {
	slow := gate.CheckerFunc(func(ctx context.Context) (gate.Decision, error) {
		<-ctx.Done()
		return gate.Allow(), nil
	})
	d := gate.FailClosed(context.Background(), 20*time.Millisecond, slow)
	if d.Allowed {
		t.Fatal("a gate that timed out returned Allowed — licence breach")
	}
}

func TestErrorDenies(t *testing.T) {
	broken := gate.CheckerFunc(func(context.Context) (gate.Decision, error) {
		return gate.Allow(), errors.New("register unreachable")
	})
	// Note the checker returned Allow AND an error; the error must win.
	if gate.FailClosed(context.Background(), time.Second, broken).Allowed {
		t.Fatal("a gate that errored returned Allowed")
	}
}

func TestPanicDenies(t *testing.T) {
	panicky := gate.CheckerFunc(func(context.Context) (gate.Decision, error) {
		panic("nil map write")
	})
	if gate.FailClosed(context.Background(), time.Second, panicky).Allowed {
		t.Fatal("a panicking gate returned Allowed")
	}
}

func TestCancelledParentDenies(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	blocked := gate.CheckerFunc(func(ctx context.Context) (gate.Decision, error) {
		<-ctx.Done()
		return gate.Allow(), nil
	})
	if gate.FailClosed(ctx, time.Second, blocked).Allowed {
		t.Fatal("a cancelled gate returned Allowed")
	}
}

func TestAllowPassesThrough(t *testing.T) {
	ok := gate.CheckerFunc(func(context.Context) (gate.Decision, error) {
		return gate.Allow(), nil
	})
	if !gate.FailClosed(context.Background(), time.Second, ok).Allowed {
		t.Fatal("a healthy allow was denied")
	}
}

func TestExplicitDenyKeepsItsReason(t *testing.T) {
	d := gate.FailClosed(context.Background(), time.Second,
		gate.CheckerFunc(func(context.Context) (gate.Decision, error) {
			return gate.Deny("self-excluded"), nil
		}))
	if d.Allowed || d.Reason != "self-excluded" {
		t.Fatalf("reason lost: %+v", d)
	}
}

// A request that has already ended is not asked about: the checker never runs,
// so a register call is not made on behalf of a caller who has gone.
func TestAnEndedRequestDeniesWithoutRunningTheChecker(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancel2 := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel2()

	for name, c := range map[string]struct {
		ctx    context.Context
		reason string
	}{
		"cancelled": {cancelled, "gate cancelled"},
		"expired":   {expired, "gate timed out"},
	} {
		ran := false
		d := gate.FailClosed(c.ctx, time.Second, gate.CheckerFunc(func(context.Context) (gate.Decision, error) {
			ran = true
			return gate.Allow(), nil
		}))
		if d.Allowed || d.Reason != c.reason {
			t.Errorf("%s: %+v, want denied with %q", name, d, c.reason)
		}
		if ran {
			t.Errorf("%s: the checker ran for a request that had already ended", name)
		}
	}
}

// An Allow that arrives after the request was cancelled is denied. Without the
// check, select picked between the answer and the cancellation at random.
func TestAnAllowAfterCancellationIsDenied(t *testing.T) {
	for i := 0; i < 200; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		d := gate.FailClosed(ctx, time.Second, gate.CheckerFunc(func(context.Context) (gate.Decision, error) {
			cancel() // the caller goes away while the checker, ignoring ctx, says yes
			return gate.Allow(), nil
		}))
		if d.Allowed {
			t.Fatalf("run %d: an Allow returned after cancellation was accepted", i)
		}
	}
}
