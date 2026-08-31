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
