// Package gate implements platform rule 6: licence-relevant gates fail closed.
//
// If the answer does not arrive, the answer is no. Every timeout, transport
// error, panic and unknown state resolves to Deny. There is deliberately no
// option to configure this the other way — a gate that fails open under load is
// a licence breach that only shows up in an audit.
package gate

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Decision is the outcome of a gate check.
type Decision struct {
	Allowed bool
	// Reason is safe to log and to surface to operators. It must never contain
	// personal data.
	Reason string
}

// Allow is the only way to construct a permitted decision.
func Allow() Decision { return Decision{Allowed: true, Reason: "allowed"} }

// Deny constructs a refusal.
func Deny(reason string) Decision { return Decision{Allowed: false, Reason: reason} }

// Checker answers one licence-relevant question.
type Checker interface {
	Check(ctx context.Context) (Decision, error)
}

// None is the explicit "this command has no licence-relevant gate" Checker.
//
// It exists so that absence is a VALUE rather than a nil, because those two
// must not look alike. "This service needs no gate" and "someone forgot to
// wire the gate" would otherwise be the same state. Note what a nil Gate does
// NOT do here: it does not fail open — FailClosed's recover turns the
// nil-interface call into Deny. It denies EVERY command instead, which is a
// total outage found in production. Requiring None() turns that into a panic
// at boot, and makes the exemption greppable across all 36 services at audit
// time.
//
// Do not use None to work around a gate that is failing: it always allows.
func None() Checker {
	return CheckerFunc(func(context.Context) (Decision, error) {
		return Decision{Allowed: true, Reason: "no licence gate for this command"}, nil
	})
}

// CheckerFunc adapts a function to Checker.
type CheckerFunc func(ctx context.Context) (Decision, error)

func (f CheckerFunc) Check(ctx context.Context) (Decision, error) { return f(ctx) }

// FailClosed runs c with a timeout and converts every abnormal outcome into a
// denial. It never returns an error: the caller gets a Decision it can act on,
// which removes the "forgot to handle the error and continued" failure mode.
func FailClosed(ctx context.Context, timeout time.Duration, c Checker) (d Decision) {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// A panic in a gate must deny, not crash the request into an ambiguous state.
	defer func() {
		if r := recover(); r != nil {
			d = Deny(fmt.Sprintf("gate panicked: %v", r))
		}
	}()

	type result struct {
		d   Decision
		err error
	}
	ch := make(chan result, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				ch <- result{Deny(fmt.Sprintf("gate panicked: %v", r)), nil}
			}
		}()
		dd, err := c.Check(ctx)
		ch <- result{dd, err}
	}()

	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return Deny("gate timed out")
		}
		return Deny("gate cancelled")
	case r := <-ch:
		if r.err != nil {
			return Deny("gate errored: " + r.err.Error())
		}
		return r.d
	}
}
