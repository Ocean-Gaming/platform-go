// Package idempotency implements platform rule 3: every command carries an
// idempotency key, and the stored result is replayed on retry.
//
// The contract a caller sees:
//
//	rec, replay, err := store.Claim(ctx, tx, key, fingerprint)
//	if replay { return rec.Response }   // do NOT re-execute
//	... do the work, in the same tx ...
//	store.Complete(ctx, tx, key, response)
//
// Claim and Complete must run inside the SAME transaction as the state change
// they guard. That is what makes "the command ran" and "we recorded that the
// command ran" a single atomic fact.
package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/Ocean-Gaming/platform-go/tenant"
)

// Key is the caller-supplied idempotency key, unique per tenant.
type Key string

// State of a claimed key.
type State string

const (
	StateInFlight  State = "in_flight"
	StateCompleted State = "completed"
)

// Record is what the store holds for one key.
type Record struct {
	Tenant      tenant.ID
	Key         Key
	Fingerprint string
	Response    []byte
	State       State
	CreatedAt   time.Time
	CompletedAt time.Time
}

var (
	// ErrFingerprintMismatch means the key was reused for a materially
	// different request. This is a client bug and must not be papered over by
	// replaying the old response.
	ErrFingerprintMismatch = errors.New("idempotency: key reused with a different request")

	// ErrInFlight means a concurrent request holds the key and has not
	// completed. The caller should retry rather than execute in parallel.
	ErrInFlight = errors.New("idempotency: key already in flight")

	// ErrNotClaimed means Complete was called for a key that was never claimed.
	ErrNotClaimed = errors.New("idempotency: key was not claimed")
)

// Fingerprint derives a stable digest of a request body, so the same key with a
// different body can be detected.
func Fingerprint(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Store records claimed keys and their results.
//
// Implementations must scope every operation by tenant: two merchants may use
// the same key value and must never collide.
type Store interface {
	// Claim reserves key for this request. It returns replay=true together with
	// the stored record when the key has already completed, in which case the
	// caller must return the stored response and do no work.
	Claim(ctx context.Context, key Key, fingerprint string) (rec Record, replay bool, err error)

	// Complete stores the response for a previously claimed key.
	Complete(ctx context.Context, key Key, response []byte) error
}
