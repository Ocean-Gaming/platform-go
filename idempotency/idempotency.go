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
	"unicode/utf8"

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

	// ErrAlreadyCompleted means Complete was called for a key that already holds
	// a DIFFERENT response. The first answer stands: overwriting it would make a
	// later replay return something other than what the first caller was told.
	ErrAlreadyCompleted = errors.New("idempotency: key already completed with a different response")

	// ErrInvalidKey means the key is empty or longer than MaxKeyLength characters. The caller
	// chose it, so this is the caller's error, not the service's.
	ErrInvalidKey = errors.New("idempotency: key is empty or longer than 255 characters")

	// ErrInvalidFingerprint means the fingerprint is not a lowercase hex SHA-256. The service
	// derives it, normally with Fingerprint, so this is a wiring bug: a fingerprint of another
	// shape weakens the "same key, different request" check without anything noticing.
	ErrInvalidFingerprint = errors.New("idempotency: fingerprint is not a lowercase hex SHA-256")
)

// MaxKeyLength is the longest key accepted, in characters. migrations/0003 holds the column to it.
const MaxKeyLength = 255

// ValidFingerprint reports whether fp has the shape Fingerprint produces: exactly 64 characters
// of lowercase hex.
func ValidFingerprint(fp string) bool {
	if len(fp) != sha256.Size*2 {
		return false
	}
	for i := 0; i < len(fp); i++ {
		if c := fp[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// check applies the rules migrations/0003 enforces in Postgres, so both stores refuse the same
// claims. It runs after the tenant check, so a claim with no tenant is still ErrMissing.
func check(key Key, fingerprint string) error {
	if n := utf8.RuneCountInString(string(key)); n == 0 || n > MaxKeyLength {
		return ErrInvalidKey
	}
	if !ValidFingerprint(fingerprint) {
		return ErrInvalidFingerprint
	}
	return nil
}

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
	// caller must return the stored response and do no work. A key that is empty
	// or over MaxKeyLength characters is ErrInvalidKey; a fingerprint that is not
	// a lowercase hex SHA-256 is ErrInvalidFingerprint.
	Claim(ctx context.Context, key Key, fingerprint string) (rec Record, replay bool, err error)

	// Complete stores the response for a previously claimed key. Only an
	// in-flight key moves to completed. Completing it again with the same
	// response is a no-op that succeeds; with a different response it is
	// ErrAlreadyCompleted and the stored response is kept; an unknown key is
	// ErrNotClaimed.
	Complete(ctx context.Context, key Key, response []byte) error
}
