// Package tenant carries the tenant identity through a request.
//
// Platform rule 2: tenant_id is in every token, every row and every event
// header. Nothing below the transport layer should ever read a tenant from a
// request body or a query parameter — a client must not be able to choose its
// own tenant.
package tenant

import (
	"context"
	"errors"
	"strings"
)

// ID identifies one merchant tenant.
type ID string

func (i ID) String() string { return string(i) }

// Valid reports whether the id is well-formed. Empty or whitespace-only ids are
// rejected so a blank tenant can never silently become a wildcard.
//
// During the ADR-0127 cut-over both the UUID and the slug it replaces are
// valid; phase 5 of that programme makes this IsUUID.
func (i ID) Valid() bool { return strings.TrimSpace(string(i)) != "" }

// IsUUID reports whether the id is in the form ADR-0127 makes the tenant id:
// a canonical lowercase UUID, 8-4-4-4-12 hex digits. The slug form fails it.
// Services use this to tell the two apart while they accept both.
func (i ID) IsUUID() bool {
	s := string(i)
	if len(s) != 36 {
		return false
	}
	for j := 0; j < 36; j++ {
		c := s[j]
		switch j {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
				return false
			}
		}
	}
	return true
}

// ErrMissing is returned when a code path that requires a tenant is reached
// without one in context. Callers should treat this as a fail-closed condition,
// never as "use a default tenant".
var ErrMissing = errors.New("tenant: no tenant id in context")

type ctxKey struct{}

// NewContext returns a copy of ctx carrying id.
func NewContext(ctx context.Context, id ID) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// FromContext returns the tenant carried by ctx, if any.
func FromContext(ctx context.Context) (ID, bool) {
	id, ok := ctx.Value(ctxKey{}).(ID)
	if !ok || !id.Valid() {
		return "", false
	}
	return id, true
}

// Require returns the tenant carried by ctx, or ErrMissing.
//
// Use this at every boundary that touches storage or publishes an event. A
// query that runs without a tenant filter is how a whitelabel platform leaks
// one merchant's data into another's.
func Require(ctx context.Context) (ID, error) {
	id, ok := FromContext(ctx)
	if !ok {
		return "", ErrMissing
	}
	return id, nil
}
