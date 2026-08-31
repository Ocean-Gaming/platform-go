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
func (i ID) Valid() bool { return strings.TrimSpace(string(i)) != "" }

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
