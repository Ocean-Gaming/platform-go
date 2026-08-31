// Package obs wires structured logging and request identity.
//
// Platform requirement: tenant_id and a correlation id propagate to every
// downstream call and appear on every log line, so a single player's journey
// can be followed across services without joining databases.
package obs

import (
	"context"
	"log/slog"
	"os"

	"github.com/Ocean-Gaming/platform-go/tenant"
)

type correlationKey struct{}

// NewLogger returns a JSON logger at the given level.
func NewLogger(service string, level slog.Level) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(h).With("service", service)
}

// WithCorrelation attaches a correlation id to ctx. Set by grpcx's interceptor
// from the x-correlation-id metadata; W3C traceparent supersedes this once the
// mesh lands (LAP-254).
func WithCorrelation(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, correlationKey{}, id)
}

// CorrelationFromContext returns the correlation id carried by ctx.
func CorrelationFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(correlationKey{}).(string)
	return id, ok && id != ""
}

// LoggerFor returns a logger pre-tagged with whatever identity ctx carries.
// Using this instead of the bare logger is what keeps tenant_id on every line.
func LoggerFor(ctx context.Context, base *slog.Logger) *slog.Logger {
	l := base
	if tid, ok := tenant.FromContext(ctx); ok {
		l = l.With("tenant_id", tid.String())
	}
	if id, ok := CorrelationFromContext(ctx); ok {
		l = l.With("correlation_id", id)
	}
	return l
}
