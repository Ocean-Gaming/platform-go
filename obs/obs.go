// Package obs wires structured logging and request identity.
//
// Platform requirement: tenant_id and a correlation id propagate to every
// downstream call and appear on every log line, so a single player's journey
// can be followed across services without joining databases.
package obs

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/Ocean-Gaming/platform-go/tenant"
)

type correlationKey struct{}

// NewLogger returns a JSON logger at the given level.
func NewLogger(service string, level slog.Level) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(h).With("service", service)
}

// ParseLevel turns a LOG_LEVEL value into a level. Empty means info.
//
// It rejects what it does not understand instead of quietly returning info,
// because an operator who asks for debug and silently gets info debugs the
// wrong thing. What the caller does with the error is the caller's policy;
// NewLoggerFromEnv warns and carries on.
func ParseLevel(raw string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("%q is not debug, info, warn or error", raw)
	}
}

// NewLoggerFromEnv returns the service logger at the level LOG_LEVEL asks for.
//
// This is the constructor a service should call. NewLogger takes a level and
// every caller passed slog.LevelInfo, so LOG_LEVEL was set in every compose
// file and deployment manifest on the platform and honoured by almost none of
// them — a knob that looked connected and was not.
//
// It logs two lines of its own, which is unusual for a constructor and
// deliberate:
//
//   - a WARN when LOG_LEVEL is not understood, so a typo is visible rather
//     than silently downgrading to info;
//   - an INFO naming the level in force, so "is LOG_LEVEL being honoured here"
//     is answerable from the logs instead of from the source. Finding that out
//     by reading main.go, during an incident, is how this function came to
//     exist.
//
// A bad value is not fatal. Refusing to boot over an observability setting —
// most likely mistyped by someone raising the level mid-incident — turns a
// logging mistake into an outage.
func NewLoggerFromEnv(service string) *slog.Logger {
	level, err := ParseLevel(os.Getenv("LOG_LEVEL"))
	log := NewLogger(service, level)
	if err != nil {
		log.Warn("LOG_LEVEL not understood; logging at info", "err", err)
	}
	log.Info("logging", "level", level.String())
	return log
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
