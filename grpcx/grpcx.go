// Package grpcx carries the platform's gRPC conventions: metadata names, the
// tenant and recovery interceptors, and the mapping from platform errors to
// gRPC status codes. Every service wires these the same way, so the rules are
// written once.
//
// Two rules with reasons:
//
//   - The tenant interceptor is PERMISSIVE: it lifts x-tenant-id into the
//     context when present and enforces nothing — enforcement stays in
//     tenant.Require inside the command, exactly like the old HTTP
//     TenantFromHeader. Enforcing here would break grpc.health.v1 probes and
//     reflection, which send no metadata.
//   - Recovery is the OUTERMOST interceptor so it catches panics from every
//     later interceptor and handler (pg.InTx deliberately re-panics after
//     rollback).
package grpcx

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Ocean-Gaming/platform-go/config"
	"github.com/Ocean-Gaming/platform-go/idempotency"
	"github.com/Ocean-Gaming/platform-go/obs"
	"github.com/Ocean-Gaming/platform-go/tenant"
)

// Metadata keys, binding platform-wide. Set by the calling service's client;
// the tenant id is trusted only because services sit behind the gateway, which
// resolves it from the verified hostname and token — never expose a service
// built from this template directly to the internet.
const (
	MetaTenantID       = "x-tenant-id"
	MetaIdempotencyKey = "idempotency-key"
	MetaCorrelationID  = "x-correlation-id" // successor: W3C traceparent, once the mesh lands
)

// ServerOptions is the platform interceptor chain, defined ONCE so production
// and tests cannot drift. A test that builds its own chain proves nothing about
// the chain the service actually serves.
//
// Order is load-bearing: recovery is outermost so it catches panics from every
// later interceptor and handler.
func ServerOptions(log *slog.Logger) []grpc.ServerOption {
	return []grpc.ServerOption{grpc.ChainUnaryInterceptor(
		RecoveryInterceptor(log),
		TenantInterceptor(),
		CorrelationInterceptor(),
	)}
}

// TenantInterceptor lifts x-tenant-id into the context when present.
func TenantInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if v := firstMeta(ctx, MetaTenantID); v != "" {
			ctx = tenant.NewContext(ctx, tenant.ID(v))
		}
		return handler(ctx, req)
	}
}

// CorrelationInterceptor lifts x-correlation-id into the context so obs.LoggerFor
// can stamp it on every line. Without this the metadata key is a name nothing
// carries — declared, documented, and never propagated.
func CorrelationInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(obs.WithCorrelation(ctx, firstMeta(ctx, MetaCorrelationID)), req)
	}
}

// RecoveryInterceptor turns a panic into codes.Internal with a scrubbed
// message. Chain it first.
func RecoveryInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("panic in handler", "method", info.FullMethod, "panic", r)
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}

// IdempotencyKeyFromContext returns the idempotency-key metadata value, or "".
func IdempotencyKeyFromContext(ctx context.Context) string {
	return firstMeta(ctx, MetaIdempotencyKey)
}

// CorrelationIDFromContext returns the x-correlation-id metadata value, or "".
func CorrelationIDFromContext(ctx context.Context) string {
	return firstMeta(ctx, MetaCorrelationID)
}

func firstMeta(ctx context.Context, key string) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if vs := md.Get(key); len(vs) > 0 {
		return vs[0]
	}
	return ""
}

// Status maps platform errors to gRPC status codes:
//
//	tenant.ErrMissing            -> FAILED_PRECONDITION (tenant_missing)
//	config.ErrUnknownTenant      -> PERMISSION_DENIED   (fail closed)
//	idempotency.ErrFingerprint.. -> FAILED_PRECONDITION (fingerprint_mismatch; state-dependent, terminal)
//	idempotency.ErrInFlight      -> ABORTED             (in_flight; the one retryable conflict)
//	anything else                -> INTERNAL, message scrubbed
//
// NOT UNAUTHENTICATED for a missing tenant: that trips client credential
// machinery, and the metadata is infra-set, not a credential. Service-specific
// errors (validation, gate denials, domain rejections) are mapped by the
// service's own transport before falling back here.
func Status(err error) error {
	switch {
	case errors.Is(err, tenant.ErrMissing):
		return withReason(codes.FailedPrecondition, "no tenant id in metadata", "tenant_missing")
	case errors.Is(err, config.ErrUnknownTenant):
		return status.Error(codes.PermissionDenied, "unknown tenant")
	case errors.Is(err, idempotency.ErrFingerprintMismatch):
		return withReason(codes.FailedPrecondition, "idempotency key reused with a different request", "fingerprint_mismatch")
	case errors.Is(err, idempotency.ErrInFlight):
		return withReason(codes.Aborted, "an identical request is in flight", "in_flight")
	default:
		return status.Error(codes.Internal, "internal error")
	}
}

// withReason attaches an errdetails.ErrorInfo carrying a stable reason code.
func withReason(c codes.Code, msg, reason string) error {
	st := status.New(c, msg)
	if with, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason: reason,
		Domain: "ocean.platform",
	}); err == nil {
		st = with
	}
	return st.Err()
}

// AsInt64 reads a numeric value that may have round-tripped through stored
// JSON. Fresh command results carry int64; a REPLAYED result was unmarshaled
// from the idempotency store, where json.Decoder.UseNumber() yields
// json.Number. Anything else (notably float64, which loses precision above
// 2^53) is a bug surfaced loudly, never a silent zero — these are money
// amounts.
func AsInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	default:
		return 0, false
	}
}
