package grpcx

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Ocean-Gaming/platform-go/config"
	"github.com/Ocean-Gaming/platform-go/idempotency"
	"github.com/Ocean-Gaming/platform-go/obs"
	"github.com/Ocean-Gaming/platform-go/tenant"
)

func TestStatusMapping(t *testing.T) {
	cases := []struct {
		err    error
		code   codes.Code
		reason string
	}{
		{tenant.ErrMissing, codes.FailedPrecondition, "tenant_missing"},
		{config.ErrUnknownTenant, codes.PermissionDenied, ""},
		{idempotency.ErrFingerprintMismatch, codes.FailedPrecondition, "fingerprint_mismatch"},
		{idempotency.ErrInFlight, codes.Aborted, "in_flight"},
		{errors.New("some sql thing with a password in it"), codes.Internal, ""},
	}
	for _, c := range cases {
		st := status.Convert(Status(c.err))
		if st.Code() != c.code {
			t.Fatalf("%v -> %v, want %v", c.err, st.Code(), c.code)
		}
		if c.code == codes.Internal && st.Message() != "internal error" {
			t.Fatalf("internal message not scrubbed: %q", st.Message())
		}
		if c.reason != "" {
			found := ""
			for _, d := range st.Details() {
				if info, ok := d.(*errdetails.ErrorInfo); ok {
					found = info.Reason
				}
			}
			if found != c.reason {
				t.Fatalf("%v: reason = %q, want %q", c.err, found, c.reason)
			}
		}
	}
}

func TestTenantInterceptorIsPermissive(t *testing.T) {
	ic := TenantInterceptor()
	// With metadata: tenant lands in context.
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(MetaTenantID, "acme"))
	_, err := ic(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/x/Y"}, func(ctx context.Context, _ any) (any, error) {
		id, err := tenant.Require(ctx)
		if err != nil || id.String() != "acme" {
			t.Fatalf("tenant not lifted: %v %v", id, err)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Without metadata: the handler still runs (enforcement is tenant.Require's
	// job) — this is what keeps health probes working.
	ran := false
	_, err = ic(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/x/Y"}, func(ctx context.Context, _ any) (any, error) {
		ran = true
		if _, ok := tenant.FromContext(ctx); ok {
			t.Fatal("phantom tenant")
		}
		return nil, nil
	})
	if err != nil || !ran {
		t.Fatalf("handler must run without metadata: ran=%v err=%v", ran, err)
	}
}

func TestRecoveryInterceptorTurnsPanicsIntoInternal(t *testing.T) {
	ic := RecoveryInterceptor(slog.New(slog.NewTextHandler(testWriter{t}, nil)))
	_, err := ic(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/x/Y"}, func(context.Context, any) (any, error) {
		panic("tx rolled back")
	})
	st := status.Convert(err)
	if st.Code() != codes.Internal || st.Message() != "internal error" {
		t.Fatalf("panic -> %v %q", st.Code(), st.Message())
	}
}

// TestHealthWorksWithNoMetadata runs a real server with the full interceptor
// chain and calls Health/Check the way a kubelet does: no metadata at all.
func TestHealthWorksWithNoMetadata(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(
		RecoveryInterceptor(slog.New(slog.NewTextHandler(testWriter{t}, nil))),
		TenantInterceptor(),
	))
	hs := health.NewServer()
	healthv1.RegisterHealthServer(srv, hs)
	hs.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	resp, err := healthv1.NewHealthClient(conn).Check(context.Background(), &healthv1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("health check without metadata failed: %v", err)
	}
	if resp.Status != healthv1.HealthCheckResponse_SERVING {
		t.Fatalf("status %v", resp.Status)
	}
}

func TestAsInt64(t *testing.T) {
	if v, ok := AsInt64(int64(9_000_000_000_000_000_000)); !ok || v != 9_000_000_000_000_000_000 {
		t.Fatal("int64 passthrough")
	}
	if v, ok := AsInt64(json.Number("9007199254740993")); !ok || v != 9007199254740993 {
		t.Fatalf("json.Number above 2^53 must survive exactly, got %d %v", v, ok)
	}
	// float64 is refused loudly: it means someone decoded stored JSON without
	// UseNumber, and above 2^53 the value is already corrupt.
	if _, ok := AsInt64(float64(100)); ok {
		t.Fatal("float64 must be refused")
	}
	if _, ok := AsInt64("100"); ok {
		t.Fatal("string must be refused")
	}
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) { w.t.Log(string(p)); return len(p), nil }

// The correlation id was previously a declared metadata name that nothing
// carried: no interceptor read it, so obs could never stamp it. This proves the
// whole path — metadata -> context -> log field — is connected.
func TestCorrelationIDReachesTheContext(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(MetaCorrelationID, "corr-123"))

	var got string
	var found bool
	_, err := CorrelationInterceptor()(ctx, nil, &grpc.UnaryServerInfo{},
		func(ctx context.Context, _ any) (any, error) {
			got, found = obs.CorrelationFromContext(ctx)
			return nil, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if !found || got != "corr-123" {
		t.Fatalf("correlation id did not reach the handler: got=%q found=%v", got, found)
	}

	// Absent metadata must not fabricate one.
	_, _ = CorrelationInterceptor()(context.Background(), nil, &grpc.UnaryServerInfo{},
		func(ctx context.Context, _ any) (any, error) {
			if _, ok := obs.CorrelationFromContext(ctx); ok {
				t.Error("a correlation id appeared with no metadata")
			}
			return nil, nil
		})
}
