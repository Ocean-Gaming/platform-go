package stafftoken_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/Ocean-Gaming/platform-go/stafftoken"
)

// command is a request message with the attribution fields owners use.
func command(t *testing.T, changedBy, actor string) proto.Message {
	t.Helper()
	str := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	opt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: proto.String("cmd.proto"), Package: proto.String("t"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Cmd"), Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("changed_by"), Number: proto.Int32(1), Type: str, Label: opt, JsonName: proto.String("changedBy")},
			{Name: proto.String("actor"), Number: proto.Int32(2), Type: str, Label: opt, JsonName: proto.String("actor")},
		}}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	md := fd.Messages().Get(0)
	m := dynamicpb.NewMessage(md)
	m.Set(md.Fields().ByName("changed_by"), protoValue(changedBy))
	m.Set(md.Fields().ByName("actor"), protoValue(actor))
	return m
}

// keyPair is operations-service's signing key, published as a JWKS.
type keyPair struct {
	key *ecdsa.PrivateKey
	url string
}

func newKey(t *testing.T) keyPair {
	t.Helper()
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	enc := base64.RawURLEncoding.EncodeToString
	set, _ := json.Marshal(map[string]any{"keys": []map[string]string{{"kty": "EC", "crv": "P-256", "kid": "k1", "alg": "ES256",
		"x": enc(k.PublicKey.X.FillBytes(make([]byte, 32))), "y": enc(k.PublicKey.Y.FillBytes(make([]byte, 32)))}}})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(set) }))
	t.Cleanup(srv.Close)
	return keyPair{key: k, url: srv.URL}
}

func (k keyPair) sign(t *testing.T, c map[string]any) string {
	t.Helper()
	enc := base64.RawURLEncoding.EncodeToString
	h, _ := json.Marshal(map[string]string{"alg": "ES256", "kid": "k1", "typ": "JWT"})
	p, _ := json.Marshal(c)
	signed := enc(h) + "." + enc(p)
	d := sha256.Sum256([]byte(signed))
	r, s, _ := ecdsa.Sign(rand.Reader, k.key, d[:])
	return signed + "." + enc(append(r.FillBytes(make([]byte, 32)), s.FillBytes(make([]byte, 32))...))
}

func claims(sub, tid, aud string) map[string]any {
	now := time.Now()
	return map[string]any{"iss": "operations-service", "sub": sub, "aud": aud, "tid": tid,
		"iat": now.Unix(), "nbf": now.Unix(), "exp": now.Add(time.Minute).Unix()}
}

func call(t *testing.T, i grpc.UnaryServerInterceptor, method, tenant, token string, req proto.Message) codes.Code {
	t.Helper()
	pairs := []string{"x-tenant-id", tenant}
	if token != "" {
		pairs = append(pairs, "authorization", "Bearer "+token)
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(pairs...))
	_, err := i(ctx, req, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) { return "ok", nil })
	return status.Code(err)
}

// ADR-0060: a staff command reaches an owner only with operations-service's token, for this
// service, this tenant and this staff member. Anything else is refused; a player's command is not
// touched.
func TestAStaffCommandNeedsOperationsServicesTokenForThisServiceTenantAndStaffMember(t *testing.T) {
	ops, other := newKey(t), newKey(t)
	v, err := stafftoken.New(stafftoken.Config{JWKSURL: ops.url, Audience: "player-accounts"})
	if err != nil {
		t.Fatal(err)
	}
	i := stafftoken.Interceptor(v, stafftoken.Rule{})
	const m = "/playeraccounts.v1.PlayerAccounts/SuspendAccount"
	staffCmd := command(t, "staff:idn-1", "")
	good := ops.sign(t, claims("staff:idn-1", "acme", "player-accounts"))

	cases := []struct {
		name   string
		tenant string
		token  string
		req    proto.Message
		want   codes.Code
	}{
		{"a player's command, no token", "acme", "", command(t, "player", ""), codes.OK},
		{"the right token", "acme", good, staffCmd, codes.OK},
		{"no token", "acme", "", staffCmd, codes.Unauthenticated},
		{"another key", "acme", other.sign(t, claims("staff:idn-1", "acme", "player-accounts")), staffCmd, codes.Unauthenticated},
		{"another service", "acme", ops.sign(t, claims("staff:idn-1", "acme", "wallet-ledger")), staffCmd, codes.Unauthenticated},
		{"another tenant", "globex", good, staffCmd, codes.PermissionDenied},
		{"another staff member", "acme", ops.sign(t, claims("staff:idn-2", "acme", "player-accounts")), staffCmd, codes.PermissionDenied},
		{"expired", "acme", ops.sign(t, map[string]any{"iss": "operations-service", "sub": "staff:idn-1", "aud": "player-accounts",
			"tid": "acme", "exp": time.Now().Add(-time.Hour).Unix()}), staffCmd, codes.Unauthenticated},
		{"the attribution in another field", "acme", "", command(t, "", "staff:idn-1"), codes.Unauthenticated},
	}
	for _, c := range cases {
		if got := call(t, i, m, c.tenant, c.token, c.req); got != c.want {
			t.Errorf("%s: %s, want %s", c.name, got, c.want)
		}
	}
}

// A method that only a staff approval may call needs the token whatever its attribution says.
func TestAnAlwaysStaffMethodNeedsATokenWhateverItsAttribution(t *testing.T) {
	ops := newKey(t)
	v, _ := stafftoken.New(stafftoken.Config{JWKSURL: ops.url, Audience: "wallet-ledger"})
	const m = "/wallet.v1.WalletLedger/ApprovedManualAdjustment"
	i := stafftoken.Interceptor(v, stafftoken.Rule{Always: []string{m}})
	if got := call(t, i, m, "acme", "", command(t, "", "ops-tool")); got != codes.Unauthenticated {
		t.Fatalf("no token: %s", got)
	}
	if got := call(t, i, m, "acme", ops.sign(t, claims("staff:idn-1", "acme", "wallet-ledger")), command(t, "", "ops-tool")); got != codes.OK {
		t.Fatalf("with a token: %s", got)
	}
}

// Fail closed: an owner with no key set refuses every staff command and nothing else.
func TestAnOwnerWithNoKeySetRefusesEveryStaffCommand(t *testing.T) {
	v, err := stafftoken.New(stafftoken.Config{})
	if err != nil || v != nil {
		t.Fatalf("%v %v", v, err)
	}
	i := stafftoken.Interceptor(v, stafftoken.Rule{})
	if got := call(t, i, "/x/Y", "acme", "", command(t, "staff:idn-1", "")); got != codes.PermissionDenied {
		t.Fatalf("staff: %s", got)
	}
	if got := call(t, i, "/x/Y", "acme", "", command(t, "player", "")); got != codes.OK {
		t.Fatalf("player: %s", got)
	}
}

func protoValue(s string) protoreflect.Value { return protoreflect.ValueOfString(s) }

// An owner whose every non-player attribution is a staff command: "ops" is not a way around the
// token, and a token for staff:idn-1 does not cover changed_by "ops".
func TestAnOwnerWithNonStaffValuesTreatsEveryOtherAttributionAsStaff(t *testing.T) {
	ops := newKey(t)
	v, _ := stafftoken.New(stafftoken.Config{JWKSURL: ops.url, Audience: "player-accounts"})
	i := stafftoken.Interceptor(v, stafftoken.Rule{Fields: []string{"changed_by"}, NonStaff: []string{"player"}})
	const m = "/playeraccounts.v1.PlayerAccounts/SuspendAccount"
	tok := ops.sign(t, claims("staff:idn-1", "acme", "player-accounts"))
	if got := call(t, i, m, "acme", "", command(t, "player", "")); got != codes.OK {
		t.Fatalf("player: %s", got)
	}
	if got := call(t, i, m, "acme", "", command(t, "ops", "")); got != codes.Unauthenticated {
		t.Fatalf("ops without a token: %s", got)
	}
	if got := call(t, i, m, "acme", tok, command(t, "ops", "")); got != codes.PermissionDenied {
		t.Fatalf("ops with a staff token: %s", got)
	}
	if got := call(t, i, m, "acme", tok, command(t, "staff:idn-1", "")); got != codes.OK {
		t.Fatalf("staff with its token: %s", got)
	}
}
