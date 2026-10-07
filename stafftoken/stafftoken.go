// Package stafftoken is how an owner service knows a staff command really came from
// operations-service, the back office's maker-checker (meta-repo ADR-0060).
//
// A staff command is one whose attribution names a staff member: a `changed_by`, `actor` or
// `requested_by` of "staff:<id>". Every such command reaches an owner only after operations-service
// has approved it, and operations-service signs each call with a one-minute ES256 token: issuer
// operations-service, audience the owner, `sub` the requester, `tid` the tenant. [Interceptor]
// refuses a staff command without one, with one signed by another key, for another service, another
// tenant or another staff member. Network position is not a credential: a container on the network
// that is not operations-service cannot suspend an account or adjust a balance.
//
// Fail closed: an owner with no key set configured refuses every staff command rather than trusting
// the network. Commands attributed to a player or the system are untouched.
//
// The verification is the standard library's: ES256 against a JWKS fetched over HTTP, so this module
// takes no JWT dependency into every service.
package stafftoken

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Issuer is operations-service's token issuer.
const Issuer = "operations-service"

// ErrInvalid is every reason a token is not accepted, deliberately undifferentiated on the wire.
var ErrInvalid = errors.New("stafftoken: token not accepted")

// Config is one owner's verifier.
type Config struct {
	JWKSURL  string        // STAFF_TOKEN_JWKS_URL; "" = not configured, every staff command refused
	Issuer   string        // STAFF_TOKEN_ISSUER, default operations-service
	Audience string        // this service's name, which operations-service mints for
	Refresh  time.Duration // how long a fetched key set is trusted before it is fetched again; default 5 min
	Leeway   time.Duration // clock skew allowed on exp, nbf and iat; default 30 s
	Client   *http.Client
	Now      func() time.Time
}

// FromEnv reads STAFF_TOKEN_JWKS_URL and STAFF_TOKEN_ISSUER for a service named audience.
func FromEnv(audience string) Config {
	return Config{JWKSURL: strings.TrimSpace(os.Getenv("STAFF_TOKEN_JWKS_URL")),
		Issuer: strings.TrimSpace(os.Getenv("STAFF_TOKEN_ISSUER")), Audience: audience}
}

// Verifier checks operations-service's tokens. A nil *Verifier is "not configured".
type Verifier struct {
	c Config

	mu      sync.Mutex
	keys    map[string]*ecdsa.PublicKey
	fetched time.Time
}

// New builds a verifier, or nil when no key set is configured.
func New(c Config) (*Verifier, error) {
	if c.JWKSURL == "" {
		return nil, nil
	}
	if c.Audience == "" {
		return nil, errors.New("stafftoken: an audience (this service's name) is required")
	}
	if c.Issuer == "" {
		c.Issuer = Issuer
	}
	if c.Refresh <= 0 {
		c.Refresh = 5 * time.Minute
	}
	if c.Leeway <= 0 {
		c.Leeway = 30 * time.Second
	}
	if c.Client == nil {
		c.Client = &http.Client{Timeout: 5 * time.Second}
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return &Verifier{c: c}, nil
}

// Claims are what a verified token says.
type Claims struct {
	Subject string // staff:<id>, the requester
	Tenant  string
}

type header struct {
	Alg string `json:"alg"`
	Kid string `json:"kid"`
}

type claims struct {
	Iss string          `json:"iss"`
	Sub string          `json:"sub"`
	Aud json.RawMessage `json:"aud"`
	Tid string          `json:"tid"`
	Exp int64           `json:"exp"`
	Nbf int64           `json:"nbf"`
	Iat int64           `json:"iat"`
}

// Verify checks the signature, then the claims.
func (v *Verifier) Verify(ctx context.Context, raw string) (Claims, error) {
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return Claims{}, fmt.Errorf("%w: not a compact JWS", ErrInvalid)
	}
	var h header
	if err := decode(parts[0], &h); err != nil {
		return Claims{}, err
	}
	if h.Alg != "ES256" { // "none", HMAC and RSA fall out here: alg confusion has nowhere to go
		return Claims{}, fmt.Errorf("%w: alg %q", ErrInvalid, h.Alg)
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(sig) != 64 {
		return Claims{}, fmt.Errorf("%w: an ES256 signature is 64 bytes", ErrInvalid)
	}
	key, err := v.key(ctx, h.Kid)
	if err != nil {
		return Claims{}, err
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if !ecdsa.Verify(key, digest[:], new(big.Int).SetBytes(sig[:32]), new(big.Int).SetBytes(sig[32:])) {
		return Claims{}, fmt.Errorf("%w: bad signature", ErrInvalid)
	}
	var c claims
	if err := decode(parts[1], &c); err != nil {
		return Claims{}, err
	}
	now := v.c.Now()
	switch {
	case c.Iss != v.c.Issuer:
		return Claims{}, fmt.Errorf("%w: issuer %q", ErrInvalid, c.Iss)
	case !audience(c.Aud, v.c.Audience):
		return Claims{}, fmt.Errorf("%w: not for %s", ErrInvalid, v.c.Audience)
	case c.Exp == 0 || now.After(time.Unix(c.Exp, 0).Add(v.c.Leeway)):
		return Claims{}, fmt.Errorf("%w: expired", ErrInvalid)
	case c.Nbf != 0 && now.Add(v.c.Leeway).Before(time.Unix(c.Nbf, 0)):
		return Claims{}, fmt.Errorf("%w: not yet valid", ErrInvalid)
	case c.Iat != 0 && now.Add(v.c.Leeway).Before(time.Unix(c.Iat, 0)):
		return Claims{}, fmt.Errorf("%w: issued in the future", ErrInvalid)
	case !strings.HasPrefix(c.Sub, "staff:") || c.Tid == "":
		return Claims{}, fmt.Errorf("%w: no staff subject or tenant", ErrInvalid)
	}
	return Claims{Subject: c.Sub, Tenant: c.Tid}, nil
}

func decode(part string, into any) error {
	b, err := base64.RawURLEncoding.DecodeString(part)
	if err != nil {
		return fmt.Errorf("%w: not base64url", ErrInvalid)
	}
	if err := json.Unmarshal(b, into); err != nil {
		return fmt.Errorf("%w: not JSON", ErrInvalid)
	}
	return nil
}

// audience accepts both encodings RFC 7519 permits for aud.
func audience(raw json.RawMessage, want string) bool {
	var one string
	if json.Unmarshal(raw, &one) == nil {
		return one == want
	}
	var many []string
	if json.Unmarshal(raw, &many) == nil {
		for _, a := range many {
			if a == want {
				return true
			}
		}
	}
	return false
}

// key finds a key by id, fetching the set when it is stale or the id is new. A restart of
// operations-service rotates its key, so an unknown kid is a reason to fetch, at most every 10 s.
func (v *Verifier) key(ctx context.Context, kid string) (*ecdsa.PublicKey, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	now := v.c.Now()
	k, ok := v.keys[kid]
	stale := now.Sub(v.fetched) > v.c.Refresh
	if ok && !stale {
		return k, nil
	}
	if !stale && !ok && now.Sub(v.fetched) < 10*time.Second {
		return nil, fmt.Errorf("%w: unknown key %q", ErrInvalid, kid)
	}
	keys, err := v.fetch(ctx)
	if err != nil {
		if ok { // a stale but known key beats an outage of the key server
			return k, nil
		}
		return nil, err
	}
	v.keys, v.fetched = keys, now
	if k, ok = keys[kid]; !ok {
		return nil, fmt.Errorf("%w: unknown key %q", ErrInvalid, kid)
	}
	return k, nil
}

func (v *Verifier) fetch(ctx context.Context) (map[string]*ecdsa.PublicKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.c.JWKSURL, nil)
	if err != nil {
		return nil, err
	}
	res, err := v.c.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("stafftoken: fetch key set: %w", err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("stafftoken: fetch key set: HTTP %d", res.StatusCode)
	}
	var set struct {
		Keys []struct {
			Kty, Crv, Kid, X, Y string
		} `json:"keys"`
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&set); err != nil {
		return nil, fmt.Errorf("stafftoken: key set is not JSON: %w", err)
	}
	out := map[string]*ecdsa.PublicKey{}
	for _, k := range set.Keys {
		if k.Kty != "EC" || k.Crv != "P-256" {
			continue
		}
		x, errX := base64.RawURLEncoding.DecodeString(k.X)
		y, errY := base64.RawURLEncoding.DecodeString(k.Y)
		if errX != nil || errY != nil || len(x) > 32 || len(y) > 32 {
			continue
		}
		// The JWK's coordinates as the SEC 1 uncompressed point (0x04 ‖ X ‖ Y, each 32 bytes for
		// P-256). ParseUncompressedPublicKey performs the on-curve check; setting PublicKey.X/.Y
		// and calling Curve.IsOnCurve is deprecated (staticcheck SA1019 on Go 1.26).
		point := make([]byte, 65)
		point[0] = 4
		copy(point[33-len(x):33], x)
		copy(point[65-len(y):], y)
		pub, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), point)
		if err != nil {
			continue
		}
		out[k.Kid] = pub
	}
	if len(out) == 0 {
		return nil, errors.New("stafftoken: the key set has no P-256 key")
	}
	return out, nil
}

// Rule says which commands are staff commands.
type Rule struct {
	// Fields are the attribution fields read off a request, in order; the first holding "staff:…"
	// names the staff member the token must be for. Default: changed_by, actor, requested_by.
	Fields []string
	// Always are full method names that are staff commands whatever their attribution says, such as
	// wallet-ledger's ApprovedManualAdjustment, which only a staff approval may call.
	Always []string
	// NonStaff are the attribution values that are NOT staff (e.g. "player"), for an owner that
	// treats every other value as a staff command. Set, any other non-empty attribution needs a
	// token whose sub equals it, so "changed_by: ops" cannot slip past as not-quite-staff.
	NonStaff []string
}

// Interceptor refuses a staff command that does not carry operations-service's token for this
// service, this tenant and this staff member. Chain it after grpcx.ServerOptions' interceptors.
func Interceptor(v *Verifier, r Rule) grpc.UnaryServerInterceptor {
	fields := r.Fields
	if len(fields) == 0 {
		fields = []string{"changed_by", "actor", "requested_by"}
	}
	always := map[string]bool{}
	for _, m := range r.Always {
		always[m] = true
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		staff := attribution(req, fields, r.NonStaff)
		if staff == "" && !always[info.FullMethod] {
			return handler(ctx, req)
		}
		if v == nil {
			return nil, status.Error(codes.PermissionDenied,
				"staff commands need operations-service's token, and STAFF_TOKEN_JWKS_URL is not set")
		}
		md, _ := metadata.FromIncomingContext(ctx)
		bearer := ""
		if vs := md.Get("authorization"); len(vs) > 0 && strings.HasPrefix(strings.ToLower(vs[0]), "bearer ") {
			bearer = strings.TrimSpace(vs[0][len("bearer "):])
		}
		if bearer == "" {
			return nil, status.Error(codes.Unauthenticated, "a staff command needs operations-service's token")
		}
		c, err := v.Verify(ctx, bearer)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "the staff token was not accepted")
		}
		if tids := md.Get("x-tenant-id"); len(tids) == 0 || tids[0] != c.Tenant {
			return nil, status.Error(codes.PermissionDenied, "the staff token is for another tenant")
		}
		if staff != "" && c.Subject != staff {
			return nil, status.Error(codes.PermissionDenied, "the staff token is for another staff member")
		}
		return handler(ctx, req)
	}
}

// attribution is the staff subject a proto request names: the first "staff:…" value among fields,
// or, when nonStaff is given, the first non-empty value not in it. "" when none.
func attribution(req any, fields, nonStaff []string) string {
	m, ok := req.(proto.Message)
	if !ok {
		return ""
	}
	r := m.ProtoReflect()
	desc := r.Descriptor().Fields()
	for _, name := range fields {
		fd := desc.ByName(protoreflect.Name(name))
		if fd == nil || fd.Kind() != protoreflect.StringKind || fd.IsList() {
			continue
		}
		s := r.Get(fd).String()
		if strings.HasPrefix(s, "staff:") {
			return s
		}
		if s != "" && len(nonStaff) > 0 && !slices.Contains(nonStaff, s) {
			return s
		}
	}
	return ""
}
