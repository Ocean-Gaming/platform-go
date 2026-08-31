// Package config implements platform rule 7: tenant config is read from a local
// snapshot, invalidated by ConfigChanged.
//
// Nothing here makes a request-time call to tenant-config. A config read is a
// map lookup against a snapshot this service already holds, which is why config
// reads are not listed as synchronous calls anywhere in the architecture docs.
package config

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// TenantConfig is the per-tenant document this service cares about. Services
// narrow this to the fields they actually use rather than carrying everything.
type TenantConfig struct {
	TenantID string            `json:"tenant_id"`
	Brand    string            `json:"brand"`
	Currency string            `json:"currency"`
	Locale   string            `json:"locale"`
	Limits   map[string]string `json:"limits"`
	Version  int64             `json:"version"`
}

// ErrUnknownTenant is returned when a tenant is absent from the snapshot.
// Callers must treat this as fail-closed: an unknown tenant is not a permitted
// tenant.
var ErrUnknownTenant = errors.New("config: tenant not present in snapshot")

// Loader fetches a full snapshot. Called on boot and on ConfigChanged, never
// per request.
type Loader interface {
	Load(ctx context.Context) (map[string]TenantConfig, error)
}

// Provider holds the current snapshot and swaps it atomically.
type Provider struct {
	loader Loader
	snap   atomic.Pointer[map[string]TenantConfig]

	mu      sync.Mutex // serialises reloads, so a burst of ConfigChanged does not stampede
	reloads atomic.Int64
}

func New(loader Loader) *Provider { return &Provider{loader: loader} }

// Reload replaces the snapshot. Call once at boot, then on every ConfigChanged.
func (p *Provider) Reload(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	m, err := p.loader.Load(ctx)
	if err != nil {
		// Keep serving the previous snapshot. A config service outage must not
		// take the platform down; it just freezes config until it returns.
		return err
	}
	p.snap.Store(&m)
	p.reloads.Add(1)
	return nil
}

// Get returns the config for a tenant from the current snapshot.
func (p *Provider) Get(tenantID string) (TenantConfig, error) {
	m := p.snap.Load()
	if m == nil {
		return TenantConfig{}, ErrUnknownTenant
	}
	c, ok := (*m)[tenantID]
	if !ok {
		return TenantConfig{}, ErrUnknownTenant
	}
	return c, nil
}

// Reloads reports how many successful reloads have happened, for assertions and
// for a readiness metric.
func (p *Provider) Reloads() int64 { return p.reloads.Load() }

// StaticLoader is a fixed snapshot, for tests and local development.
type StaticLoader struct{ Snapshot map[string]TenantConfig }

func (s StaticLoader) Load(context.Context) (map[string]TenantConfig, error) {
	return s.Snapshot, nil
}
