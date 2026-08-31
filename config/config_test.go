package config_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Ocean-Gaming/platform-go/config"
)

type flakyLoader struct {
	snaps []map[string]config.TenantConfig
	errs  []error
	calls int
}

func (f *flakyLoader) Load(context.Context) (map[string]config.TenantConfig, error) {
	i := f.calls
	f.calls++
	if i < len(f.errs) && f.errs[i] != nil {
		return nil, f.errs[i]
	}
	if i < len(f.snaps) {
		return f.snaps[i], nil
	}
	return f.snaps[len(f.snaps)-1], nil
}

func TestGetBeforeReloadIsUnknownTenant(t *testing.T) {
	p := config.New(config.StaticLoader{})
	if _, err := p.Get("acme"); !errors.Is(err, config.ErrUnknownTenant) {
		t.Fatalf("want ErrUnknownTenant before any reload, got %v", err)
	}
}

func TestReloadThenRead(t *testing.T) {
	p := config.New(config.StaticLoader{Snapshot: map[string]config.TenantConfig{
		"acme": {TenantID: "acme", Brand: "Acme Casino", Currency: "EUR", Version: 1},
	}})
	if err := p.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	c, err := p.Get("acme")
	if err != nil {
		t.Fatal(err)
	}
	if c.Brand != "Acme Casino" {
		t.Fatalf("got %+v", c)
	}
}

func TestUnknownTenantFailsClosed(t *testing.T) {
	p := config.New(config.StaticLoader{Snapshot: map[string]config.TenantConfig{
		"acme": {TenantID: "acme"},
	}})
	_ = p.Reload(context.Background())
	if _, err := p.Get("not-a-tenant"); !errors.Is(err, config.ErrUnknownTenant) {
		t.Fatal("an unknown tenant must not resolve")
	}
}

// A tenant-config outage must not take this service down: keep serving the
// snapshot we already hold.
func TestFailedReloadKeepsPreviousSnapshot(t *testing.T) {
	good := map[string]config.TenantConfig{"acme": {TenantID: "acme", Brand: "v1", Version: 1}}
	l := &flakyLoader{
		snaps: []map[string]config.TenantConfig{good, nil},
		errs:  []error{nil, errors.New("tenant-config unreachable")},
	}
	p := config.New(l)

	if err := p.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.Reload(context.Background()); err == nil {
		t.Fatal("expected the loader error to surface")
	}

	c, err := p.Get("acme")
	if err != nil || c.Brand != "v1" {
		t.Fatalf("previous snapshot was dropped on a failed reload: %+v %v", c, err)
	}
	if p.Reloads() != 1 {
		t.Fatalf("failed reload counted as success: %d", p.Reloads())
	}
}

// ConfigChanged arriving repeatedly should swap the snapshot, not stampede.
func TestConfigChangedSwapsSnapshot(t *testing.T) {
	v1 := map[string]config.TenantConfig{"acme": {TenantID: "acme", Brand: "v1", Version: 1}}
	v2 := map[string]config.TenantConfig{"acme": {TenantID: "acme", Brand: "v2", Version: 2}}
	l := &flakyLoader{snaps: []map[string]config.TenantConfig{v1, v2}}
	p := config.New(l)

	_ = p.Reload(context.Background())
	_ = p.Reload(context.Background()) // simulates ConfigChanged

	c, _ := p.Get("acme")
	if c.Brand != "v2" || c.Version != 2 {
		t.Fatalf("snapshot not swapped: %+v", c)
	}
}
