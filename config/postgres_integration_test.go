//go:build integration

package config_test

import (
	"context"
	"testing"

	"github.com/Ocean-Gaming/platform-go/config"
	"github.com/Ocean-Gaming/platform-go/pgtest"
)

// Rule 7's read path over real SQL. PostgresLoader was constructed by nothing
// and executed by no test, so nothing had ever written or read
// tenant_config_snapshot — the table the migration creates and pgtest truncates.
func TestPostgresLoaderReadsTheSnapshot(t *testing.T) {
	db := pgtest.Open(t)
	ctx := context.Background()

	// Rule 8 — two tenants, and they must be distinguishable, or a test that
	// returns the wrong one still looks right.
	for _, tc := range []struct{ id, doc string }{
		{"acme", `{"brand":"Acme Casino","currency":"EUR","locale":"en-GB","limits":{"max":"1000"}}`},
		{"globex", `{"brand":"Globex Bet","currency":"SEK","locale":"sv-SE"}`},
	} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO tenant_config_snapshot (tenant_id, version, document)
			VALUES ($1, 7, $2::jsonb)`, tc.id, tc.doc); err != nil {
			t.Fatal(err)
		}
	}

	p := config.New(config.PostgresLoader{DB: db})
	if err := p.Reload(ctx); err != nil {
		t.Fatal(err)
	}

	acme, err := p.Get("acme")
	if err != nil {
		t.Fatal(err)
	}
	if acme.Currency != "EUR" || acme.Brand != "Acme Casino" {
		t.Fatalf("document did not round-trip: %+v", acme)
	}
	// The column wins over anything in the document — the row is the authority.
	if acme.Version != 7 || acme.TenantID != "acme" {
		t.Fatalf("id/version not taken from the columns: %+v", acme)
	}
	if acme.Limits["max"] != "1000" {
		t.Fatalf("nested map lost: %+v", acme.Limits)
	}
	if g, _ := p.Get("globex"); g.Currency != "SEK" {
		t.Fatalf("tenants collapsed into one: %+v", g)
	}

	// Rule 6 — an unknown tenant fails closed rather than returning a zero value.
	if _, err := p.Get("not-a-tenant"); err == nil {
		t.Fatal("a tenant absent from the snapshot was served")
	}
}

// A config service outage must not take the platform down: the previous
// snapshot keeps serving. This is the half that matters in an incident.
func TestReloadFailureKeepsThePreviousSnapshot(t *testing.T) {
	db := pgtest.Open(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO tenant_config_snapshot (tenant_id, version, document)
		VALUES ('acme', 1, '{"currency":"EUR"}'::jsonb)`); err != nil {
		t.Fatal(err)
	}
	p := config.New(config.PostgresLoader{DB: db})
	if err := p.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	_ = db.Close() // the "config database is gone" case

	if err := p.Reload(ctx); err == nil {
		t.Fatal("a reload against a closed database reported success")
	}
	if c, err := p.Get("acme"); err != nil || c.Currency != "EUR" {
		t.Fatalf("a failed reload dropped the working snapshot: %+v %v", c, err)
	}
}
