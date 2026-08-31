//go:build integration

// Package pgtest wires integration tests to a real Postgres.
//
// These tests exist because the unit suite cannot prove atomicity. A fake can
// be made to return whatever we like; only a real database can demonstrate that
// a rollback actually leaves nothing behind.
package pgtest

import (
	"context"
	"database/sql"
	"io/fs"
	"os"
	"sort"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	platform "github.com/Ocean-Gaming/platform-go"
)

// DSN is the test database. Override with DATABASE_URL.
func DSN() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://localhost:5432/ocean_platform_test?sslmode=disable"
}

// Open returns a connected pool with the platform schema applied and every
// table truncated, so each test starts clean.
func Open(t *testing.T) *sql.DB { return OpenWith(t) }

// OpenWith is Open plus service-specific migrations, applied after the platform
// schema in lexical filename order. A service passes its own embedded
// migrations FS; the platform half comes from the module, so a service can
// never drift from the schema its platform code was written against.
func OpenWith(t *testing.T, extra ...fs.FS) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", DSN())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("ping %s: %v", DSN(), err)
	}

	// Truncate BEFORE applying, then let the migrations run: seed rows written
	// by a migration (currencies, reference data) are restored by the same
	// statement that created the table, because seeds are ON CONFLICT DO
	// NOTHING. Truncating afterwards would wipe seed data that other tables
	// have foreign keys to, and the alternative — a per-service list of tables
	// to spare — is the hardcoded list this replaced.
	truncateAll(t, db)
	apply(t, db, platform.Migrations())
	for _, e := range extra {
		apply(t, db, e)
	}

	t.Cleanup(func() { _ = db.Close() })
	return db
}

// truncateAll empties every table in the public schema. Whatever exists, rather
// than a hardcoded list: a service adds tables, and a list that silently misses
// one leaks state between tests.
func truncateAll(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.Query(
		`SELECT tablename FROM pg_tables WHERE schemaname = 'public'`)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table: %v", err)
		}
		tables = append(tables, `"`+name+`"`)
	}
	_ = rows.Close()
	if len(tables) > 0 {
		if _, err := db.Exec("TRUNCATE " + join(tables) + " CASCADE"); err != nil {
			t.Fatalf("truncate: %v", err)
		}
	}
}

func apply(t *testing.T, db *sql.DB, src fs.FS) {
	t.Helper()
	var files []string
	err := fs.WalkDir(src, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && len(p) > 4 && p[len(p)-4:] == ".sql" {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk migrations: %v", err)
	}
	sort.Strings(files) // 0001 before 0002; the ledger needs the platform tables
	for _, f := range files {
		b, err := fs.ReadFile(src, f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		if _, err := db.Exec(string(b)); err != nil {
			t.Fatalf("apply %s: %v", f, err)
		}
	}
}

func join(xs []string) string {
	out := xs[0]
	for _, x := range xs[1:] {
		out += ", " + x
	}
	return out
}

// Count returns the row count of a table.
func Count(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}
