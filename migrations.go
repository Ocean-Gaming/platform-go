// Package platform carries the Ocean platform primitives every service inherits:
// tenant propagation, idempotency, the transactional outbox, the consumer inbox,
// the tenant config snapshot, fail-closed licence gates, observability, and the
// gRPC wire conventions.
//
// The schema ships WITH the code, embedded. That is not tidiness — the two are
// one artifact. The inbox dedup bug proved it: a tenant-blind PRIMARY KEY and an
// ON CONFLICT arbiter that matched it were a single defect spread across a .sql
// file and a .go file. Split them across a version boundary and `go get -u`
// becomes a way to silently break a service's conflict target.
package platform

import "embed"

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrations returns the platform schema, to be applied before any
// service-specific migration. Filenames sort lexically in apply order.
func Migrations() embed.FS { return migrationsFS }
