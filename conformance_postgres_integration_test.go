//go:build integration

package platform_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/Ocean-Gaming/platform-go/idempotency"
	"github.com/Ocean-Gaming/platform-go/inbox"
	"github.com/Ocean-Gaming/platform-go/pg"
	"github.com/Ocean-Gaming/platform-go/pgtest"
	"github.com/Ocean-Gaming/platform-go/platformtest"
)

// The SAME bodies, against real SQL. Divergence between the two implementations
// is now a test failure rather than a production incident.
func TestPostgresConformance(t *testing.T) {
	db := pgtest.Open(t)
	platformtest.RunConformance(t, platformtest.Harness{
		Name: "postgres",
		Do: func(ctx context.Context, fn func(idempotency.Store, inbox.Store) error) error {
			// One transaction per unit of work: the dedup row and the handler's
			// writes must commit together, which is the whole point of rule 5.
			return pg.InTx(ctx, db, func(tx *sql.Tx) error {
				return fn(idempotency.NewPostgresStore(tx), inbox.NewPostgresStore(tx))
			})
		},
	})
}
