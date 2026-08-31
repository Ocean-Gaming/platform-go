# platform-go

The Ocean platform primitives every service inherits, as a **versioned Go module**
rather than copied source.

Ocean is a multi-merchant whitelabel iGaming platform: 36 Go services, one repo each.
This module carries the parts that must be *identical* across all of them.

## Why a module and not a template

`service-template` used to carry these packages, and every service copied them. After
exactly one service was scaffolded the two copies had already diverged by 66 non-import
lines, and two real fixes had to be applied by hand, twice, seconds apart:

    fix(inbox): scope event dedup by tenant     -> applied to template and wallet-ledger
    fix(gate):  a nil Gate must not fail open   -> applied to template and wallet-ledger

At 36 services that is 36 hand-applied fixes to a cross-tenant isolation bug a licence
audit will ask about by name — drifting as they go. The two repos had *already* reached
different answers to rule 6.

`grpcx` settles the argument on its own: the metadata names and the error→status mapping
are a **wire contract between services**. If service A maps `ErrInFlight` to `ABORTED` and
service B maps it to `FAILED_PRECONDITION`, no client can retry generically — and no
amount of copying enforces agreement.

A template is for what each service *should change*. This is for what none of them may.

## What is here

| Package | Rule it makes free |
|---|---|
| `tenant` | 2 — `tenant_id` in every token, row and event header |
| `idempotency` | 3 — every command carries a key; the stored result replays |
| `outbox` | 4 — state changes write to an outbox in the same transaction |
| `inbox` | 5 — consumers dedup on event id in their own transaction |
| `config` | 7 — tenant config from a local snapshot |
| `gate` | 6 — licence gates fail closed |
| `grpcx` | the wire contract: metadata names, interceptor chain, error→status |
| `obs` | structured logging carrying tenant and correlation id |
| `pg` | `InTx`, the transaction seam |
| `pgtest`, `platformtest` | the test harness and the conformance suite |

## The schema ships with the code

`migrations/0001_platform.sql` is **embedded** and exposed as `platform.Migrations()`.
That is not tidiness. The inbox bug was a tenant-blind `PRIMARY KEY` and an `ON CONFLICT`
arbiter that matched it — one defect spread across a `.sql` file and a `.go` file. Split
them across a version boundary and `go get -u` becomes a way to silently break a
service's conflict target.

## The conformance suite

`platformtest.RunConformance` is the reason this module exists in its current shape.
It runs **one set of test bodies against every implementation** of `idempotency.Store`
and `inbox.Store` — the fakes unconditionally, Postgres under the `integration` tag.

The bug it was written for: `inbox.MemoryStore` keyed dedup on tenant+consumer+event
while the SQL `PRIMARY KEY` omitted `tenant_id`. The unit test asserting cross-tenant
isolation passed, because it could only ever exercise the map. Production silently
dropped one tenant's event — no error, no row, no retry.

Reintroduce that bug today and the memory run stays fully green while the *same body*
fails against Postgres:

    --- PASS: TestMemoryConformance/memory/inbox/the_same_event_id_across_tenants_runs_both_handlers
    --- FAIL: TestPostgresConformance/postgres/inbox
        one tenant suppressed another's event: handler ran 1 times, want 2

Every service should run both harnesses. See `conformance_memory_test.go` and
`conformance_postgres_integration_test.go`.

## Using it

    go get github.com/Ocean-Gaming/platform-go

This is a **private** module, so `go` needs credentials:

    export GOPRIVATE='github.com/Ocean-Gaming/*'

Locally that is enough if `gh` has configured a git credential helper. In CI see the
`platform module` step in a consuming repo's `ci.yml`.

## Tests

    go test ./...                                  # no Docker, no database
    createdb ocean_platform_test
    DATABASE_URL='postgres://localhost:5432/ocean_platform_test?sslmode=disable' \
      go test -tags=integration -p 1 ./...

`-p 1` is required: the integration harness shares one database and truncates between
tests, so parallel packages would wipe each other mid-test.

## Versioning

Semver, tagged. Services pin their own version and upgrade when they choose — the module
makes a coordinated fix *possible*, never mandatory. A change to `grpcx`'s error mapping
or to `migrations/` is a **minor at minimum**, and a breaking wire change is a major.
