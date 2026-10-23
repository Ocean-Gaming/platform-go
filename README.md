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
| `stafftoken` | a staff command (attribution `staff:…`) reaches an owner only with operations-service's signed token for this service, tenant and staff member; fail closed when unconfigured (meta-repo ADR-0060) |
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

The module is **private**, like every other Ocean-Gaming repository (meta-repo ADR-0129;
the visibility flips at the end of platform-meta `docs/runbooks/platform-go-private.md`,
and until then the public repository is served exactly the same way). Module path, import
paths, tags and pinned versions are unchanged. What changes is who may fetch it:

- **CI** authenticates as the Ocean CI **GitHub App** (`Contents: read` on the
  organisation's repositories, nothing else). Each consuming repository holds the App id and
  private key as the repository secrets `OCEAN_APP_ID` and `OCEAN_APP_PRIVATE_KEY`; its
  `.github/actions/private-modules` action mints a one-hour installation token, sets
  `GOPRIVATE=github.com/Ocean-Gaming/*` and configures git's `insteadOf` rewrite in process
  scope, so no token lands in `go.mod`, in a file or in a layer. The former
  `ORG_MODULE_TOKEN` personal token is a fallback for one release and then goes.
- **Docker builds** mount a netrc as a BuildKit secret (`--secret id=netrc`); the stack's
  runbook, platform-meta `deploy/ocean/README.md`, says how to produce one from the App.
- **A laptop** sets `GOPRIVATE=github.com/Ocean-Gaming/*` and lets git authenticate as
  usual (SSH, a credential helper, or a `~/.netrc` holding a fine-grained token with
  `Contents: read`). `GOPRIVATE` also keeps `go` from asking `sum.golang.org` about these
  versions, which it could not answer.

Making the repository private hides nothing already released. Every version fetched so far,
**v0.3.5 up to v0.6.0, stays on `proxy.golang.org` and `sum.golang.org` for good**; only
versions tagged after the flip are protected. An anonymous `go mod download` of v0.6.0 with
the default `GOPROXY` therefore still succeeds, and the proof that the flip worked is the
one with `GOPROXY=direct`, which must fail.

It was public before by a deliberate trade: kept private, every consuming repository would
have needed a personal access token as a CI secret, because a repository's default
`GITHUB_TOKEN` cannot read a sibling private repository. `event-bus` going private made
that secret necessary anyway, and the App replaces the personal token in every repository.

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

### v0.5.0

Breaking for a caller that wired a store on the pool, which was already a bug:
`idempotency.NewPostgresStore`, `inbox.NewPostgresStore` and `outbox.NewPostgresWriter` take a
`*sql.Tx`. Every service checked out on 2026-10-19 already passes one and builds unchanged.
Behaviour that a service may notice on upgrade:

- `Complete` moves only `in_flight` to `completed`. The same response again succeeds; a different
  one is `ErrAlreadyCompleted` (INTERNAL, reason `idempotency_already_completed`).
- The outbox refuses a message whose tenant differs from the context's (`ErrTenantMismatch`),
  checks the whole batch before writing any of it, and no longer writes into the caller's
  `Headers` map. The memory outbox now stamps the `tenant_id` header too.
- The relay drains full batches back to back, bounds each tick with `TickTimeout` (default
  30 s), ticks once at start, and treats a mark that matched fewer rows than it published as a
  failure (`ErrNotMarked`).
- `grpcx.Status` maps `context.Canceled` and `context.DeadlineExceeded`, wrapped or not, to
  CANCELED and DEADLINE_EXCEEDED instead of INTERNAL.
- `gate.FailClosed` denies a request that has already ended without running the checker, and
  denies an Allow that arrives after cancellation.
- A staff token needs `iat`, and `exp - iat` may not exceed `MaxLifetime` (default one minute,
  operations-service's TTL).

### v0.5.1

Go 1.26 is the floor (`go 1.26.0` in go.mod; CI installs the newest 1.26.x). Go 1.25 is out of
support and gets no fix for the nine net/http, net/textproto and crypto/tls advisories published
on 2026-10-08 (GO-2026-6603 to GO-2026-6617), fixed in go1.26.9. `golang.org/x/net` is at v0.60.0
for the same advisories. A service taking this version has its own `go` directive raised to 1.26.0
by `go get`, and its CI's `go-version` must follow.

### v0.6.0

Two migrations, and the same rules in Go so the memory and Postgres stores refuse the same values.
Lengths are characters, not bytes, on both sides.

- `0003_bounds_and_checks.sql`:
  - idempotency keys are 1 to 255 characters (`ErrInvalidKey`, INVALID_ARGUMENT);
  - fingerprints are lowercase hex SHA-256 (`ErrInvalidFingerprint`, INTERNAL; use `Fingerprint`);
  - `completed_at` is set exactly when `state` is completed;
  - outbox `topic` is a registry event type, PascalCase (`ErrInvalidTopic`). It was never a Kafka
    topic;
  - `partition_key` is at most 255 characters (`ErrPartitionKeyTooLong`);
  - inbox `consumer` and `event_id` are 1 to 255 characters (`ErrInvalidConsumer`,
    `ErrInvalidEventID`). The event id's format is still never checked (meta-repo ADR-0061).

  Each check is added NOT VALID and then validated. A database whose old rows break one still
  boots: the check refuses new bad rows at once, logs a WARNING, and validates on the first boot
  after those rows are fixed.
- `0004_outbox_unpublished_index_order.sql`: the relay's index becomes `(occurred_at, id) WHERE
  published_at IS NULL`, matching its ORDER BY. It keeps the old name, so 0001's
  `IF NOT EXISTS` stays a no-op at every boot.

A service must apply every file in `platform.Migrations()`. One that copies `0001_platform.sql`
by name gets neither these nor 0002.

### v0.7.0

- `retention`: the sweeper ADR-0130 describes. Four policies, every one opt-in with no default
  period: `PublishedOutbox{After}`, `Inbox{After}` (at least 7 days), `IdempotencyResponse{ClearAfter}`
  (nulls the response, keeps the key) and `IdempotencyKey{DeleteAfter}` (never shorter than
  `ClearAfter`). Unpublished outbox rows and in-flight keys are never aged out; an in-flight key
  older than a day is reported as a crashed command. Bounded batches, one tenant at a time, a
  `BeforeBatch` hook for a service's RLS setting. A service that enables a policy adds the index
  its DELETE needs in its own migration; this module ships none.
- `tenant.ID.IsUUID`: the canonical lowercase UUID form ADR-0127 makes the tenant id. `Valid()`
  still accepts the slug for the cut-over.
- `0005_idempotency_state_enum.sql`: `idempotency_keys.state` is the ENUM `idempotency_state`
  (`in_flight`, `completed`). The Go stores are unchanged; 0003's `completed_at` check is
  re-created across the conversion.
