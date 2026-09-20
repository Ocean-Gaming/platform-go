# platform-go

<!-- seeded by meta-repo scripts/gen-service-claude.py -->

Part of **Ocean**, 36 services, one repo each. Cross-service work belongs in a
session started from the meta-repo root, where every service is checked out and
`docs/architecture/contract-map.md` says who depends on what.

## Which file wins

When two documents disagree, the later item here is wrong, not the earlier:

1. `README.md` — orientation
2. this file — agent notes only

## The loop

```bash
go test ./...                       # unit; no docker
```

## Traps

- **This is a library, not a service.** It is the one module every service imports, so a change here ships to all 36. There is no `cmd/`, no database of its own, and the eight rules are *implemented* here rather than obeyed.
- **The schema ships with the code.** `migrations/0001_platform.sql` is embedded and exposed as `platform.Migrations()`. Do not move it out or let a service apply a hand-edited copy: the inbox bug was one defect split across a `.sql` file and a `.go` file, and a version boundary between them makes `go get -u` a way to silently break a service's conflict target.
- **`platformtest.RunConformance` is why this module exists in this shape.** One set of test bodies runs against every `idempotency.Store` and `inbox.Store` — fakes unconditionally, Postgres under the `integration` tag. A cross-tenant isolation bug passes the memory run and fails the Postgres one, so run both.
- **`grpcx` is a wire contract between services**, not a helper. If one service maps `ErrInFlight` to `ABORTED` and another to `FAILED_PRECONDITION`, no client can retry generically. Changing a metadata name or an error→status mapping is a platform-wide breaking change.
- **Migrations are append-only.** Never edit one that has merged; add a new numbered file.

## Platform rules

The eight rules are **implemented here**, not obeyed here. `tenant`,
`idempotency`, `outbox`, `inbox`, `gate`, `config`, `grpcx`, `obs`, `pg`: a
change to any of them is a change to all 36 services at once. Run the
conformance suite against both the memory and the Postgres harness before
believing a green test.

A change that breaks one of these is wrong even with green tests. Say so and stop.
