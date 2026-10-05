# goldlapel-go

[![Tests](https://github.com/goldlapel/goldlapel-go/actions/workflows/test.yml/badge.svg)](https://github.com/goldlapel/goldlapel-go/actions/workflows/test.yml)

The Go wrapper for [Gold Lapel](https://goldlapel.com) — a self-optimizing Postgres proxy that caches query results, creates indexes from your query patterns, and keeps the cache correct as your data changes. Zero code changes beyond the connection string.

The wrapper runs the proxy as a managed subprocess: it finds the bundled binary, starts it with your app and stops it on `gl.Stop`, translates options into proxy flags, generates the dashboard token, and hands back a driver-ready URL. It also provides Postgres-backed helpers — search and percolator, a document store, streams, counters, sorted sets, hashes, queues, geo, and pub/sub. Caching happens in the proxy, which serves every client the same way; the `*sql.DB` you open (or `gl.DB()`) is a plain `database/sql` pool from your driver — pgx or lib/pq.

## Install

```bash
go get github.com/goldlapel/goldlapel-go

# Plus a database/sql driver — pgx (stdlib adapter) is recommended:
go get github.com/jackc/pgx/v5
```

## Quickstart

```go
package main

import (
    "context"
    "database/sql"
    "log"

    _ "github.com/jackc/pgx/v5/stdlib"
    "github.com/goldlapel/goldlapel-go"
)

func main() {
    ctx := context.Background()

    // Spawn the proxy in front of your upstream DB
    gl, err := goldlapel.Start(ctx, "postgresql://user:pass@localhost:5432/mydb")
    if err != nil {
        log.Fatal(err)
    }
    defer gl.Stop(ctx)

    // Point any database/sql driver at gl.URL()
    db, err := sql.Open("pgx", gl.URL())
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    rows, err := db.QueryContext(ctx, "SELECT id, name FROM users LIMIT 10")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
}
```

Point `database/sql` at `gl.URL()`. Gold Lapel sits between your app and your DB, caching results and creating indexes from your query patterns. Connections are tagged `application_name=goldlapel:go:<version>` so they're recognisable in `pg_stat_activity`.

The proxy listens on two ports: the proxy itself (`WithProxyPort`) and the dashboard (`WithDashboardPort`, default proxy port + 1; `0` disables it). Without `WithProxyPort`, Start picks the first free pair from 7932 up, so several databases in one process each get their own proxy. Starting the same upstream twice shares one proxy; it stops when the last handle is stopped. An explicit port another of your proxies already holds is an error, and a port some other process holds makes the proxy refuse to start, with its message in the error.

`gl.URL()` is plaintext to the local proxy: the upstream's TLS parameters (`sslmode`, `sslrootcert`, `channel_binding`, …) stay on the proxy's upstream hop and the app's URL carries `sslmode=disable` instead — unless you give the proxy its own certificate (`tls_cert` / `tls_key`), in which case the parameters are kept.

Scoped transactions via `gl.InTx(ctx, db, fn)`, per-call `WithTx(tx)`, and the document-store / search / Redis-replacement wrapper methods are in the docs.

## Document store and streams

Document-store and stream verbs are reached through nested namespaces — `gl.Documents.<Verb>(...)` and `gl.Streams.<Verb>(...)`. The proxy owns the canonical helper-table DDL (Phase 4 of schema-to-core), so the wrapper never hand-writes `CREATE TABLE _goldlapel.doc_*` SQL. The first call for a given collection or stream POSTs `/api/ddl/doc_store/create` (or `/api/ddl/stream/create`); the resulting table name and query patterns are cached for the session.

```go
gl.Documents.Insert(ctx, "users", map[string]interface{}{"name": "alice"})
gl.Documents.Find(ctx, "users", map[string]interface{}{"active": true})
gl.Documents.CreateCollection(ctx, "sessions", goldlapel.DocUnlogged(true))

gl.Streams.Add(ctx, "events", `{"type":"click"}`)
gl.Streams.CreateGroup(ctx, "events", "workers")
gl.Streams.Read(ctx, "events", "workers", "consumer-1", 10)
```

Other namespaces (search, queues, counters, hashes, zsets, geo, …) stay flat for now and migrate to nested form one at a time as their own schema-to-core phase fires.

> **Breaking change in v0.3** — `gl.Doc<Verb>` and `gl.Stream<Verb>` flat methods were removed. Search and replace `gl.DocInsert(...)` → `gl.Documents.Insert(...)`, `gl.StreamAdd(...)` → `gl.Streams.Add(...)`, etc.

## Dashboard

Gold Lapel exposes a live dashboard at `gl.DashboardURL()`:

```go
fmt.Println(gl.DashboardURL())
// -> http://127.0.0.1:7933
```

## Documentation

Full API reference, configuration, transactions, document store, search, upgrading from v0.1 (including the BIGSERIAL-to-UUID doc-table migration), and production deployment: https://goldlapel.com/docs/go

## Uninstalling

Before removing the package, drop Gold Lapel's helper schema and indexes from your Postgres:

```bash
goldlapel clean
```

Then remove the import from your code, tidy the module, and clear any local state:

```bash
go mod tidy
rm -rf ~/.goldlapel
rm -f goldlapel.toml     # only if you wrote one
```

Cancelling your subscription does not delete your data — only Gold Lapel's helper schema and indexes go away.

## License

MIT. See `LICENSE`.
