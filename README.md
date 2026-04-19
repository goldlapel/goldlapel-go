# Gold Lapel

[![Tests](https://github.com/goldlapel/goldlapel-go/actions/workflows/test.yml/badge.svg)](https://github.com/goldlapel/goldlapel-go/actions/workflows/test.yml)

Self-optimizing Postgres proxy — automatic materialized views and indexes, with an L1 native cache that serves repeated reads in microseconds. Zero code changes required.

Gold Lapel sits between your app and Postgres, watches query patterns, and automatically creates materialized views and indexes to make your database faster. Port 7932 (79 = atomic number for gold, 32 from Postgres).

## Install

```bash
go get github.com/goldlapel/goldlapel-go
```

You also need a `database/sql` Postgres driver. `pgx` (via its stdlib adapter) is recommended; `lib/pq` also works.

```bash
go get github.com/jackc/pgx/v5
```

## Quick Start

```go
package main

import (
    "context"
    "database/sql"
    "log"

    _ "github.com/jackc/pgx/v5/stdlib" // register the "pgx" driver
    "github.com/goldlapel/goldlapel-go"
)

func main() {
    ctx := context.Background()

    // Start the proxy. Returns a ready *GoldLapel.
    gl, err := goldlapel.Start(ctx, "postgresql://user:pass@db/mydb",
        goldlapel.WithPort(7932),
        goldlapel.WithLogLevel("info"),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer gl.Stop(ctx)

    // Open a pool against the proxy with any registered driver.
    db, err := sql.Open("pgx", gl.URL())
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Normal SQL through the proxy — reads get the L1 cache automatically.
    rows, err := db.QueryContext(ctx, "SELECT id, name FROM users LIMIT 10")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    // Or use higher-level wrapper methods (document store, search, etc.).
    hits, err := gl.Search(ctx, "articles", "body", "postgres tuning")
    if err != nil {
        log.Fatal(err)
    }
    _ = hits
}
```

## API

### `goldlapel.Start(ctx, upstream, opts...) (*GoldLapel, error)`

Spawns the proxy binary, waits for it to accept connections, eagerly opens a `*sql.DB` pool against the proxy, and returns a ready instance. On failure, the subprocess is killed and stderr is surfaced in the error.

Construction options:

- `WithPort(port)` — proxy port (default `7932`)
- `WithLogLevel(level)` — proxy log level: `"trace"`, `"debug"`, `"info"`, `"warn"`, `"error"`. The binary defaults to `"warn"` (banner only); `"info"`, `"debug"`, and `"trace"` progressively add detail.
- `WithConfig(map)` — structured config map mapped to CLI flags (see below)
- `WithExtraArgs(args...)` — raw CLI arguments passed through to the binary

### `gl.Stop(ctx) error`

Sends SIGTERM to the proxy, closes the pool, and waits up to 5 seconds. Cancelling `ctx` forces an immediate kill. Safe to call multiple times. `gl.Close()` is available too — it calls `Stop(context.Background())` so the type satisfies `io.Closer`.

### `gl.URL() string`

Returns the proxy connection string (`postgresql://user:pass@localhost:7932/mydb`). Pass this to `sql.Open` with your driver of choice.

### `gl.DB() *sql.DB`

Returns the pool opened by `Start`. Nil if no driver was registered or the initial ping failed — in that case open your own pool via `sql.Open(..., gl.URL())`.

### `gl.InTx(ctx, db, fn)` — scoped transactions

```go
err := gl.InTx(ctx, db, func(gl *goldlapel.GoldLapel) error {
    if _, err := gl.DocInsert(ctx, "events", map[string]any{"type": "order.created"}); err != nil {
        return err
    }
    if _, err := gl.DocInsert(ctx, "events", map[string]any{"type": "email.sent"}); err != nil {
        return err
    }
    return nil
})
```

`InTx` begins a transaction on `db`, hands `fn` a scoped `*GoldLapel` whose wrapper methods automatically target that transaction, and commits on success or rolls back on error or panic. Pass `nil` for `db` to reuse the pool opened at `Start` (i.e. `gl.DB()`).

### `goldlapel.WithTx(tx)` — per-call transaction

Any wrapper method can take `WithTx(tx)` as the last argument to target a specific `*sql.Tx` for that one call:

```go
tx, _ := db.BeginTx(ctx, nil)
_, _ = gl.DocInsert(ctx, "events", map[string]any{"type": "x"}, goldlapel.WithTx(tx))
// ... more work on tx ...
tx.Commit()
```

### Dashboard & introspection

- `gl.Port() int`
- `gl.Running() bool`
- `gl.DashboardURL() string` — e.g. `http://127.0.0.1:7933`, or `""` if disabled

The dashboard port defaults to proxy-port + 1. Set `dashboard_port: 0` in `WithConfig` to disable.

## Configuration

Pass a config map via `WithConfig`:

```go
gl, _ := goldlapel.Start(ctx, "postgresql://user:pass@localhost/mydb",
    goldlapel.WithConfig(map[string]interface{}{
        "mode":              "waiter",
        "pool_size":         50,
        "disable_matviews":  true,
        "replica":           []interface{}{"postgresql://user:pass@replica1/mydb"},
    }),
)
```

Keys use `snake_case` and map to CLI flags (`pool_size` → `--pool-size`). Boolean keys emit a bare flag when true. Slice keys produce repeated flags. Unknown keys return an error.

```go
goldlapel.ConfigKeys() // sorted list of all valid keys
```

Or pass raw CLI flags directly:

```go
gl, _ := goldlapel.Start(ctx, upstream,
    goldlapel.WithExtraArgs("--threshold-duration-ms", "200", "--refresh-interval-secs", "30"),
)
```

## Document store

Gold Lapel ships a MongoDB-ish document store layered on Postgres JSONB. Every collection is a table with the schema

```sql
CREATE TABLE IF NOT EXISTS <collection> (
    _id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
)
```

Operations return `map[string]interface{}` with three top-level keys:

- `_id` — UUID string
- `data` — the document body as `map[string]interface{}`
- `created_at` — RFC 3339 timestamp string

```go
doc, _ := gl.DocInsert(ctx, "users", map[string]interface{}{"name": "alice"})
// doc == map[string]interface{}{
//     "_id": "7b2e...-uuid",
//     "data": map[string]interface{}{"name": "alice"},
//     "created_at": "2026-04-19T12:34:56.789012+00:00",
// }
```

The schema matches the other Gold Lapel wrappers (Python, JavaScript, Ruby, Java, PHP, .NET), so a collection created by one wrapper is readable and writable by any other against the same Postgres.

### Migration from v0.1

v0.1 tables used `id BIGSERIAL` primary keys. v0.2 switched to UUIDs for cross-wrapper interop. One-shot migration per existing doc table:

```sql
-- Migrate a goldlapel-go v0.1 doc table to the v0.2 schema.
-- `gen_random_uuid()` is in contrib; ensure `pgcrypto` is installed first:
--     CREATE EXTENSION IF NOT EXISTS pgcrypto;
ALTER TABLE <table> ADD COLUMN _id UUID DEFAULT gen_random_uuid();
UPDATE <table> SET _id = gen_random_uuid() WHERE _id IS NULL;
ALTER TABLE <table> ALTER COLUMN _id SET NOT NULL;
ALTER TABLE <table> DROP CONSTRAINT <table>_pkey;
ALTER TABLE <table> ADD PRIMARY KEY (_id);
ALTER TABLE <table> DROP COLUMN id;
```

## Wrapper methods

Every helper is available both as a package-level function (taking `ctx`, `execQuerier`, and any args) and as a receiver method on `*GoldLapel` (which resolves the target automatically based on `InTx` / `WithTx`):

- **Document store**: `DocCreateCollection`, `DocInsert`, `DocInsertMany`, `DocFind`, `DocFindOne`, `DocFindCursor`, `DocUpdate`, `DocUpdateOne`, `DocDelete`, `DocDeleteOne`, `DocFindOneAndUpdate`, `DocFindOneAndDelete`, `DocDistinct`, `DocCount`, `DocCreateIndex`, `DocAggregate`, `DocWatch`, `DocUnwatch`, `DocCreateTtlIndex`, `DocRemoveTtlIndex`, `DocCreateCapped`, `DocRemoveCap`
- **Full-text / fuzzy search**: `Search`, `SearchFuzzy`, `SearchPhonetic`, `Similar`, `Suggest`, `Facets`, `Aggregate`, `Analyze`, `ExplainScore`, `CreateSearchConfig`
- **Pub/Sub**: `Publish`, `Subscribe`, `SubscribeAsync`
- **Queues**: `Enqueue`, `Dequeue`
- **Counters / hashes / sorted sets**: `Incr`, `GetCounter`, `Hset`, `Hget`, `Hgetall`, `Hdel`, `Zadd`, `Zincrby`, `Zrange`, `Zrank`, `Zscore`, `Zrem`
- **Geo**: `Geoadd`, `Georadius`, `Geodist`
- **Streams**: `StreamAdd`, `StreamCreateGroup`, `StreamRead`, `StreamAck`, `StreamClaim`
- **Percolator**: `PercolateAdd`, `Percolate`, `PercolateDelete`
- **Misc**: `CountDistinct`, `Script`

## How it works

1. `Start` locates the bundled Rust binary (honouring `GOLDLAPEL_BINARY`), spawns it as a subprocess, and waits for its port to accept TCP connections.
2. It then eagerly opens a `database/sql` pool against the proxy — trying `pgx` first, then `postgres` (lib/pq) — and pings once. If the ping fails the pool is closed and `gl.DB()` returns `nil`; the proxy itself still runs, and callers can open their own pool via `gl.URL()`.
3. Wrapper methods run through `gl.DB()` by default. `InTx` begins a transaction and hands a scoped `*GoldLapel` to a closure; `WithTx(tx)` overrides the target for a single call.
4. `Stop(ctx)` sends `SIGTERM`, waits for the child to exit, and closes the pool.

## Testing

Run your test suite with the race detector enabled: `go test -race ./...`. The wrapper spawns a subprocess and holds shared state (the last-started instance used by `Wrap` auto-detection), and `-race` catches concurrency issues that silent PASSes would otherwise hide.

## Upgrading from v0.1

Breaking changes at v0.2.0. No deprecation aliases.

| v0.1 | v0.2 |
| --- | --- |
| `gl := goldlapel.New(url); gl.Start()` | `gl, err := goldlapel.Start(ctx, url, opts...)` |
| `gl.Stop()` | `gl.Stop(ctx)` (or `gl.Close()`) |
| `gl.DocInsert("users", doc)` | `gl.DocInsert(ctx, "users", doc)` |
| `DocInsert(db, "users", doc)` | `DocInsert(ctx, db, "users", doc)` |
| `goldlapel.Start(url)` (singleton) | `goldlapel.Start(ctx, url)` (instance) |
| none | `gl.InTx(ctx, db, fn)`, `goldlapel.WithTx(tx)` |

All wrapper methods now take `context.Context` as the first argument. Package-level helpers take an `execQuerier` (either `*sql.DB` or `*sql.Tx`) as the second argument.

## Links

- [Website](https://goldlapel.com)
- [Main project](https://github.com/goldlapel/goldlapel)
