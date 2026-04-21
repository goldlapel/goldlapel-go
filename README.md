# goldlapel-go

[![Tests](https://github.com/goldlapel/goldlapel-go/actions/workflows/test.yml/badge.svg)](https://github.com/goldlapel/goldlapel-go/actions/workflows/test.yml)

The Go wrapper for [Gold Lapel](https://goldlapel.com) — a self-optimizing Postgres proxy that watches query patterns and creates materialized views + indexes automatically. Zero code changes beyond the connection string.

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

Point `database/sql` at `gl.URL()`. Gold Lapel sits between your app and your DB, watching query patterns and creating materialized views + indexes automatically. Zero code changes beyond the connection string.

Scoped transactions via `gl.InTx(ctx, db, fn)`, per-call `WithTx(tx)`, and the document-store / search / Redis-replacement wrapper methods are in the docs.

## Dashboard

Gold Lapel exposes a live dashboard at `gl.DashboardURL()`:

```go
fmt.Println(gl.DashboardURL())
// -> http://127.0.0.1:7933
```

## Documentation

Full API reference, configuration, transactions, document store, search, upgrading from v0.1 (including the BIGSERIAL-to-UUID doc-table migration), and production deployment: https://goldlapel.com/docs/go

## License

MIT. See `LICENSE`.
