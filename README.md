# Gold Lapel

Self-optimizing Postgres proxy — automatic materialized views and indexes. Zero code changes required.

Gold Lapel sits between your app and Postgres, watches query patterns, and automatically creates materialized views and indexes to make your database faster. Port 7932 (79 = atomic number for gold, 32 from Postgres).

## Install

```bash
go get github.com/goldlapel/goldlapel-go
```

## Quick Start

```go
package main

import (
	"database/sql"
	"log"

	goldlapel "github.com/goldlapel/goldlapel-go"
	_ "github.com/lib/pq"
)

func main() {
	// Start the proxy — returns a connection string pointing at Gold Lapel
	url, err := goldlapel.Start("postgresql://user:pass@localhost:5432/mydb")
	if err != nil {
		log.Fatal(err)
	}
	defer goldlapel.Stop()

	// Use the URL with any Postgres driver
	db, err := sql.Open("postgres", url)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}
```

Gold Lapel is driver-agnostic. `Start` returns a connection string (`postgresql://...@localhost:7932/...`) that works with any Postgres driver or ORM.

## API

### `goldlapel.Start(upstream, opts...) (string, error)`

Starts the Gold Lapel proxy (singleton) and returns the proxy connection string.

- `upstream` — your Postgres connection string (e.g. `postgresql://user:pass@localhost:5432/mydb`)
- `opts` — functional options: `WithPort(port)`, `WithConfig(config)`, `WithExtraArgs(args...)`

### `goldlapel.Stop() error`

Stops the singleton proxy.

### `goldlapel.ProxyURL() string`

Returns the current proxy URL, or `""` if not running.

### `goldlapel.DashboardURL() string`

Returns the dashboard URL (e.g. `http://127.0.0.1:7933`), or `""` if not running. The dashboard port defaults to 7933 and can be changed via config:

```go
url, err := goldlapel.Start("postgresql://user:pass@localhost:5432/mydb",
    goldlapel.WithConfig(map[string]interface{}{
        "dashboard_port": 8080,
    }),
)
fmt.Println(goldlapel.DashboardURL()) // http://127.0.0.1:8080
```

Set `dashboard_port` to `0` to disable.

### Instance API

For managing multiple proxy instances:

```go
proxy := goldlapel.New("postgresql://user:pass@localhost:5432/mydb",
	goldlapel.WithPort(9000),
	goldlapel.WithExtraArgs("--threshold-duration-ms", "200"),
)
url, err := proxy.Start()
// ...
proxy.Stop()
```

Instance methods: `Start()`, `Stop()`, `URL()`, `Port()`, `Running()`, `DashboardURL()`.

## Configuration

Pass a config map using `WithConfig`:

```go
import goldlapel "github.com/goldlapel/goldlapel-go"

url, err := goldlapel.Start("postgresql://user:pass@localhost/mydb",
    goldlapel.WithConfig(map[string]interface{}{
        "mode":              "butler",
        "pool_size":         50,
        "disable_matviews":  true,
        "replica":           []interface{}{"postgresql://user:pass@replica1/mydb"},
    }),
)
```

Keys use `snake_case` and map to CLI flags (`pool_size` → `--pool-size`). Boolean keys are flags — `true` enables them. Slice keys produce repeated flags.

Unknown keys return an error. To see all valid keys:

```go
goldlapel.ConfigKeys()
```

For the full configuration reference, see the [main documentation](https://github.com/goldlapel/goldlapel#setting-reference).

### Raw flags

You can also pass raw CLI flags via `WithExtraArgs`:

```go
url, err := goldlapel.Start(
	"postgresql://user:pass@localhost:5432/mydb",
	goldlapel.WithExtraArgs("--threshold-duration-ms", "200", "--refresh-interval-secs", "30"),
)
```

Or set environment variables (`GOLDLAPEL_PORT`, `GOLDLAPEL_UPSTREAM`, etc.) — the binary reads them automatically.

## Driver Examples

### database/sql + lib/pq

```go
import (
	goldlapel "github.com/goldlapel/goldlapel-go"
	_ "github.com/lib/pq"
)

url, _ := goldlapel.Start("postgresql://user:pass@localhost:5432/mydb")
defer goldlapel.Stop()
db, _ := sql.Open("postgres", url)
```

### pgx

```go
import (
	"context"
	goldlapel "github.com/goldlapel/goldlapel-go"
	"github.com/jackc/pgx/v5"
)

url, _ := goldlapel.Start("postgresql://user:pass@localhost:5432/mydb")
defer goldlapel.Stop()
conn, _ := pgx.Connect(context.Background(), url)
```

### GORM

```go
import (
	goldlapel "github.com/goldlapel/goldlapel-go"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

url, _ := goldlapel.Start("postgresql://user:pass@localhost:5432/mydb")
defer goldlapel.Stop()
db, _ := gorm.Open(postgres.Open(url), &gorm.Config{})
```

## How It Works

This module bundles the Gold Lapel Rust binary for your platform. When you call `Start`, it:

1. Locates the binary (bundled in module, on PATH, or via `GOLDLAPEL_BINARY` env var)
2. Spawns it as a subprocess listening on localhost
3. Waits for the port to be ready
4. Returns a connection string pointing at the proxy

Go convention: use `defer goldlapel.Stop()` for cleanup.

## Links

- [Website](https://goldlapel.com)
- [Documentation](https://github.com/goldlapel/goldlapel)
