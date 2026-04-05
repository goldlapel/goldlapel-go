# Gold Lapel

Self-optimizing Postgres proxy — automatic materialized views and indexes, with an L1 native cache that serves repeated reads in microseconds. Zero code changes required.

Gold Lapel sits between your app and Postgres, watches query patterns, and automatically creates materialized views and indexes to make your database faster. Port 7932 (79 = atomic number for gold, 32 from Postgres).

## Install

```bash
go get github.com/goldlapel/goldlapel-go
```

## Quick Start

```go
package main

import (
	"log"

	goldlapel "github.com/goldlapel/goldlapel-go"
)

func main() {
	// Start the proxy — returns a database connection with L1 cache built in
	conn, err := goldlapel.Start("postgresql://user:pass@localhost:5432/mydb")
	if err != nil {
		log.Fatal(err)
	}
	defer goldlapel.Stop()

	// Use the connection directly — no driver setup needed
	rows, err := conn.Query("SELECT * FROM users WHERE id = $1", 42)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
}
```

## API

### `goldlapel.Start(upstream, opts...) (*Connection, error)`

Starts the Gold Lapel proxy (singleton) and returns a database connection with L1 cache.

- `upstream` — your Postgres connection string (e.g. `postgresql://user:pass@localhost:5432/mydb`)
- `opts` — functional options: `WithPort(port)`, `WithConfig(config)`, `WithExtraArgs(args...)`

### `goldlapel.Stop() error`

Stops the singleton proxy.

### `goldlapel.ProxyURL() string`

Returns the current proxy URL, or `""` if not running.

### `goldlapel.DashboardURL() string`

Returns the dashboard URL (e.g. `http://127.0.0.1:7933`), or `""` if not running. The dashboard port defaults to 7933 and can be changed via config:

```go
conn, err := goldlapel.Start("postgresql://user:pass@localhost:5432/mydb",
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
conn, err := proxy.Start()
// ...
proxy.Stop()
```

Instance methods: `Start()`, `Stop()`, `URL()`, `Port()`, `Running()`, `DashboardURL()`.

## Configuration

Pass a config map using `WithConfig`:

```go
import goldlapel "github.com/goldlapel/goldlapel-go"

conn, err := goldlapel.Start("postgresql://user:pass@localhost/mydb",
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
conn, err := goldlapel.Start(
	"postgresql://user:pass@localhost:5432/mydb",
	goldlapel.WithExtraArgs("--threshold-duration-ms", "200", "--refresh-interval-secs", "30"),
)
```

Or set environment variables (`GOLDLAPEL_PROXY_PORT`, `GOLDLAPEL_UPSTREAM`, etc.) — the binary reads them automatically.

## How It Works

This module bundles the Gold Lapel Rust binary for your platform. When you call `Start`, it:

1. Locates the binary (bundled in module, on PATH, or via `GOLDLAPEL_BINARY` env var)
2. Spawns it as a subprocess listening on localhost
3. Waits for the port to be ready
4. Returns a database connection with L1 native cache built in

Go convention: use `defer goldlapel.Stop()` for cleanup.

## Links

- [Website](https://goldlapel.com)
- [Documentation](https://github.com/goldlapel/goldlapel)
