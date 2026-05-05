package goldlapel

import (
	"context"
	"errors"
	"reflect"
	"sync"
)

// ErrNoRows is returned by Row.Scan when the query returns no rows.
// This mirrors pgx.ErrNoRows for code that checks errors.Is(err, ErrNoRows).
var ErrNoRows = errors.New("no rows in result set")

// Querier is the interface that CachedConn wraps. It matches the subset of
// pgx.Conn and pgxpool.Pool used for queries. Any type with Query, QueryRow,
// and Exec methods can be wrapped.
type Querier interface {
	Query(ctx context.Context, sql string, args ...interface{}) (Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) Row
	Exec(ctx context.Context, sql string, args ...interface{}) (interface{}, error)
}

// Rows is the interface for iterating query results. Matches pgx.Rows.
type Rows interface {
	Next() bool
	Scan(dest ...interface{}) error
	Close()
	Err() error
	FieldDescriptions() []FieldDescription
	Values() ([]interface{}, error)
	RawValues() [][]byte
}

// Row is the interface for a single-row query. Matches pgx.Row.
type Row interface {
	Scan(dest ...interface{}) error
}

// FieldDescription describes a column in query results.
type FieldDescription struct {
	Name string
}

// CachedConn wraps a Querier (e.g. pgx.Conn) with the wrapper's native cache.
// Reads are served from cache when possible. Writes trigger invalidation.
// Transactions bypass the cache entirely.
//
// gucState is the per-connection unsafe-GUC fingerprint (Option Y) — folded
// into every cache key so two CachedConns with different SET app.user_id
// values can never share a cache slot. Mirrors the proxy's per-connection
// guc_state. See guc_state.go for the design rationale.
type CachedConn struct {
	real          Querier
	cache         *NativeCache
	inTransaction bool
	gucState      *ConnectionGucState
}

// Wrap wraps a Querier with the wrapper's native cache and starts the
// invalidation listener if not already running. Each Wrap() call returns
// a fresh CachedConn with its own per-connection GUC state — caller is
// expected to bind one CachedConn per logical Postgres connection (the
// same way pgx.Conn is goroutine-confined).
func Wrap(conn Querier, invalidationPort int) *CachedConn {
	cache := GetNativeCache()
	if invalidationPort <= 0 {
		invalidationPort = detectInvalidationPort()
	}
	if !cache.Connected() {
		cache.ConnectInvalidation(invalidationPort)
	}
	return &CachedConn{
		real:     conn,
		cache:    cache,
		gucState: NewConnectionGucState(),
	}
}

// GucStateHash returns the current unsafe-GUC fingerprint for this
// connection. Exposed primarily for tests; production callers don't need
// to introspect it (Get/Put thread it through internally).
func (cc *CachedConn) GucStateHash() uint64 {
	return cc.ensureGucState().Hash()
}

// ensureGucState lazily initialises the per-connection GUC state for
// CachedConns built directly via struct literal (e.g. test fixtures that
// pre-date the field). Wrap() always initialises gucState eagerly; this
// method is the safety net for hand-rolled construction paths.
func (cc *CachedConn) ensureGucState() *ConnectionGucState {
	if cc.gucState == nil {
		cc.gucState = NewConnectionGucState()
	}
	return cc.gucState
}

// lastStartedInstance tracks the most recently Start()ed *GoldLapel so that
// Wrap() can auto-detect the invalidation port without requiring the caller
// to pass it explicitly. It is best-effort — if no instance has been started
// (e.g. the user is running the proxy out-of-process) Wrap falls back to
// DefaultPort+2.
var (
	lastStartedInstance   *GoldLapel
	lastStartedInstanceMu sync.Mutex
)

func registerStartedInstance(gl *GoldLapel) {
	lastStartedInstanceMu.Lock()
	lastStartedInstance = gl
	lastStartedInstanceMu.Unlock()
}

func detectInvalidationPort() int {
	lastStartedInstanceMu.Lock()
	inst := lastStartedInstance
	lastStartedInstanceMu.Unlock()
	if inst != nil {
		// invalidationPort is resolved at Start time: either the explicit
		// WithInvalidationPort value or proxyPort + 2.
		return inst.invalidationPort
	}
	return DefaultProxyPort + 2
}

// Unwrap returns the underlying Querier.
func (cc *CachedConn) Unwrap() Querier {
	return cc.real
}

// Query intercepts SQL queries. Writes trigger invalidation and are forwarded.
// Reads check the L1 cache first, falling back to the real connection on miss.
func (cc *CachedConn) Query(ctx context.Context, sql string, args ...interface{}) (Rows, error) {
	// Per-connection unsafe-GUC tracking. Every wire-bound SQL string is
	// observed so a SET app.user_id='42' that arrives via Query (some
	// drivers route bare SET through Query) updates the connection's
	// state hash before the cache is consulted. ObserveSQL is no-op for
	// non-SET statements and a fast single-statement path for the common
	// case, so the overhead per call is a couple of string-prefix checks.
	cc.ensureGucState().ObserveSQL(sql)

	// Multi-statement-aware write detection runs before any short-circuit
	// — a body like `BEGIN; INSERT INTO orders VALUES (1); COMMIT` must
	// still invalidate orders. detectWriteMulti walks every top-level
	// segment.
	writeTables, ddlHit := detectWriteMulti(sql)
	if ddlHit {
		cc.cache.InvalidateAll()
	} else {
		for _, t := range writeTables {
			cc.cache.InvalidateTable(t)
		}
	}

	// Transaction tracking — multi-statement-aware. applyTxState walks
	// every top-level segment so a body like
	// `BEGIN; INSERT INTO orders VALUES (1); COMMIT` ends with
	// inTransaction=false to mirror the server, instead of the legacy
	// first-token-only check that flipped it true based on BEGIN and
	// left the cache bypassed forever until the next BEGIN/COMMIT cycle.
	if containsTxControl(sql) {
		cc.inTransaction = applyTxState(sql, cc.inTransaction)
		return cc.real.Query(ctx, sql, args...)
	}

	// If write detection consumed the body, no further cache work is
	// possible — the response is a write tag, not a row stream.
	if ddlHit || len(writeTables) > 0 {
		return cc.real.Query(ctx, sql, args...)
	}

	// Inside transaction: bypass cache
	if cc.inTransaction {
		return cc.real.Query(ctx, sql, args...)
	}

	stateHash := cc.ensureGucState().Hash()

	// Read path: check L1 cache
	entry := cc.cache.Get(sql, args, stateHash)
	if entry != nil {
		rows, ok := entry.Rows.([][]interface{})
		if ok {
			var fields []FieldDescription
			if entry.Fields != nil {
				if f, ok2 := entry.Fields.([]FieldDescription); ok2 {
					fields = f
				}
			}
			return &cachedRows{rows: rows, fields: fields, pos: -1}, nil
		}
	}

	// Cache miss: execute for real
	rows, err := cc.real.Query(ctx, sql, args...)
	if err != nil {
		return rows, err
	}

	// Collect rows to cache
	var collected [][]interface{}
	var fields []FieldDescription
	if rows != nil {
		fields = rows.FieldDescriptions()
		for rows.Next() {
			vals, vErr := rows.Values()
			if vErr != nil {
				// Can't cache this result, return the rows as-is
				// (but rows is consumed at this point, so wrap collected + remaining)
				break
			}
			dup := make([]interface{}, len(vals))
			copy(dup, vals)
			collected = append(collected, dup)
		}
		rows.Close()
		if rows.Err() != nil {
			return &cachedRows{rows: collected, fields: fields, pos: -1, err: rows.Err()}, nil
		}
	}

	// Cache the result under the current per-connection state hash —
	// unless this was a session-state command (SET / RESET / LISTEN /
	// NOTIFY / etc.) routed through Query. Those return zero rows but
	// the truthy-tail Put would still bloat the LRU with empty entries
	// that can never serve a real read. See
	// docs/todos/wrapper-cache-set-responses.md.
	if !isSessionStateCommand(sql) {
		cc.cache.Put(sql, args, stateHash, collected, fields)
	}

	return &cachedRows{rows: collected, fields: fields, pos: -1}, nil
}

// QueryRow intercepts single-row queries.
func (cc *CachedConn) QueryRow(ctx context.Context, sql string, args ...interface{}) Row {
	cc.ensureGucState().ObserveSQL(sql)

	// Multi-statement-aware write detection runs ahead of any short-circuit
	// — see Query() for the full rationale.
	writeTables, ddlHit := detectWriteMulti(sql)
	if ddlHit {
		cc.cache.InvalidateAll()
	} else {
		for _, t := range writeTables {
			cc.cache.InvalidateTable(t)
		}
	}

	// Transaction tracking — multi-statement-aware. See Query() for the
	// full rationale.
	if containsTxControl(sql) {
		cc.inTransaction = applyTxState(sql, cc.inTransaction)
		return cc.real.QueryRow(ctx, sql, args...)
	}

	// If write detection fired, dispatch to the real backend — there is
	// no row stream to cache.
	if ddlHit || len(writeTables) > 0 {
		return cc.real.QueryRow(ctx, sql, args...)
	}

	// Inside transaction: bypass cache
	if cc.inTransaction {
		return cc.real.QueryRow(ctx, sql, args...)
	}

	stateHash := cc.ensureGucState().Hash()

	// Read path: check L1 cache
	entry := cc.cache.Get(sql, args, stateHash)
	if entry != nil {
		rows, ok := entry.Rows.([][]interface{})
		if ok && len(rows) > 0 {
			return &cachedRow{values: rows[0]}
		}
		return &cachedRow{values: nil}
	}

	// Cache miss: use Query to fetch, cache, and return first row.
	// Query() will re-observe and re-hash; that's idempotent for the
	// already-applied SET above (Apply on the same (name, value) pair
	// leaves the hash unchanged) so we don't double-count.
	rows, err := cc.Query(ctx, sql, args...)
	if err != nil {
		return &cachedRow{err: err}
	}
	if rows.Next() {
		vals, vErr := rows.Values()
		rows.Close()
		if vErr != nil {
			return &cachedRow{err: vErr}
		}
		return &cachedRow{values: vals}
	}
	rows.Close()
	return &cachedRow{values: nil}
}

// Exec intercepts write commands. It always forwards to the real connection
// and invalidates the cache for any detected writes. Bare SET / RESET
// statements typically arrive via Exec (no result rows), so this is the
// most important hook point for unsafe-GUC tracking — without it, a client
// that does `conn.Exec("SET app.user_id = '42'")` followed by
// `conn.Query("SELECT ...")` would never have moved the state hash before
// the cache lookup.
func (cc *CachedConn) Exec(ctx context.Context, sql string, args ...interface{}) (interface{}, error) {
	cc.ensureGucState().ObserveSQL(sql)

	// Multi-statement-aware write detection — runs ahead of tx tracking
	// so a `BEGIN; INSERT INTO t ...; COMMIT` body still invalidates t.
	writeTables, ddlHit := detectWriteMulti(sql)
	if ddlHit {
		cc.cache.InvalidateAll()
	} else {
		for _, t := range writeTables {
			cc.cache.InvalidateTable(t)
		}
	}

	// Transaction tracking — multi-statement-aware. See Query() for the
	// full rationale.
	cc.inTransaction = applyTxState(sql, cc.inTransaction)

	return cc.real.Exec(ctx, sql, args...)
}

// --- Cached result types ---

type cachedRows struct {
	rows   [][]interface{}
	fields []FieldDescription
	pos    int
	err    error
}

func (cr *cachedRows) Next() bool {
	cr.pos++
	return cr.pos < len(cr.rows)
}

func (cr *cachedRows) Scan(dest ...interface{}) error {
	if cr.pos < 0 || cr.pos >= len(cr.rows) {
		return nil
	}
	row := cr.rows[cr.pos]
	return scanRow(row, dest)
}

func (cr *cachedRows) Close() {}

func (cr *cachedRows) Err() error {
	return cr.err
}

func (cr *cachedRows) FieldDescriptions() []FieldDescription {
	return cr.fields
}

func (cr *cachedRows) Values() ([]interface{}, error) {
	if cr.pos < 0 || cr.pos >= len(cr.rows) {
		return nil, nil
	}
	return cr.rows[cr.pos], nil
}

func (cr *cachedRows) RawValues() [][]byte {
	return nil
}

type cachedRow struct {
	values []interface{}
	err    error
}

func (cr *cachedRow) Scan(dest ...interface{}) error {
	if cr.err != nil {
		return cr.err
	}
	if cr.values == nil {
		return ErrNoRows
	}
	return scanRow(cr.values, dest)
}

func scanRow(src []interface{}, dest []interface{}) error {
	for i := 0; i < len(dest) && i < len(src); i++ {
		if p, ok := dest[i].(*interface{}); ok {
			*p = src[i]
			continue
		}
		// Use reflect for typed pointers (*string, *int, etc.)
		dv := reflect.ValueOf(dest[i])
		if dv.Kind() != reflect.Ptr || dv.IsNil() {
			continue
		}
		sv := reflect.ValueOf(src[i])
		if !sv.IsValid() {
			dv.Elem().Set(reflect.Zero(dv.Elem().Type()))
			continue
		}
		if sv.Type().AssignableTo(dv.Elem().Type()) {
			dv.Elem().Set(sv)
		} else if sv.Type().ConvertibleTo(dv.Elem().Type()) {
			dv.Elem().Set(sv.Convert(dv.Elem().Type()))
		}
	}
	return nil
}
