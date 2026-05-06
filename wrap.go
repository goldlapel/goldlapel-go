package goldlapel

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
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
//
// Verify lifecycle:
//
//   - Every Query/QueryRow/Exec runs maybeVerifyOnCheckout before touching
//     the cache. If gucState.IsDirty() returns true (a prior call observed
//     a top-level SELECT <ident>(...) that may have done a server-side
//     SET) the wrapper re-reads pg_settings on the same connection and
//     Reseeds the unsafe-GUC value map. This is the safety net for
//     stored-function SETs the wire layer can't see.
//
//   - After observing a top-level SELECT <ident>(...) the wrapper marks
//     dirty and schedules an async verify in a background goroutine
//     (verifyWG / verifyCtx). The async path keeps the user's hot path
//     unblocked; the next checkout still re-runs verify if the async
//     path lost the race or hadn't completed yet.
//
//   - Close() cancels the in-flight verify goroutines via verifyCancel
//     and waits on verifyWG. Users that rely on pgx.Conn's own Close()
//     should call cc.Close() first to avoid leaving verify goroutines
//     dangling against a torn-down connection.
type CachedConn struct {
	real          Querier
	cache         *NativeCache
	inTransaction bool
	gucState      *ConnectionGucState

	// gucStateOnce gates lazy init of gucState for struct-literal
	// CachedConn instances (Wrap() initialises eagerly; this is the
	// safety net for test fixtures and hand-rolled construction).
	gucStateOnce sync.Once

	// Verify lifecycle. verifyCtx is the cancellation signal for in-flight
	// async verify goroutines; verifyCancel terminates them at Close(); and
	// verifyWG tracks them so Close() can wait for orderly exit.
	// verifyOnce gates lazy init of these fields — Wrap() initialises
	// eagerly, but struct-literal CachedConn instances (test fixtures) get
	// initialisation on first use.
	verifyOnce   sync.Once
	verifyCtx    context.Context
	verifyCancel context.CancelFunc
	verifyWG     sync.WaitGroup

	// inFlightVerify gates the async post-call path so a burst of
	// SELECT <fn>() calls only schedules ONE verify goroutine at a time.
	// Acts as a small concurrency limiter — if a verify is already in
	// flight, subsequent triggers no-op (the verify will pick up any
	// state mutations they would have caused). atomic.Bool because the
	// hot path needs to compare-and-set without taking a mutex.
	inFlightVerify atomic.Bool

	// closed is set by Close() to suppress further async verify
	// scheduling. Atomic for the same reason as inFlightVerify.
	closed atomic.Bool

	// poolDiscarder is an optional bridge to the pool-level
	// DISCARD-on-release tracker. When non-nil, every observed unsafe
	// SET / RESET / DISCARD updates the discarder's dirty flag so a
	// pgxpool.Config.AfterRelease hook calling OnAfterRelease can
	// decide whether to issue DISCARD ALL on connection return.
	// See pool.go for the discarder's full lifecycle.
	poolDiscarder *PoolReleaseDiscarder
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
//
// Concurrency-safe via gucStateOnce — concurrent first-use across
// multiple goroutines is well-defined (one initialiser runs, all see
// the same pointer).
func (cc *CachedConn) ensureGucState() *ConnectionGucState {
	cc.gucStateOnce.Do(func() {
		if cc.gucState == nil {
			cc.gucState = NewConnectionGucState()
		}
	})
	return cc.gucState
}

// ensureVerifyCtx lazily initialises the verify lifecycle. Idempotent
// via verifyOnce; safe to call from any goroutine before scheduling an
// async verify.
func (cc *CachedConn) ensureVerifyCtx() {
	cc.verifyOnce.Do(func() {
		cc.verifyCtx, cc.verifyCancel = context.WithCancel(context.Background())
	})
}

// Close cancels any in-flight verify goroutines and waits for them to
// exit. Safe to call multiple times; subsequent calls no-op. After
// Close, async verify scheduling is suppressed (a follow-up
// Query/QueryRow/Exec still works against the underlying Querier — only
// the verify-on-checkout path is suppressed, since dispatching
// verify against a connection the caller is about to discard would
// race with the caller's own teardown).
//
// Close does NOT close the underlying Querier — that's the caller's
// responsibility. Close exists so that callers running pgx.Conn or
// pgxpool can drop the wrapper and its verify goroutines cleanly.
//
// Idiomatic usage:
//
//	cc := goldlapel.Wrap(conn, port)
//	defer cc.Close()
//	... // queries go here
func (cc *CachedConn) Close() {
	if !cc.closed.CompareAndSwap(false, true) {
		return
	}
	cc.ensureVerifyCtx()
	if cc.verifyCancel != nil {
		cc.verifyCancel()
	}
	cc.verifyWG.Wait()
}

// maybeVerifyOnCheckout runs a synchronous verify if the connection's
// GUC state is dirty. Called from Query/QueryRow/Exec immediately
// before the cache lookup — the verify rebuilds state from
// pg_settings so the cache key reflects the server's actual session
// state, not stale wire-side observations.
//
// Verify failure (the underlying connection errors out) is logged
// implicitly via the error path of cc.real.Query; we treat the state
// as untrustworthy and leave the dirty flag set. The caller's query
// then proceeds against the same Querier — if the connection is
// genuinely broken, the user's call will surface the same error. We
// never block the user on a verify error.
//
// Inside a transaction the cache is bypassed regardless of state, so
// verify is skipped — the dirty flag will be re-evaluated on the next
// post-commit checkout.
func (cc *CachedConn) maybeVerifyOnCheckout(ctx context.Context) {
	if cc.inTransaction {
		return
	}
	state := cc.ensureGucState()
	if !state.IsDirty() {
		return
	}
	cc.runVerify(ctx)
}

// runVerify reads `pg_settings WHERE source = 'session'` directly from
// the underlying Querier and Reseeds the GUC state map. Synchronous —
// callers that need the async variant call scheduleAsyncVerify
// instead. Returns nothing because errors are intentionally swallowed:
// a transient verify failure leaves the dirty flag set so the next
// checkout retries.
//
// The verify SQL filters to source='session' so we only see GUCs the
// user actually changed (default values stay out of the map).
// Reseed's filter then drops anything that isn't an unsafe GUC, so
// the final state map matches the canonical SET-observed shape.
func (cc *CachedConn) runVerify(ctx context.Context) {
	const verifySQL = "SELECT name, setting FROM pg_settings WHERE source = 'session'"
	rows, err := cc.real.Query(ctx, verifySQL)
	if err != nil {
		return
	}
	defer rows.Close()
	values := make(map[string]string)
	for rows.Next() {
		vals, vErr := rows.Values()
		if vErr != nil {
			return
		}
		if len(vals) < 2 {
			continue
		}
		name, _ := vals[0].(string)
		setting, _ := vals[1].(string)
		if name == "" {
			continue
		}
		values[name] = setting
	}
	if rows.Err() != nil {
		return
	}
	cc.ensureGucState().Reseed(values)
}

// observeForPool propagates SQL-stream observations into the pool
// discarder (if attached). Called alongside ObserveSQL on every wire-
// bound SQL string.
//
//   - Any unsafe SET / RESET / DISCARD-other / set_config(false) →
//     pool dirty (the connection now carries session state another
//     borrower would inherit on pool reuse).
//   - DISCARD ALL / RESET ALL → pool clean (PG has cleared session
//     state authoritatively).
//   - SET LOCAL / set_config(..., true) → pool unaffected (LOCAL
//     scope is automatically dropped at COMMIT/ROLLBACK).
//
// Multi-statement Q messages are handled by inspecting each
// segment; the first state-affecting segment wins for the dirty
// direction, but RESET ALL / DISCARD ALL anywhere in the body
// drops dirty (PG's actual behaviour: DISCARD inside a script
// resets state at that point).
func (cc *CachedConn) observeForPool(sql string) {
	if cc.poolDiscarder == nil {
		return
	}
	process := func(stmt string) {
		cmd := ParseSetCommand(stmt)
		switch cmd.Kind {
		case SetCmdSet:
			if IsUnsafeGUC(cmd.Name) {
				cc.poolDiscarder.MarkDirty()
			}
		case SetCmdReset:
			if IsUnsafeGUC(cmd.Name) {
				cc.poolDiscarder.MarkDirty()
			}
		case SetCmdResetAll:
			cc.poolDiscarder.MarkClean()
		case SetCmdDiscardOther:
			// Doesn't touch GUC state; doesn't change pool-clean
			// status either. (DISCARD PLANS still leaves the
			// connection's GUCs in place.)
		}
	}
	stripped := sql
	if !containsTopLevelSemicolon(stripped) {
		process(stripped)
		return
	}
	for _, seg := range SplitStatements(stripped) {
		process(seg)
	}
}

// containsTopLevelSemicolon reports whether `sql` contains a `;`
// character outside of a string literal — i.e. whether it's a
// multi-statement body. Cheap to compute; mirrors the fast-path
// check inside ObserveSQL so observeForPool stays consistent.
func containsTopLevelSemicolon(sql string) bool {
	var quote byte
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		if quote != 0 {
			if c == quote {
				if i+1 < len(sql) && sql[i+1] == quote {
					i++
					continue
				}
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"':
			quote = c
		case ';':
			// Trailing-only `;` is benign; check we have content
			// after it (otherwise it's just a terminator).
			rest := sql[i+1:]
			for _, r := range rest {
				if r != ' ' && r != '\t' && r != '\r' && r != '\n' && r != ';' {
					return true
				}
			}
			return false
		}
	}
	return false
}

// shouldScheduleVerifyAfter reports whether the wrapper should mark
// the connection dirty + schedule an async verify after this SQL
// completes. True for top-level `SELECT <ident>(...)` shapes that
// AREN'T already covered by inline parsing (set_config). The wrapper's
// ObserveSQL has already extracted state from set_config calls, so
// they don't need a follow-up verify; arbitrary other functions are
// the post-call-verify case the spec calls out.
//
// The verify SQL itself (`SELECT name, setting FROM pg_settings ...`)
// is a column-projection SELECT, not a bare function call, so
// IsTopLevelFunctionCall returns false — no recursion concern.
func (cc *CachedConn) shouldScheduleVerifyAfter(sql string) bool {
	if !IsTopLevelFunctionCall(sql) {
		return false
	}
	// A top-level set_config(...) is a recognised SET-equivalent — the
	// wrapper's ObserveSQL has already extracted name/value from it
	// and updated the state map. Scheduling a verify for it would be
	// redundant work + would set the dirty flag the verify would then
	// have to clear.
	if cmd := ParseSetCommand(sql); cmd.Kind != SetCmdNone {
		return false
	}
	return true
}

// scheduleAsyncVerify spawns a background goroutine that calls
// runVerify. Used after observing a top-level SELECT <ident>(...) so
// any server-side SET inside the function body is reflected in our
// state map without blocking the user's hot path.
//
// Concurrency-limited: if a verify is already in flight on this
// connection, the new trigger no-ops. The in-flight verify will pick
// up any state mutations the new trigger would have refreshed.
//
// Goroutine-leak protection:
//   - Close() cancels verifyCtx, terminating any in-flight verify when
//     the underlying Query returns or its context is observed.
//   - Close() waits on verifyWG, so callers that defer cc.Close()
//     don't race with verify completion.
//   - Once Close has been called, scheduleAsyncVerify is a no-op
//     (closed atomic check) — no new goroutines are dispatched.
//
// The user's calling-context is intentionally NOT propagated into
// the verify goroutine — the user's ctx is bound to their statement
// lifetime, but verify must run AFTER the user's call completes (the
// statement may have moved server state). We use the long-lived
// verifyCtx (cancelled only by Close) so verify can outlive the
// triggering call.
func (cc *CachedConn) scheduleAsyncVerify() {
	if cc.closed.Load() {
		return
	}
	if !cc.inFlightVerify.CompareAndSwap(false, true) {
		return
	}
	cc.ensureVerifyCtx()
	cc.verifyWG.Add(1)
	go func() {
		defer cc.verifyWG.Done()
		defer cc.inFlightVerify.Store(false)
		// Run verify against the long-lived verifyCtx; if Close()
		// has cancelled it, runVerify's underlying Query will
		// observe ctx.Err() and return without mutating state. We
		// don't short-circuit on closed.Load() here — once we've
		// passed the schedule-time closed check + incremented WG,
		// completing the verify (or letting it observe its own ctx
		// cancellation) is the simpler invariant.
		cc.runVerify(cc.verifyCtx)
	}()
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
	// Verify-on-checkout — if a prior call observed a SQL shape that
	// could have moved server state without us seeing the SET (e.g.
	// `SELECT my_func()` whose body did `SET app.user_id`), the dirty
	// flag is set and we re-read pg_settings before the cache lookup.
	// No-op outside a transaction with a clean state. Inside a tx the
	// cache is bypassed regardless, so verify is also skipped.
	cc.maybeVerifyOnCheckout(ctx)

	// Per-connection unsafe-GUC tracking. Every wire-bound SQL string is
	// observed so a SET app.user_id='42' that arrives via Query (some
	// drivers route bare SET through Query) updates the connection's
	// state hash before the cache is consulted. ObserveSQL is no-op for
	// non-SET statements and a fast single-statement path for the common
	// case, so the overhead per call is a couple of string-prefix checks.
	cc.ensureGucState().ObserveSQL(sql)
	cc.observeForPool(sql)

	// Top-level SELECT <ident>(...) shapes are stored-function calls
	// whose body may have done a server-side SET we'll never see on
	// the wire. Mark the connection dirty (the next checkout will
	// verify) and schedule an async verify so the dirty window is
	// short. Excludes set_config — already handled inline by ObserveSQL.
	if cc.shouldScheduleVerifyAfter(sql) {
		cc.ensureGucState().MarkDirty()
		defer cc.scheduleAsyncVerify()
	}

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
	cc.maybeVerifyOnCheckout(ctx)
	cc.ensureGucState().ObserveSQL(sql)
	cc.observeForPool(sql)
	if cc.shouldScheduleVerifyAfter(sql) {
		cc.ensureGucState().MarkDirty()
		defer cc.scheduleAsyncVerify()
	}

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
	cc.maybeVerifyOnCheckout(ctx)
	cc.ensureGucState().ObserveSQL(sql)
	cc.observeForPool(sql)
	if cc.shouldScheduleVerifyAfter(sql) {
		cc.ensureGucState().MarkDirty()
		defer cc.scheduleAsyncVerify()
	}

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
