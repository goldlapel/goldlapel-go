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
//   - After observing a top-level SELECT <ident>(...) the wrapper marks
//     dirty and schedules an async verify in a background goroutine
//     (verifyWG / verifyCtx). The async path keeps the user's hot path
//     unblocked.
//
//   - While gucState.IsDirty() is true, Query/QueryRow bypass the L1
//     cache and route directly to the underlying Querier. The dirty
//     window closes when the async verify Reseeds state from
//     pg_settings, or when a session-clearing command (RESET ALL /
//     DISCARD ALL) lands. No synchronous verify-on-checkout — the hot
//     path never blocks on a verify roundtrip.
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

	// realMu serialises calls into cc.real. The wrapper assumes the
	// underlying Querier follows pgx.Conn's "goroutine-confined"
	// convention — but the async verify path spawns a goroutine that
	// also issues `SELECT name, setting FROM pg_settings ...` against
	// cc.real. Without a mutex, the verify goroutine and a concurrent
	// user-issued query would race on a non-goroutine-safe connection
	// (raw pgx.Conn, sql.Conn). The mutex is held only for the
	// duration of the cc.real call itself; it does NOT protect
	// caller-side iteration of the returned Rows (callers must
	// already serialise their own usage of pgx.Rows).
	//
	// For pgxpool / sql.DB users where cc.real IS goroutine-safe,
	// the mutex is essentially free (uncontended). For raw-conn
	// users, the mutex is what makes the async verify path safe.
	realMu sync.Mutex

	// Aggressive post-DML mode. See aggressive_verify.go for the full
	// design. Per-CachedConn override (mode + Set sentinel) piggybacks
	// on atomic.Int32 / atomic.Bool so the read path
	// (Query/QueryRow/Exec) doesn't need a mutex. Resolution falls back
	// to the *GoldLapel option and finally the package default (Auto).
	aggressiveVerifyMode    atomic.Int32 // AggressiveVerifyMode value when aggressiveVerifyModeSet=true
	aggressiveVerifyModeSet atomic.Bool  // true once SetAggressiveVerify has been called
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
	cc := &CachedConn{
		real:     conn,
		cache:    cache,
		gucState: NewConnectionGucState(),
	}
	return cc
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

// Close cancels any in-flight async verify goroutines and waits for
// them to exit. Safe to call multiple times; subsequent calls no-op.
// After Close, scheduleAsyncVerify is a no-op so a follow-up
// Query/QueryRow/Exec on a "closed" CachedConn still routes the user's
// SQL to the underlying Querier (verify-on-checkout still fires
// synchronously if state is dirty) — only the async post-call path
// is suppressed, since dispatching a fresh goroutine against a
// connection the caller is about to discard would race with the
// caller's own teardown.
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

// runVerify reads `pg_settings WHERE source = 'session'` directly from
// the underlying Querier and Reseeds the GUC state map. Runs in the
// background async-verify goroutine (see scheduleAsyncVerify); the
// hot path no longer fires a synchronous verify — instead, the dirty
// flag steers reads to bypass L1 until this async refresh clears it.
// Returns nothing because errors are intentionally swallowed: a
// transient verify failure leaves the dirty flag set so subsequent
// reads continue to bypass L1 until a later verify succeeds or a
// session-clearing command resolves the uncertainty.
//
// The verify SQL filters to source='session' so we only see GUCs the
// user actually changed (default values stay out of the map).
// Reseed's filter then drops anything that isn't an unsafe GUC, so
// the final state map matches the canonical SET-observed shape.
//
// realMu is taken for the duration of the cc.real.Query and the row
// drain — pgx.Conn semantics require single-goroutine access to the
// conn AND its outstanding Rows. Holding the lock for the whole
// drain ensures verify doesn't overlap with a concurrent user-issued
// Query/QueryRow/Exec on the same connection (pgxpool / sql.DB users
// won't notice; raw pgx.Conn users can rely on serialization).
func (cc *CachedConn) runVerify(ctx context.Context) {
	const verifySQL = "SELECT name, setting FROM pg_settings WHERE source = 'session'"
	cc.realMu.Lock()
	defer cc.realMu.Unlock()
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

// fetchAndDrain takes realMu, issues cc.real.Query, fully drains the
// Rows into a [][]interface{} slice, and releases realMu — all under
// a defer so a panic in any downstream call still releases the
// mutex. Used by Query's cache-miss path; the resulting slice is
// what gets stored in the native cache.
//
// Returns:
//   - collected: rows successfully iterated (possibly partial).
//   - fields:    column metadata.
//   - drainErr:  rows.Err() observed during iteration. Surfaced via
//     the returned cachedRows.err so the caller sees it on first
//     iteration — preserves the pre-realMu wrapper contract.
//   - queryErr:  cc.real.Query's own error. Propagated to the user
//     directly; collected/fields are nil in this case.
func (cc *CachedConn) fetchAndDrain(ctx context.Context, sql string, args ...interface{}) (collected [][]interface{}, fields []FieldDescription, drainErr error, queryErr error) {
	cc.realMu.Lock()
	defer cc.realMu.Unlock()
	rows, err := cc.real.Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, nil, err
	}
	if rows == nil {
		return nil, nil, nil, nil
	}
	defer rows.Close()
	fields = rows.FieldDescriptions()
	for rows.Next() {
		vals, vErr := rows.Values()
		if vErr != nil {
			break
		}
		dup := make([]interface{}, len(vals))
		copy(dup, vals)
		collected = append(collected, dup)
	}
	drainErr = rows.Err()
	return collected, fields, drainErr, nil
}

// realQuery takes realMu briefly while issuing cc.real.Query and
// hands the live Rows back to the caller. Used by branches that
// don't drain rows themselves (the BEGIN / COMMIT / ddl-hit / in-tx
// paths). The mutex protects against an in-flight verify goroutine
// concurrently issuing pg_settings against the same connection.
//
// The mutex is NOT held during the caller's subsequent iteration of
// the returned Rows — that's a per-Querier concern (pgx.Conn forbids
// it; pgxpool / sql.DB tolerate it). Callers that ALSO mix live
// Rows with concurrent verify-eligible queries on raw pgx.Conn are
// outside the wrapper's safety boundary; this is a pre-existing
// pgx.Conn convention, not a new wrapper limitation.
func (cc *CachedConn) realQuery(ctx context.Context, sql string, args ...interface{}) (Rows, error) {
	cc.realMu.Lock()
	rows, err := cc.real.Query(ctx, sql, args...)
	cc.realMu.Unlock()
	return rows, err
}

// realQueryRow mirrors realQuery for the single-row API. pgx.Row /
// sql.Row hold a connection until Scan is called; the mutex is
// taken only for the QueryRow call itself.
func (cc *CachedConn) realQueryRow(ctx context.Context, sql string, args ...interface{}) Row {
	cc.realMu.Lock()
	row := cc.real.QueryRow(ctx, sql, args...)
	cc.realMu.Unlock()
	return row
}

// realExec mirrors realQuery for Exec. Exec returns immediately so
// the mutex is held for the full call.
func (cc *CachedConn) realExec(ctx context.Context, sql string, args ...interface{}) (interface{}, error) {
	cc.realMu.Lock()
	defer cc.realMu.Unlock()
	return cc.real.Exec(ctx, sql, args...)
}

// applyPoolEffectFromCmds propagates a pre-parsed command list into
// the pool discarder (if attached). Idempotent under repeated calls
// for the same SQL; the wrapper invokes this from settleObservation
// only when the underlying Querier returned nil error, so failed SETs
// don't dirty the pool.
//
// Per-command effect:
//
//   - Unsafe SET / RESET / set_config(false) → pool dirty (the
//     connection now carries session state another borrower would
//     inherit on pool reuse).
//   - DISCARD ALL / RESET ALL → pool clean (PG has cleared session
//     state authoritatively).
//   - SET LOCAL / set_config(..., true) → pool unaffected (LOCAL
//     scope is automatically dropped at COMMIT/ROLLBACK).
//   - DISCARD PLANS / SEQUENCES / TEMP / TEMPORARY → pool unaffected
//     (these don't touch GUC state, so the pool dirty axis doesn't
//     move).
//
// Multi-statement bodies feed every parsed command in order — RESET
// ALL / DISCARD ALL anywhere in the list clears the dirty bit (PG's
// actual behaviour: DISCARD inside a script resets state at that
// point).
func (cc *CachedConn) applyPoolEffectFromCmds(cmds []SetCommand) {
	if cc.poolDiscarder == nil {
		return
	}
	for _, cmd := range cmds {
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
		case SetCmdDiscardOther, SetCmdSetLocal, SetCmdNone:
			// No pool effect.
		}
	}
}

// pendingObservation is the wrapper-level bundle of every state
// mutation a single Query / QueryRow / Exec call observed on the wire
// — the GUC-state hash mutation, the pool-discarder dirty/clean
// effect, and the inTransaction flag flip. None of these are committed
// until the Querier reports nil error; on error, settle() drops the
// pending mutations and (when at least one mutation was observed)
// marks the GUC state dirty so the next checkout reconciles via
// verify-against-pg_settings.
//
// This is the SET-actually-applied protocol. Without it, the wrapper
// eagerly mutates state on observation, which means a SET that errored
// at the wire (network blip, syntax error, permission denied) would
// still move the wrapper's hash — divergence from the server's actual
// session state, which fragments cache slots and (for an errored
// RESET) opens an RLS-shaped correctness hole when baseline-cached
// rows get served under non-baseline server state.
//
// Multi-statement edge case: pgx returns the LAST error encountered,
// and PG semantics say earlier successful segments still apply
// server-side. We can't tell from a single err which segments landed,
// so DiscardAfterError marks dirty — the next checkout's verify
// queries pg_settings authoritatively. The cost is one verify on the
// unhappy path; the win is provable correctness.
type pendingObservation struct {
	guc     *PendingObservation
	cmds    []SetCommand // parsed commands; shared with guc.cmds
	txAfter bool         // post-body inTransaction value (only valid when hasTx==true)
	hasTx   bool         // true if SQL contained any transaction-control segment
}

// preparePending parses sql once for state-mutation commands and tx
// control, stages every effect without applying it, and returns the
// staged bundle. The caller must finalise via settle() — applying on
// success, discarding (with implicit dirty mark) on error.
//
// Cheap on SQL with no SET / RESET / DISCARD / BEGIN / COMMIT —
// reduces to a single ParseSetCommand fast-path call plus the
// containsTxControl boolean check.
func (cc *CachedConn) preparePending(sql string) *pendingObservation {
	state := cc.ensureGucState()
	pending := &pendingObservation{
		guc:     state.Pending(sql),
		txAfter: cc.inTransaction,
	}
	pending.cmds = pending.guc.cmds
	if containsTxControl(sql) {
		pending.hasTx = true
		pending.txAfter = applyTxState(sql, cc.inTransaction)
	}
	return pending
}

// settle commits or rolls back the staged observation based on err
// from the Querier.
//
// Success path (err == nil):
//   - Apply parsed SET / RESET / DISCARD commands to the GUC state hash.
//   - Propagate observed effects into the pool discarder.
//   - Flip cc.inTransaction to the post-body value when the body
//     contained tx control.
//
// Error path (err != nil):
//   - Drop the staged GUC commands. If any state-mutation command was
//     observed, mark the GUC state dirty so the next checkout fires a
//     verify-against-pg_settings — this is the safety net for
//     multi-statement bodies that may have applied earlier segments
//     before failing.
//   - Skip pool-dirty propagation; if the Exec failed the pool
//     borrower hasn't moved server state we'd want to DISCARD on
//     release.
//   - Leave cc.inTransaction untouched at its pre-call value. A
//     failed BEGIN never opened a transaction; a failed COMMIT
//     leaves the transaction open server-side; a failed ROLLBACK
//     SHOULD have rolled back, but our safest default is to defer
//     to verify-on-checkout (same as the multi-stmt SET case): a
//     genuine ROLLBACK failure is rare and recovers on the next
//     successful tx-control statement.
func (cc *CachedConn) settle(p *pendingObservation, err error) {
	if p == nil {
		return
	}
	if err == nil {
		p.guc.Apply()
		cc.applyPoolEffectFromCmds(p.cmds)
		if p.hasTx {
			cc.inTransaction = p.txAfter
		}
		return
	}
	p.guc.DiscardAfterError()
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

// maybeBumpDmlSeq advances the per-connection dml_seq counter when
// `sql` looks like a DML statement (INSERT/UPDATE/DELETE/MERGE/TRUNCATE
// — see looksLikeDML) and the effective aggressive-verify mode for
// this CachedConn is on (Auto/On). The bump rolls the connection's
// state hash forward so any subsequent cacheable read on this
// connection lands in a fresh cache slot — closing the
// trigger-internal-SET correctness gap at zero round-trip cost. See
// aggressive_verify.go for the resolution order.
//
// Called from Query/QueryRow/Exec right after preparePending. Returns
// true if a bump fired (purely informational; the wrapper doesn't
// branch on the return value).
func (cc *CachedConn) maybeBumpDmlSeq(sql string) bool {
	if !looksLikeDML(sql) {
		return false
	}
	if !cc.aggressiveVerifyEnabled() {
		return false
	}
	cc.ensureGucState().BumpDmlSeq()
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
	// Per-connection unsafe-GUC tracking. Every wire-bound SQL string is
	// parsed for SET / RESET / DISCARD, but mutations are STAGED rather
	// than applied — settle commits on Querier success, discards (and
	// marks dirty) on error. The cache lookup below uses the pre-call
	// hash, which is the right slot: if a SET is in this body and the
	// call succeeds, the new hash applies to the NEXT call's lookup.
	pending := cc.preparePending(sql)

	// Top-level SELECT <ident>(...) shapes are stored-function calls
	// whose body may have done a server-side SET we'll never see on
	// the wire. Mark the connection dirty (the next checkout will
	// verify) and schedule an async verify so the dirty window is
	// short. Excludes set_config — already handled inline by ObserveSQL.
	// Note: dirty is marked unconditionally (independent of Querier
	// success) — the dirty flag is the safety net for "we don't know
	// what the function body did", and even an erroring function call
	// may have done a server-side SET before failing.
	if cc.shouldScheduleVerifyAfter(sql) {
		cc.ensureGucState().MarkDirty()
		defer cc.scheduleAsyncVerify()
	}
	// Aggressive-verify (always-on under Auto/On): every DML statement
	// bumps the per-connection dml_seq, rolling the state hash forward
	// so subsequent cacheable reads on this connection can't share a
	// cache slot with a pre-DML peer. Closes the trigger-internal-SET
	// coverage gap at zero round-trip cost. See aggressive_verify.go.
	cc.maybeBumpDmlSeq(sql)

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

	// Transaction-control / write / in-tx paths bypass the cache entirely
	// and dispatch straight to the underlying Querier. Settle pending
	// based on the Querier's err — the SET-actually-applied protocol
	// applies to every wire-bound dispatch, not just SET-bearing SQL.
	if pending.hasTx || ddlHit || len(writeTables) > 0 || cc.inTransaction {
		rows, err := cc.realQuery(ctx, sql, args...)
		cc.settle(pending, err)
		return rows, err
	}

	// Dirty-flag bypass: when the wrapper has any uncertainty about the
	// connection's session state (a recent top-level SELECT my_func()
	// could have done a server-side SET; an errored SET observation may
	// have left server state diverged from wire-side tracking), route
	// this read AND every subsequent read directly to the proxy until
	// an authoritative refresh resolves the uncertainty. Skipping L1
	// here means a current-call cache hit can't serve stale rows under
	// possibly-mutated server state. The proxy has its own GUC-aware
	// state-hash + cache so we don't lose all caching — just the L1
	// layer's slot, which is the only one with stale information.
	state := cc.ensureGucState()
	if state.IsDirty() {
		rows, err := cc.realQuery(ctx, sql, args...)
		cc.settle(pending, err)
		return rows, err
	}

	// Cacheable read path: use the pre-call hash (settle hasn't run yet).
	// Same-call SETs route through the bypass branches above; this branch
	// only runs for non-SET, non-tx-control, non-write SQL where the
	// pre-call hash IS the right cache slot.
	stateHash := state.Hash()

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
			// Cache hit — we never sent SQL to the wire. Settle as
			// success: pending has no SET commands for non-SET SQL
			// (the bypass branches catch SETs), so this is a no-op.
			cc.settle(pending, nil)
			return &cachedRows{rows: rows, fields: fields, pos: -1}, nil
		}
	}

	// Cache miss: execute for real. Hold realMu across the full
	// drain — pgx.Conn forbids overlapping use of the conn while a
	// Rows is alive, and the verify goroutine takes the same mutex
	// so the two paths can't race on a non-goroutine-safe Querier.
	collected, fields, drainErr, err := cc.fetchAndDrain(ctx, sql, args...)
	if err != nil {
		// fetchAndDrain has already released realMu.
		cc.settle(pending, err)
		return nil, err
	}

	// fetchAndDrain succeeded at the Query level (rows.Err() may still
	// surface a drain-side issue, but the SQL did dispatch and the
	// server processed it — earlier multi-stmt segments DID apply).
	// Settle as success.
	cc.settle(pending, nil)

	// Cache the result under the current per-connection state hash —
	// unless this was a session-state command (SET / RESET / LISTEN /
	// NOTIFY / etc.) routed through Query. Those return zero rows but
	// the truthy-tail Put would still bloat the LRU with empty entries
	// that can never serve a real read. See
	// docs/todos/wrapper-cache-set-responses.md.
	if !isSessionStateCommand(sql) {
		cc.cache.Put(sql, args, stateHash, collected, fields)
	}

	return &cachedRows{rows: collected, fields: fields, pos: -1, err: drainErr}, nil
}

// QueryRow intercepts single-row queries.
//
// pgx.Row delivers errors at Scan() time, not at QueryRow-return time
// — the underlying conn is held until Scan reads the response. To
// honour the SET-actually-applied protocol on this surface, the
// returned Row is wrapped in a settlingRow that observes the Scan err
// and runs settle(pending, err) before returning to the caller. Cache
// hits and the realQueryRow bypass paths each settle in their own
// branch; the cache-miss-via-Query fallback piggybacks on Query's
// own settle (Query owns its pending), so QueryRow's pending is
// committed/discarded synchronously alongside the inner Query call
// (no settlingRow needed there).
func (cc *CachedConn) QueryRow(ctx context.Context, sql string, args ...interface{}) Row {
	pending := cc.preparePending(sql)
	if cc.shouldScheduleVerifyAfter(sql) {
		cc.ensureGucState().MarkDirty()
		defer cc.scheduleAsyncVerify()
	}
	cc.maybeBumpDmlSeq(sql)

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

	// Bypass the read cache for tx-control / write / in-tx bodies and
	// wrap the underlying Row in a settlingRow so the SET-actually-
	// applied protocol observes the deferred Scan err.
	if pending.hasTx || ddlHit || len(writeTables) > 0 || cc.inTransaction {
		row := cc.realQueryRow(ctx, sql, args...)
		return &settlingRow{row: row, cc: cc, pending: pending}
	}

	// Dirty-flag bypass: see Query() for the full rationale. Skipping
	// L1 here means a current-call cache hit can't serve stale rows
	// under possibly-mutated server state.
	state := cc.ensureGucState()
	if state.IsDirty() {
		row := cc.realQueryRow(ctx, sql, args...)
		return &settlingRow{row: row, cc: cc, pending: pending}
	}

	// Cacheable read path. Use the pre-call hash so a same-body SET
	// (which routes through the bypass above anyway) doesn't influence
	// the cache slot for non-SET reads.
	stateHash := state.Hash()

	// Read path: check L1 cache
	entry := cc.cache.Get(sql, args, stateHash)
	if entry != nil {
		rows, ok := entry.Rows.([][]interface{})
		// Cache hit — we never sent SQL to the wire. Settle as success
		// (no-op for non-SET SQL).
		cc.settle(pending, nil)
		if ok && len(rows) > 0 {
			return &cachedRow{values: rows[0]}
		}
		return &cachedRow{values: nil}
	}

	// Cache miss: route through Query so the cache populates uniformly
	// with the multi-row path. Query owns its OWN pending observation
	// over the same SQL — it parses, settles, applies idempotently. We
	// settle THIS layer's pending alongside Query's err, which has
	// already been settled inside Query; the double-settle is safe
	// because Apply on already-applied commands is a hash-stable no-op
	// (sorted-key recomputeHashLocked produces the same hash from the
	// same value map). DiscardAfterError is also idempotent.
	rows, err := cc.Query(ctx, sql, args...)
	cc.settle(pending, err)
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
//
// SET-actually-applied protocol: the SET / RESET / DISCARD parsing and
// the inTransaction flip are STAGED in `pending` and committed only
// after the underlying Querier reports nil err. On err, the staged
// effects are discarded; if any state-mutation command was observed,
// the GUC state is marked dirty so the next checkout's verify-against-
// pg_settings reconciles authoritatively (the right safety net for
// multi-statement bodies that may have applied earlier segments
// before the failing one).
func (cc *CachedConn) Exec(ctx context.Context, sql string, args ...interface{}) (interface{}, error) {
	pending := cc.preparePending(sql)
	if cc.shouldScheduleVerifyAfter(sql) {
		cc.ensureGucState().MarkDirty()
		defer cc.scheduleAsyncVerify()
	}
	cc.maybeBumpDmlSeq(sql)

	// Multi-statement-aware write detection — runs ahead of tx tracking
	// so a `BEGIN; INSERT INTO t ...; COMMIT` body still invalidates t.
	// Cache invalidation fires unconditionally (even for failed writes):
	// over-invalidation is safe — the next read just reloads from the
	// origin — whereas under-invalidation could serve stale data, which
	// is the unsafe direction.
	writeTables, ddlHit := detectWriteMulti(sql)
	if ddlHit {
		cc.cache.InvalidateAll()
	} else {
		for _, t := range writeTables {
			cc.cache.InvalidateTable(t)
		}
	}

	result, err := cc.realExec(ctx, sql, args...)
	cc.settle(pending, err)
	return result, err
}

// settlingRow wraps a Row so that the underlying Querier's deferred
// Scan-time error can drive the SET-actually-applied protocol — the
// settle hook fires on the FIRST Scan call, applying or discarding
// the staged pending observation based on the scan err.
//
// Why first Scan: pgx.Row.Scan is the canonical materialisation point
// for the underlying conn's response. Before Scan, the conn may not
// have read the response yet; after Scan, the err is authoritative.
// Multiple Scan calls on the same Row are supported by pgx (the
// second returns the same err), so settlingRow guards the settle so
// it fires exactly once (further Scans are pass-through).
type settlingRow struct {
	row     Row
	cc      *CachedConn
	pending *pendingObservation
	settled atomic.Bool
}

func (sr *settlingRow) Scan(dest ...interface{}) error {
	err := sr.row.Scan(dest...)
	// Settle on first Scan only. Any err — including pgx.ErrNoRows /
	// sql.ErrNoRows surfaced for SETs routed through QueryRow — is
	// treated as "discard staged pending + mark dirty if mutating".
	// For non-SET SQL the discard is a no-op (pending has no cmds),
	// so legitimate "no rows matched" SELECTs are unaffected. For the
	// rare SET-via-QueryRow path, the dirty flag triggers a verify on
	// the next checkout that reseeds against pg_settings — at the
	// cost of one extra roundtrip on an already-pathological route.
	if sr.settled.CompareAndSwap(false, true) {
		sr.cc.settle(sr.pending, err)
	}
	return err
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
