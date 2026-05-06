// Aggressive post-DML verify (smart-auto-enable).
//
// Background. The Option Y wire-side GUC tracker (see guc_state.go) catches
// every `SET` / `RESET` / `set_config` the wrapper sees in the SQL stream.
// What it CAN'T see is a server-side state change made by a TRIGGER body
// firing on INSERT/UPDATE/DELETE — the wire layer never observes the SET
// because it happens entirely inside Postgres. This is the
// "trigger-internal SET" gap documented at
// `goldlapel/docs/todos/aggressive-verify-flag.md` and called out in
// `docs/todos/guc-rls-cache-safety.md` as a v1 limitation.
//
// Mitigation: after every DML statement (INSERT/UPDATE/DELETE/MERGE/
// TRUNCATE), schedule the same async verify the
// `SELECT <fn>(...)` post-call path already uses (Wave 1's verifyCtx /
// verifyCancel / verifyWG lifecycle, gated by the inFlightVerify CAS).
// Cost: ~one verify roundtrip per write burst (the in-flight gate
// coalesces a flurry of writes onto a single verify). No read-path
// impact.
//
// Smart-auto-enable. The customer should not have to manually opt into
// this — the wrapper figures it out:
//
//  1. If WithAggressiveVerify(On) was supplied, post-DML verify is on
//     unconditionally.
//  2. If WithAggressiveVerify(Off) was supplied, post-DML verify is off.
//  3. Otherwise (Auto, the default):
//     - License payload: if `aggressive_verify_active` is true (set by
//       the proxy / HQ on the customer's account), turn it on.
//     - Detection: on the FIRST CachedConn use against this upstream,
//       run the detection SQL — `pg_proc` rows for trigger functions
//       whose body contains a `SET` / `set_config` / `PERFORM
//       set_config`. If we find any, the database HAS the
//       trigger-internal-SET pattern; turn it on. Cache the result by
//       upstream URL so subsequent CachedConns on the same database
//       pay no detection tax.
//
// Detection SQL is best-effort: a false negative (we missed a trigger)
// degrades to today's behavior (potential RLS leak under
// trigger-internal SETs); a false positive costs ~1ms per write. The
// safer bias is towards enabling — wrong-on means a tiny perf cost,
// wrong-off means a correctness gap. Detection failure (transient
// connection error, permission denied on pg_proc) defaults to OFF
// (don't burden every write on a transient unavailability).
//
// Override flag: WithAggressiveVerify(mode) functional option exposed
// via *GoldLapel; the wrapper reads it through lastStartedInstance the
// same way detectInvalidationPort does (best-effort — tests and
// out-of-process spawns can use SetAggressiveVerifyActive directly).

package goldlapel

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
)

// AggressiveVerifyMode controls the post-DML async-verify behavior of
// every CachedConn associated with this proxy instance. See package
// docs for the smart-auto-enable rationale.
type AggressiveVerifyMode int

const (
	// AggressiveVerifyAuto is the default: enable post-DML verify when
	// either the license payload or the per-database detection probe
	// reports the trigger-internal-SET pattern is present. This is the
	// "GL figures it out" mode.
	AggressiveVerifyAuto AggressiveVerifyMode = iota
	// AggressiveVerifyOn forces post-DML verify on regardless of
	// detection. ~1ms tax per write; full coverage of the
	// trigger-internal-SET gap. Pick this if you know your schema has
	// triggers that mutate session state and prefer belt-and-suspenders.
	AggressiveVerifyOn
	// AggressiveVerifyOff forces post-DML verify off regardless of
	// detection. The trigger-internal-SET gap is left open — see
	// docs/todos/guc-rls-cache-safety.md for the consequences.
	AggressiveVerifyOff
)

// String renders the mode for log lines / panics.
func (m AggressiveVerifyMode) String() string {
	switch m {
	case AggressiveVerifyAuto:
		return "auto"
	case AggressiveVerifyOn:
		return "on"
	case AggressiveVerifyOff:
		return "off"
	default:
		return "unknown"
	}
}

// aggressiveVerifyDetectionCache memoises the detection result per
// upstream URL across the package. Keyed by upstream URL so multiple
// *GoldLapel instances pointing at the same database share a single
// detection probe; subsequent CachedConn instances reuse the cached
// boolean instead of re-probing pg_proc on every spawn.
//
// Concurrent first-use is safe: sync.Map's LoadOrStore semantics let
// two callers race on first access without either of them observing a
// missing entry — the loser's probe result is discarded in favor of
// the winner's. (The probe is idempotent so this is fine.)
//
// Values stored are `bool`. A false entry means "we checked, no
// triggers with internal SETs"; a true entry means "we found at least
// one". Detection failure is NOT cached — the next CachedConn retries
// (a transient permission error shouldn't disable verify
// permanently).
var aggressiveVerifyDetectionCache sync.Map

// licenseAggressiveVerifyActive is the wrapper's hook for the
// license-payload signal. The proxy / HQ flips this via
// SetAggressiveVerifyActive when the customer's license payload
// reports `aggressive_verify_active=true`. Default false; the
// detection probe still gets a chance to enable verify when this is
// false.
//
// Atomic so the proxy startup goroutine and the wrapper's hot path
// don't race on it.
var licenseAggressiveVerifyActive atomic.Bool

// SetAggressiveVerifyActive is the public hook for the license /
// payload layer to push the customer-account-level
// `aggressive_verify_active` flag into the wrapper. Called from the
// proxy startup path when license payload arrives, and from tests
// directly. Idempotent and safe for concurrent calls.
//
// Effect: when active=true, every CachedConn in this process treats
// AggressiveVerifyAuto as "on" (without running the detection probe);
// active=false leaves the auto-detect path to make the call.
//
// Explicit per-CachedConn modes (On/Off) ignore this signal — they
// override.
func SetAggressiveVerifyActive(active bool) {
	licenseAggressiveVerifyActive.Store(active)
}

// AggressiveVerifyActive reports the current license-payload signal.
// Exposed primarily for tests; production callers don't need to
// introspect.
func AggressiveVerifyActive() bool {
	return licenseAggressiveVerifyActive.Load()
}

// ResetAggressiveVerifyDetectionCache clears the per-upstream
// memoisation. Used by tests to re-exercise detection between cases;
// production callers don't need it.
func ResetAggressiveVerifyDetectionCache() {
	aggressiveVerifyDetectionCache = sync.Map{}
}

// aggressiveVerifyDetectSQL is the probe that decides whether a
// database carries the trigger-internal-SET pattern. We look for
// trigger functions whose body literally contains a SET command or a
// set_config call — both forms map to a server-side GUC mutation the
// wrapper can't observe on the wire.
//
// Why pg_proc + pg_trigger join: pg_trigger.tgfoid points at the
// trigger function's pg_proc oid; pg_proc.prosrc is the function body.
// We filter on tgenabled != 'D' so disabled triggers don't trip the
// probe. The body match is case-insensitive (PG identifiers on the
// LHS of SET are case-insensitive; users mix `SET app.user_id` with
// `set app.user_id` freely).
//
// LIMIT 1 short-circuits — we only need ONE positive hit to flip the
// verdict. Production schemas with hundreds of triggers don't have to
// pay a full-table scan.
//
// Best-effort by design: this misses triggers whose function body
// imports another function via PERFORM that itself does the SET (one
// indirection level). For the common pattern (`CREATE FUNCTION ...
// LANGUAGE plpgsql AS $$ BEGIN PERFORM set_config(...); ... $$`) we
// catch it. For deeper nesting we degrade to the next-best signal
// (license payload or explicit On).
// Match patterns deliberately scoped:
//
//   - `set_config(` catches the function form (Supabase's canonical
//     JWT pattern, GIS extension, etc.).
//   - `perform set_config(` catches plpgsql's PERFORM wrapping of
//     set_config — common for trigger bodies that don't care about the
//     return value.
//   - `set ` followed by a word containing `.` (e.g. `SET app.user_id`,
//     `SET myapp.tenant`) catches namespaced GUCs, which is the
//     overwhelmingly common shape for RLS-style state.
//   - `set search_path` / `set role` / `set session_authorization`
//     catch the unsafe-by-default GUCs from guc_state.go.
//
// We do NOT broadly match `%set %` — that would over-match on words
// like `setting`, `settable`, `offset`, etc. and false-positive on
// triggers whose body just CONTAINS the substring without a real SET
// command. False-positive cost is real (every write pays ~1ms tax),
// so the probe biases toward precision over recall — combined with
// the explicit `On` override and the license-payload signal, a
// false-negative degrades gracefully to today's behavior; a
// false-positive imposes an unnecessary tax on every write.
const aggressiveVerifyDetectSQL = `
SELECT 1
FROM pg_trigger t
JOIN pg_proc p ON p.oid = t.tgfoid
WHERE t.tgenabled <> 'D'
  AND (
    p.prosrc ILIKE '%set_config(%'
    OR p.prosrc ILIKE '%perform set_config(%'
    OR p.prosrc ~* '\mset\s+\w+\.\w+'
    OR p.prosrc ~* '\mset\s+search_path\M'
    OR p.prosrc ~* '\mset\s+role\M'
    OR p.prosrc ~* '\mset\s+session_authorization\M'
  )
LIMIT 1
`

// detectAggressiveVerifyNeeded runs the detection probe against the
// supplied Querier and returns true when the database has at least
// one enabled trigger function whose body looks like it does a
// server-side SET / set_config.
//
// Caching is keyed on upstreamKey — empty string skips the cache (the
// caller is in a context where database identity isn't reliably
// known, e.g. raw conn fixtures in tests; we re-probe every call).
//
// Concurrency: sync.Map.LoadOrStore lets concurrent first-callers
// race the probe without observing a missing entry. The loser's
// probe result is discarded in favor of the winner's. The probe SQL
// is idempotent (it doesn't mutate state) so duplicate work is the
// only cost, and only on the first concurrent burst per upstream.
//
// Detection failure (Querier.Query returns error, rows.Err non-nil,
// permission denied on pg_proc, etc.) returns false WITHOUT caching
// — a transient unavailability shouldn't durably disable verify; the
// next CachedConn retries.
func detectAggressiveVerifyNeeded(ctx context.Context, q Querier, upstreamKey string) bool {
	if upstreamKey != "" {
		if cached, ok := aggressiveVerifyDetectionCache.Load(upstreamKey); ok {
			if b, ok := cached.(bool); ok {
				return b
			}
		}
	}

	rows, err := q.Query(ctx, aggressiveVerifyDetectSQL)
	if err != nil {
		return false
	}
	defer rows.Close()
	found := false
	if rows.Next() {
		found = true
	}
	if rows.Err() != nil {
		return false
	}

	if upstreamKey != "" {
		aggressiveVerifyDetectionCache.Store(upstreamKey, found)
	}
	return found
}

// looksLikeDML returns true when sql's first non-whitespace token
// (or the first segment's first token, for a multi-statement body)
// is one of the DML verbs that triggers post-call aggressive verify:
// INSERT, UPDATE, DELETE, MERGE, TRUNCATE.
//
// This is intentionally narrower than detectWrite's notion of
// "writes" — DDL (CREATE/ALTER/DROP/REFRESH/DO/CALL) is excluded
// because triggers don't fire on DDL. COPY is also excluded; while
// COPY ... FROM does insert rows, the trigger-internal-SET pattern
// targets row-by-row INSERT/UPDATE/DELETE handlers, not bulk-loader
// triggers (and PG fires per-row triggers on COPY anyway, so an
// INSERT we already counted via detectWriteMulti would also produce
// the verify trigger via the COPY path — keeping COPY out avoids
// double-counting).
//
// Multi-statement bodies: returns true if ANY top-level segment is
// DML, not just the first. A body like `SET app.user_id = '42';
// INSERT INTO t VALUES (1)` does need post-DML verify.
//
// Fast path: bodies with no ';' check the first token directly,
// avoiding the splitter allocation.
func looksLikeDML(sql string) bool {
	if !strings.ContainsRune(sql, ';') {
		return firstTokenIsDML(sql)
	}
	for _, seg := range SplitStatements(sql) {
		if firstTokenIsDML(seg) {
			return true
		}
	}
	return false
}

// firstTokenIsDML reports whether sql's first SQL token (after
// optional CTE-prefix) is INSERT/UPDATE/DELETE/MERGE/TRUNCATE.
//
// CTE handling: `WITH ... INSERT INTO ...` (or UPDATE/DELETE) is also
// DML. The detectWrite function already classifies these via its own
// scan. We mirror that logic here so a CTE-with-INSERT body still
// counts.
func firstTokenIsDML(sql string) bool {
	trimmed := strings.TrimLeft(sql, " \t\r\n(")
	end := 0
	for end < len(trimmed) {
		c := trimmed[end]
		if c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == ';' || c == '(' {
			break
		}
		end++
	}
	if end == 0 {
		return false
	}
	first := strings.ToUpper(trimmed[:end])
	switch first {
	case "INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE":
		return true
	case "WITH":
		// CTE prefix — scan the body for one of the DML keywords. Use
		// the same scan shape as detectWrite (matches stripped of
		// parens / commas).
		restUpper := strings.ToUpper(trimmed[end:])
		for _, token := range strings.Fields(restUpper) {
			word := strings.TrimLeft(token, "(")
			if word == "INSERT" || word == "UPDATE" || word == "DELETE" {
				return true
			}
		}
	}
	return false
}

// WithAggressiveVerify sets the post-DML aggressive-verify mode for
// every CachedConn associated with this *GoldLapel instance.
//
// Default: AggressiveVerifyAuto — the wrapper consults the license
// payload first, then runs a per-database detection probe on the
// first CachedConn use, caches the verdict, and applies it to every
// subsequent CachedConn against the same upstream. Explicit
// AggressiveVerifyOn / AggressiveVerifyOff overrides skip both
// signals.
//
// Equivalent CLI flag (proxy side): --verify-after-all-dml.
// Equivalent env var: GOLDLAPEL_VERIFY_AFTER_ALL_DML.
//
// Construction-time only.
func WithAggressiveVerify(mode AggressiveVerifyMode) Option {
	return startOnly(func(gl *GoldLapel) {
		gl.aggressiveVerify = mode
		gl.aggressiveVerifySet = true
	})
}

// SetAggressiveVerify sets the per-CachedConn aggressive-verify
// override. When mode is AggressiveVerifyAuto, the wrapper falls
// back to the *GoldLapel-level mode (or, if no instance is
// registered, to the package default of Auto). Explicit On/Off
// overrides every other signal — license payload and detection
// probe alike.
//
// Returns the same CachedConn so the call chain reads naturally:
//
//	cc := goldlapel.Wrap(conn, port).SetAggressiveVerify(goldlapel.AggressiveVerifyOn)
func (cc *CachedConn) SetAggressiveVerify(mode AggressiveVerifyMode) *CachedConn {
	cc.aggressiveVerifyMode.Store(int32(mode))
	cc.aggressiveVerifyModeSet.Store(true)
	return cc
}

// SetAggressiveVerifyUpstream stamps the upstream URL onto cc so the
// detection-cache key for this CachedConn matches other CachedConn
// instances against the same database. Empty string is the unset
// sentinel — detection caches under "", which means re-probe per
// CachedConn (ok for tests; sub-optimal for production).
//
// Wrap()-via-*GoldLapel sets this automatically from gl.upstream.
// Hand-rolled CachedConn construction can call this method directly
// when the caller knows the upstream identity.
//
// Returns cc so the call reads naturally as a chain.
func (cc *CachedConn) SetAggressiveVerifyUpstream(upstream string) *CachedConn {
	cc.aggressiveVerifyUpstreamMu.Lock()
	cc.aggressiveVerifyUpstream = upstream
	cc.aggressiveVerifyUpstreamMu.Unlock()
	return cc
}

// resolveAggressiveVerifyMode reads the effective mode for cc:
// per-CachedConn override > *GoldLapel option > package default
// (Auto). Called once per call site that needs to make a
// scheduling decision.
func (cc *CachedConn) resolveAggressiveVerifyMode() AggressiveVerifyMode {
	if cc.aggressiveVerifyModeSet.Load() {
		return AggressiveVerifyMode(cc.aggressiveVerifyMode.Load())
	}
	lastStartedInstanceMu.Lock()
	inst := lastStartedInstance
	lastStartedInstanceMu.Unlock()
	if inst != nil && inst.aggressiveVerifySet {
		return inst.aggressiveVerify
	}
	return AggressiveVerifyAuto
}

// aggressiveVerifyEnabled reports whether post-DML verify should
// fire for this CachedConn right now. Resolution order:
//
//  1. Explicit per-CachedConn mode (On / Off) — wins.
//  2. License-payload signal (true) — Auto upgrades to On.
//  3. Detection probe — Auto runs the probe (cached per upstream),
//     enables on a positive hit.
//  4. Otherwise off.
//
// The detection probe runs at most once per upstream URL across the
// process; the per-CachedConn `aggressiveVerifyResolved` gate prevents
// the probe from racing the user's hot path more than once on a fresh
// CachedConn. Once resolved, the cached `aggressiveVerifyEnabledFlag`
// is the truth. Re-resolution happens on detection-cache reset.
//
// ctx is forwarded into the probe so the user's call cancellation
// aborts the probe — verify-on-checkout style. We deliberately don't
// span this onto the long-lived verifyCtx because the probe IS a
// pre-flight check, not an async tail; the user expects their query
// to either run or cancel as a unit.
func (cc *CachedConn) aggressiveVerifyEnabled(ctx context.Context) bool {
	mode := cc.resolveAggressiveVerifyMode()
	switch mode {
	case AggressiveVerifyOn:
		return true
	case AggressiveVerifyOff:
		return false
	}
	// Auto mode.
	if licenseAggressiveVerifyActive.Load() {
		return true
	}
	// Detection probe — gated by the per-CachedConn resolved flag so
	// repeated calls on the same connection don't re-probe (the
	// upstream-keyed sync.Map already memoises across CachedConns;
	// this avoids the sync.Map lookup on every Query).
	if cc.aggressiveVerifyResolved.Load() {
		return cc.aggressiveVerifyEnabledFlag.Load()
	}
	cc.aggressiveVerifyResolveOnce.Do(func() {
		cc.aggressiveVerifyUpstreamMu.Lock()
		key := cc.aggressiveVerifyUpstream
		cc.aggressiveVerifyUpstreamMu.Unlock()
		if key == "" {
			// Best-effort: pick up the lastStartedInstance's upstream
			// when the CachedConn wasn't built via Wrap()'s
			// *GoldLapel-aware path.
			lastStartedInstanceMu.Lock()
			if inst := lastStartedInstance; inst != nil {
				key = inst.upstream
			}
			lastStartedInstanceMu.Unlock()
		}
		if key == "" {
			// No upstream identity → can't memoise + we don't want to
			// fire a detection probe per CachedConn against an unknown
			// database. Default off; the user can opt in via explicit
			// AggressiveVerifyOn or by stamping the upstream URL on the
			// CachedConn (SetAggressiveVerifyUpstream). This keeps
			// hand-rolled test fixtures and raw-conn callers from
			// paying a probe round-trip per call.
			cc.aggressiveVerifyEnabledFlag.Store(false)
			cc.aggressiveVerifyResolved.Store(true)
			return
		}
		// realMu serialises against any concurrent user query — the
		// probe runs cc.real.Query, same goroutine-confinement
		// invariant as runVerify.
		cc.realMu.Lock()
		needed := detectAggressiveVerifyNeeded(ctx, cc.real, key)
		cc.realMu.Unlock()
		cc.aggressiveVerifyEnabledFlag.Store(needed)
		cc.aggressiveVerifyResolved.Store(true)
	})
	return cc.aggressiveVerifyEnabledFlag.Load()
}

// scheduleAsyncVerifyForDML is the post-DML hook. Spec parity with
// the post-function-call path (scheduleAsyncVerify) so the
// goroutine-leak protection is identical:
//
//   - Close()-after-schedule races: the closed atomic guards the
//     spawn site.
//   - inFlightVerify gate: a burst of writes coalesces onto a single
//     verify (the second+ schedule attempts no-op, the in-flight
//     verify will pick up any state mutations they would have caused).
//   - verifyCtx / verifyCancel / verifyWG: same long-lived lifecycle
//     as the function-call path.
//
// Pre-condition: cc.aggressiveVerifyEnabled(ctx) returned true. Callers
// MUST also mark the GUC state dirty (cc.ensureGucState().MarkDirty())
// so the next checkout's verify-on-checkout fires synchronously if the
// async path lost the race or hadn't completed yet.
func (cc *CachedConn) scheduleAsyncVerifyForDML() {
	cc.scheduleAsyncVerify()
}
