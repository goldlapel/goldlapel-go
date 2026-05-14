// Aggressive post-DML cache-key isolation (always-on dml_seq bump).
//
// Background. The Option Y wire-side GUC tracker (see guc_state.go)
// catches every `SET` / `RESET` / `set_config` the wrapper sees in the
// SQL stream. What it CAN'T see is a server-side state change made by a
// TRIGGER body firing on INSERT/UPDATE/DELETE — the wire layer never
// observes the SET because it happens entirely inside Postgres. This is
// the "trigger-internal SET" gap documented at
// `goldlapel/docs/todos/aggressive-verify-flag.md` and called out in
// `docs/todos/guc-rls-cache-safety.md` as a v1 limitation.
//
// Mitigation: after every DML statement (INSERT/UPDATE/DELETE/MERGE/
// TRUNCATE), bump the per-connection `dml_seq` counter. The counter is
// mixed into the connection's state hash, so any subsequent cacheable
// read on this connection lands in a fresh cache slot — a server-side
// SET inside the trigger is irrelevant to correctness because the
// post-DML cache key can't collide with a pre-DML peer's slot. PG
// itself always returns correct rows under the new state; we just keep
// the cache from handing back a stale response.
//
// Cost: zero round-trips per write. No detection probe, no per-upstream
// memo. The bump is a single atomic-mutex-protected counter increment
// (~tens of ns) and the cache-key recompute that follows is already on
// the hot path for every Query / QueryRow.
//
// Mode resolution. The customer should not have to think about this:
//
//  1. WithAggressiveVerify(On) / AggressiveVerifyAuto (default): bump
//     dml_seq after every observed DML. This is the safe-by-default
//     behaviour — trades zero correctness for ~no perf cost.
//  2. WithAggressiveVerify(Off): suppress the bump. The wrapper emits a
//     one-time warning at construction so the operator knows they've
//     opted into the correctness gap; for caller scenarios where the
//     schema provably has no trigger-internal SETs (and the gap is
//     therefore not a real risk), Off avoids the cache-slot fragmentation
//     that would otherwise force per-DML cache misses.

package goldlapel

import (
	"log"
	"strings"
	"sync"
)

// AggressiveVerifyMode controls the post-DML dml_seq bump behaviour of
// every CachedConn associated with this proxy instance. See package
// docs for the always-on rationale.
type AggressiveVerifyMode int

const (
	// AggressiveVerifyAuto is the default. Equivalent to
	// AggressiveVerifyOn — the wrapper always bumps dml_seq after a DML
	// statement so the post-DML cache slot is fresh, closing the
	// trigger-internal-SET correctness gap at zero round-trip cost.
	AggressiveVerifyAuto AggressiveVerifyMode = iota
	// AggressiveVerifyOn forces the post-DML dml_seq bump unconditionally.
	// Identical behaviour to AggressiveVerifyAuto; kept as an explicit
	// option for callers that want to be unambiguous in their config.
	AggressiveVerifyOn
	// AggressiveVerifyOff suppresses the post-DML dml_seq bump. The
	// trigger-internal-SET gap is left open — see
	// docs/todos/guc-rls-cache-safety.md for the consequences. A one-time
	// warning is emitted at the first DML observed on a connection in
	// Off mode so operators don't silently inherit a correctness gap.
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

// aggressiveVerifyOffWarnOnce ensures the "Off mode is leaving a
// correctness gap" warning fires at most once per process, no matter
// how many CachedConns observe an Off-mode DML. The warning carries
// the same information either way; emitting it per-connection would
// just bloat logs.
var aggressiveVerifyOffWarnOnce sync.Once

// warnAggressiveVerifyOff emits the operator-facing warning the first
// time any CachedConn observes a DML under AggressiveVerifyOff. The
// message references the docs so the operator can read the full
// rationale before deciding whether the opt-out fits their schema.
func warnAggressiveVerifyOff() {
	aggressiveVerifyOffWarnOnce.Do(func() {
		log.Printf("goldlapel: WithAggressiveVerify(Off) leaves the trigger-internal-SET cache-correctness gap open. See docs/todos/aggressive-verify-flag.md for context. If your schema has no trigger functions that issue server-side SET / set_config, this opt-out is safe; otherwise prefer the default (auto/on).")
	})
}

// looksLikeDML returns true when sql's first non-whitespace token
// (or any segment's first token, for a multi-statement body) is one
// of the DML verbs that triggers the post-DML dml_seq bump: INSERT,
// UPDATE, DELETE, MERGE, TRUNCATE.
//
// This is intentionally narrower than detectWrite's notion of
// "writes" — DDL (CREATE/ALTER/DROP/REFRESH/DO/CALL) is excluded
// because triggers don't fire on DDL. COPY is also excluded; while
// COPY ... FROM does insert rows, the trigger-internal-SET pattern
// targets row-by-row INSERT/UPDATE/DELETE handlers, and PG fires
// per-row triggers on COPY anyway — so a COPY that drives an INSERT
// trigger would also pass through detectWriteMulti.
//
// Multi-statement bodies: returns true if ANY top-level segment is
// DML, not just the first. A body like `SET app.user_id = '42';
// INSERT INTO t VALUES (1)` does need a post-DML bump.
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
// Default: AggressiveVerifyAuto (equivalent to AggressiveVerifyOn) —
// the wrapper bumps dml_seq after every observed DML, closing the
// trigger-internal-SET correctness gap at zero round-trip cost.
//
// AggressiveVerifyOff opts out — the wrapper emits a one-time warning
// at the first DML the suppression actually affects.
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
// override. When mode is AggressiveVerifyAuto, the wrapper falls back
// to the *GoldLapel-level mode (or, if no instance is registered, to
// the package default — bump on every DML).
//
// Returns the same CachedConn so the call chain reads naturally:
//
//	cc := goldlapel.Wrap(conn, port).SetAggressiveVerify(goldlapel.AggressiveVerifyOn)
func (cc *CachedConn) SetAggressiveVerify(mode AggressiveVerifyMode) *CachedConn {
	cc.aggressiveVerifyMode.Store(int32(mode))
	cc.aggressiveVerifyModeSet.Store(true)
	return cc
}

// resolveAggressiveVerifyMode reads the effective mode for cc:
// per-CachedConn override > *GoldLapel option > package default
// (Auto). Called once per call site that needs to make a scheduling
// decision.
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

// aggressiveVerifyEnabled reports whether the post-DML dml_seq bump
// should fire for this CachedConn. Auto and On both enable it; Off
// disables it (and emits the one-time warning on the first call that
// observes the opt-out).
func (cc *CachedConn) aggressiveVerifyEnabled() bool {
	switch cc.resolveAggressiveVerifyMode() {
	case AggressiveVerifyOff:
		warnAggressiveVerifyOff()
		return false
	default:
		// Auto, On, and any unknown future value default to on — the
		// correctness side is the safer default and the cost is one
		// counter bump per write.
		return true
	}
}
