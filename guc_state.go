// Per-connection GUC state tracking for cache-key safety.
//
// Custom-GUC-driven RLS (e.g. `SET app.user_id = '42'; SELECT * FROM
// accounts;` where the RLS policy reads `current_setting('app.user_id')`)
// can leak user A's results to user B if the wrapper's native cache or the
// proxy cache groups requests purely by SQL+params. This module fingerprints
// the subset of GUC settings that can change query results, so the cache key
// can include the fingerprint and never cross security boundaries.
//
// Mirrors the proxy's src/guc_state.rs (Option Y, locked at
// docs/todos/guc-rls-cache-safety.md):
//
//  1. The wrapper observes every SET / RESET that flows through CachedConn.
//  2. A GUC name is unsafe if it is in a short hardcoded list (search_path,
//     role, isolation, etc.) OR contains a '.' (namespaced — app.*, myapp.*).
//  3. Unsafe GUC values are stored in a sorted map keyed by the lowercased
//     GUC name. The map's hash is the connection's state hash, recomputed
//     on every change.
//  4. The state hash is folded into the wrapper's cache key so two
//     connections with different unsafe GUC state never share a cache slot.
//
// SET LOCAL is intentionally ignored: SET LOCAL only takes effect inside a
// transaction, and CachedConn already bypasses the cache while inTransaction
// is true, so SET LOCAL effects never influence a cacheable response.

package goldlapel

import (
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// unsafeGUCShortList enumerates GUC names whose value can change query
// results — either WHICH rows the server returns, or HOW those rows are
// rendered on the wire. Matched case-insensitively. Any GUC with a '.'
// in the name is also treated as unsafe (namespaced GUCs are the canonical
// custom-RLS pattern).
//
// The list has two flavours:
//
//   - row-set GUCs: search_path / role / *_isolation / row_security —
//     change which rows come back under RLS.
//   - rendering GUCs: DateStyle / IntervalStyle / TimeZone / bytea_output /
//     lc_* — don't change the row set, but change how PG textually renders
//     dates / intervals / numerics / monetary on the wire. Two connections
//     with the same SQL but different DateStyle would otherwise share a
//     cache slot and the second connection would observe the first
//     connection's rendering — a correctness gap, even if not an RLS leak.
//     Cheap to fold into the state hash; covers a real footgun.
var unsafeGUCShortList = map[string]bool{
	// Row-set GUCs (RLS / privilege boundary).
	"search_path":                   true,
	"role":                          true,
	"session_authorization":         true,
	"default_transaction_isolation": true,
	"default_transaction_read_only": true,
	"transaction_isolation":         true,
	"row_security":                  true,
	// Rendering / locale GUCs (wire-format correctness).
	"datestyle":     true,
	"intervalstyle": true,
	"timezone":      true,
	"bytea_output":  true,
	"lc_messages":   true,
	"lc_monetary":   true,
	"lc_numeric":    true,
	"lc_time":       true,
}

// discardSubcommandsOther is the set of `DISCARD <subcommand>` values
// that are NOT equivalent to `RESET ALL`. PG accepts ALL / PLANS /
// SEQUENCES / TEMP / TEMPORARY (the last two are aliases). DISCARD ALL
// clears every session resource (GUCs, prepared statements, sequences,
// temp tables, locks, listens, advisory locks, plan cache); the others
// are scoped subsets that don't touch GUC state. Kept upper-case for
// case-insensitive comparison after Strings.ToUpper.
var discardSubcommandsOther = map[string]bool{
	"PLANS":     true,
	"SEQUENCES": true,
	"TEMP":      true,
	"TEMPORARY": true,
}

// SetCommandKind classifies a parsed SET / RESET / DISCARD. Callers care
// primarily about the (kind, name, value) triple; the kind disambiguates
// the operation.
type SetCommandKind int

const (
	// SetCmdNone marks a SQL string that did not parse as a recognised
	// state-mutation command.
	SetCmdNone SetCommandKind = iota
	// SetCmdSet is `SET name = value` / `SET name TO value` /
	// `SET SESSION name = value`.
	SetCmdSet
	// SetCmdSetLocal is `SET LOCAL name = value`. Tracked but ignored
	// for state-hash purposes.
	SetCmdSetLocal
	// SetCmdReset is `RESET name`.
	SetCmdReset
	// SetCmdResetAll is `RESET ALL` AND `DISCARD ALL` (the latter is a
	// strict superset, but for state-hash purposes both clear every
	// non-default GUC).
	SetCmdResetAll
	// SetCmdDiscardOther is `DISCARD PLANS` / `DISCARD SEQUENCES` /
	// `DISCARD TEMP` / `DISCARD TEMPORARY`. These don't touch GUC state
	// (so the hash map is unchanged), but observing them is useful — a
	// pool that recycles connections with one of these subcommands has
	// implicitly invalidated prepared-statement / temp-table state, and
	// callers that maintain side caches (prepared-statement cache, etc.)
	// can clear those when they observe this kind.
	SetCmdDiscardOther
)

// SetCommand is the parsed shape of a single SET / RESET statement. Name
// is lowercased; Value has surrounding quotes peeled.
type SetCommand struct {
	Kind  SetCommandKind
	Name  string
	Value string
}

// IsUnsafeGUC classifies a GUC name as state-affecting (true) or harmless
// (false). A GUC is unsafe if it's in the short hardcoded list OR contains
// a '.' (namespaced — app.*, myapp.*, etc.). Comparison is case-insensitive.
func IsUnsafeGUC(name string) bool {
	lower := strings.ToLower(name)
	if strings.Contains(lower, ".") {
		return true
	}
	return unsafeGUCShortList[lower]
}

// stripStringLiterals replaces the contents of `'...'` and `"..."` string
// literals with spaces, preserving overall length so byte positions line up
// with the original. PG's doubled-quote `”` / `""` escapes are handled the
// same way as in SplitStatements (both delimiters blanked, scanner stays
// inside the literal). Used by detectWrite's SELECT branch so that bare
// words like `INTO` inside a literal (e.g.
// `SELECT 'INSERT INTO orders' FROM audit_log`) don't trip the SELECT-INTO
// DDL classifier.
func stripStringLiterals(sql string) string {
	if len(sql) == 0 {
		return sql
	}
	out := []byte(sql)
	var quote byte // 0 when not in a quoted literal
	i := 0
	for i < len(sql) {
		c := sql[i]
		if quote != 0 {
			if c == quote {
				if i+1 < len(sql) && sql[i+1] == quote {
					// Doubled-quote escape: blank both, stay inside literal.
					out[i] = ' '
					out[i+1] = ' '
					i += 2
					continue
				}
				// Closing quote: leave the delimiter, drop the literal body.
				quote = 0
			} else {
				out[i] = ' '
			}
		} else {
			if c == '\'' || c == '"' {
				quote = c
			}
		}
		i++
	}
	return string(out)
}

// SplitStatements splits a SQL string on top-level ';' characters,
// respecting single- and double-quoted string literals. Returns each
// segment with surrounding whitespace trimmed; empty segments are dropped.
//
// This is the lightest possible "statement splitter" — it does not
// understand dollar-quoted strings, comments, or any other lexical nuance.
// Good enough for splitting `SET foo = 'a'; SELECT 1`-style multi-statement
// bodies, which is the entire reason it exists.
func SplitStatements(sql string) []string {
	var out []string
	var start int
	var quote byte // 0 when not in a quoted literal
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		if quote != 0 {
			if c == quote {
				// PG escapes a literal quote by doubling it ('' or "").
				if i+1 < len(sql) && sql[i+1] == quote {
					i++
					continue
				}
				quote = 0
			}
			continue
		}
		if c == '\'' || c == '"' {
			quote = c
			continue
		}
		if c == ';' {
			seg := strings.TrimSpace(sql[start:i])
			if seg != "" {
				out = append(out, seg)
			}
			start = i + 1
		}
	}
	tail := strings.TrimSpace(sql[start:])
	if tail != "" {
		out = append(out, tail)
	}
	return out
}

// ParseSetCommand parses a SET / RESET / DISCARD / set_config() command
// out of a single SQL statement. Returns SetCmdNone when the statement
// is not a recognised state-mutation shape.
//
// Recognises:
//   - SET name = value, SET name TO value
//   - SET SESSION name = value, SET SESSION name TO value
//   - SET LOCAL name = value, SET LOCAL name TO value
//   - RESET name
//   - RESET ALL
//   - DISCARD ALL — equivalent to RESET ALL for state-hash purposes
//     (and strictly more — also clears prepared statements, temp tables,
//     advisory locks, listens, etc. — but those don't affect cache
//     correctness directly).
//   - DISCARD PLANS / DISCARD SEQUENCES / DISCARD TEMP / DISCARD TEMPORARY
//     — returned as SetCmdDiscardOther. State-hash unchanged, but useful
//     for callers that maintain side caches (prepared-statement cache,
//     verify-on-checkout dirty flag, etc.).
//   - SELECT [pg_catalog.]set_config(name, value, is_local) — the
//     canonical Supabase / PostgREST RLS-context shape. is_local=false
//     maps to SetCmdSet; is_local=true maps to SetCmdSetLocal. Per PG
//     docs, set_config is the function-form equivalent of SET / SET LOCAL.
//
// The legacy two-word `SET TIME ZONE 'UTC'` form returns SetCmdNone —
// the one-word `timezone` GUC IS now in the unsafe list (rendering
// matters), but the two-word grammar doesn't fit our parser. Returning
// SetCmdNone here is acceptable in practice because the dirty flag +
// verify-on-checkout fallback (see ConnectionGucState) will rebuild
// state from pg_settings on next checkout if the unusual form ever
// lands.
//
// The parser is intentionally narrow: it handles a single statement. For
// multi-statement SQL bodies, use SplitStatements first and call
// ParseSetCommand on each segment.
func ParseSetCommand(sql string) SetCommand {
	none := SetCommand{Kind: SetCmdNone}

	s := strings.TrimSpace(sql)
	s = strings.TrimRight(strings.TrimSuffix(s, ";"), " \t\r\n")
	if s == "" {
		return none
	}

	tokens := strings.Fields(s)
	if len(tokens) == 0 {
		return none
	}

	head := tokens[0]

	// DISCARD branch — `DISCARD ALL` is identical to `RESET ALL` for
	// state-hash purposes; `DISCARD PLANS` / `SEQUENCES` / `TEMP` /
	// `TEMPORARY` are no-op for state-hash but useful for side-cache
	// callers.
	if strings.EqualFold(head, "DISCARD") {
		if len(tokens) != 2 {
			return none
		}
		sub := strings.ToUpper(tokens[1])
		if sub == "ALL" {
			return SetCommand{Kind: SetCmdResetAll}
		}
		if discardSubcommandsOther[sub] {
			return SetCommand{Kind: SetCmdDiscardOther}
		}
		return none
	}

	// SELECT [pg_catalog.]set_config(name, value, is_local) — function
	// form of SET. Delegated to a dedicated parser so the SET path stays
	// scoped to the keyword-prefixed grammar.
	if strings.EqualFold(head, "SELECT") {
		return parseSetConfigCall(s)
	}

	if strings.EqualFold(head, "RESET") {
		if len(tokens) < 2 {
			return none
		}
		// `RESET name` — anything after `name` is unexpected garbage.
		if len(tokens) > 2 {
			return none
		}
		target := tokens[1]
		if strings.EqualFold(target, "ALL") {
			return SetCommand{Kind: SetCmdResetAll}
		}
		name := normalizeGUCName(target)
		if name == "" {
			return none
		}
		return SetCommand{Kind: SetCmdReset, Name: name}
	}

	if !strings.EqualFold(head, "SET") {
		return none
	}

	if len(tokens) < 2 {
		return none
	}

	idx := 1
	isLocal := false
	switch {
	case strings.EqualFold(tokens[idx], "LOCAL"):
		isLocal = true
		idx++
	case strings.EqualFold(tokens[idx], "SESSION"):
		idx++
	}
	if idx >= len(tokens) {
		return none
	}

	// `next` is the GUC name token, but it may have an `=` glued onto it
	// (e.g. SET app.user='42').
	next := tokens[idx]
	idx++
	var nameToken, gluedValue string
	hasGluedValue := false
	if eq := strings.IndexByte(next, '='); eq >= 0 {
		nameToken = next[:eq]
		v := next[eq+1:]
		if v != "" {
			gluedValue = v
			hasGluedValue = true
		}
	} else {
		nameToken = next
	}

	name := normalizeGUCName(nameToken)
	if name == "" {
		return none
	}

	var valueStr string
	if hasGluedValue {
		// SET name=value [more...]
		if idx < len(tokens) {
			valueStr = gluedValue + " " + strings.Join(tokens[idx:], " ")
		} else {
			valueStr = gluedValue
		}
	} else {
		// Need an `=` or `TO` separator next, then at least one value token.
		if idx >= len(tokens) {
			return none
		}
		sep := tokens[idx]
		idx++
		if !(sep == "=" || strings.EqualFold(sep, "TO")) {
			return none
		}
		if idx >= len(tokens) {
			return none
		}
		valueStr = strings.Join(tokens[idx:], " ")
	}

	value := stripValueQuotes(strings.TrimSpace(valueStr))
	if value == "" && strings.TrimSpace(valueStr) == "" {
		return none
	}

	if isLocal {
		return SetCommand{Kind: SetCmdSetLocal, Name: name, Value: value}
	}
	return SetCommand{Kind: SetCmdSet, Name: name, Value: value}
}

// parseSetConfigCall parses a single SQL statement of the shape
//
//	SELECT [pg_catalog.]set_config('name', 'value', is_local)
//
// where is_local is a boolean literal (`true` / `false` / `'t'` / `'f'`
// / `0` / `1`). Returns SetCmdSet on is_local=false, SetCmdSetLocal on
// is_local=true. Anything that doesn't match is SetCmdNone.
//
// PG's set_config(name TEXT, new_value TEXT, is_local BOOLEAN) is
// documented as the function-form equivalent of SET / SET LOCAL — the
// canonical Supabase / PostgREST RLS-context shape uses it because a
// single Q-message can chain `SELECT set_config(...); SELECT ... FROM
// accounts` without two round-trips. Recognising it here is what closes
// the v1 known-limitation called out in
// docs/todos/guc-rls-cache-safety.md.
//
// The parser is intentionally narrow: it only matches the plain
// 3-argument form. set_config wrapped in another expression
// (`SELECT col, set_config(...) FROM t`), used as a join target, etc.
// returns SetCmdNone — those are exotic enough that the verify-on-
// checkout dirty path is a better safety net than a brittle parser.
func parseSetConfigCall(stmt string) SetCommand {
	none := SetCommand{Kind: SetCmdNone}

	// Strip the leading SELECT keyword (already verified by the caller).
	rest := strings.TrimSpace(stmt)
	if len(rest) < 6 || !strings.EqualFold(rest[:6], "SELECT") {
		return none
	}
	rest = strings.TrimSpace(rest[6:])

	// Optional pg_catalog. schema qualifier.
	if len(rest) >= 11 && strings.EqualFold(rest[:11], "pg_catalog.") {
		rest = strings.TrimSpace(rest[11:])
	}

	// Match the literal `set_config(` prefix (case-insensitive). Anything
	// before the open paren that isn't the bare function name is rejected.
	openParen := strings.IndexByte(rest, '(')
	if openParen < 0 {
		return none
	}
	fnName := strings.TrimSpace(rest[:openParen])
	if !strings.EqualFold(fnName, "set_config") {
		return none
	}

	// Locate the matching close paren. Walk the body honouring single-
	// and double-quoted literals so a `)` inside a quoted value doesn't
	// terminate the call early.
	body := rest[openParen+1:]
	closeParen := findMatchingParen(body)
	if closeParen < 0 {
		return none
	}
	args := body[:closeParen]
	tail := strings.TrimSpace(body[closeParen+1:])
	// Allow an optional trailing semicolon. Anything else after the `)`
	// is a more complex SELECT — we don't try to parse it.
	if tail != "" && tail != ";" {
		return none
	}

	// Split args on top-level commas (respecting quoted literals). Expect
	// exactly three.
	parts := splitTopLevelCommas(args)
	if len(parts) != 3 {
		return none
	}

	rawName := strings.TrimSpace(parts[0])
	rawValue := strings.TrimSpace(parts[1])
	rawIsLocal := strings.TrimSpace(parts[2])

	// The first argument must be a string literal — set_config requires
	// a literal name; passing a column reference is legal SQL but not
	// the canonical RLS shape, and would force us to cope with arbitrary
	// expressions. Bail out conservatively.
	if !looksLikeStringLiteral(rawName) {
		return none
	}
	name := stripValueQuotes(rawName)
	name = strings.ToLower(name)
	if name == "" {
		return none
	}

	value := stripValueQuotes(rawValue)
	// is_local must be parseable as a boolean. PG accepts the standard
	// boolean literal forms (true/false, t/f, yes/no, on/off, 1/0); we
	// accept the same set, case-insensitive, optionally quoted.
	isLocal, ok := parseBoolLiteral(rawIsLocal)
	if !ok {
		return none
	}

	if isLocal {
		return SetCommand{Kind: SetCmdSetLocal, Name: name, Value: value}
	}
	return SetCommand{Kind: SetCmdSet, Name: name, Value: value}
}

// findMatchingParen returns the byte index of the close paren that
// matches an implicit open paren at index -1 of `s`. Honours single-
// and double-quoted literals (PG's doubled-quote escape included) so
// that `')'` inside a string literal doesn't end the scan early.
// Returns -1 if no balanced close paren is found.
func findMatchingParen(s string) int {
	depth := 0
	var quote byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == quote {
				if i+1 < len(s) && s[i+1] == quote {
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
		case '(':
			depth++
		case ')':
			if depth == 0 {
				return i
			}
			depth--
		}
	}
	return -1
}

// splitTopLevelCommas splits `s` on commas that are NOT inside a quoted
// literal or a nested paren group. Returns the segments trimmed of
// surrounding whitespace.
func splitTopLevelCommas(s string) []string {
	var out []string
	depth := 0
	var quote byte
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == quote {
				if i+1 < len(s) && s[i+1] == quote {
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
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				out = append(out, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	out = append(out, strings.TrimSpace(s[start:]))
	return out
}

// looksLikeStringLiteral reports whether `s` is wrapped in matching
// single or double quotes. Used by parseSetConfigCall to reject calls
// where the name argument is not a literal (we don't want to extract
// state from `set_config(some_col, ...)`).
func looksLikeStringLiteral(s string) bool {
	if len(s) < 2 {
		return false
	}
	first := s[0]
	last := s[len(s)-1]
	return (first == '\'' && last == '\'') || (first == '"' && last == '"')
}

// parseBoolLiteral parses one of the boolean-literal forms PG accepts
// for a BOOLEAN argument: true / false / t / f / yes / no / on / off /
// 1 / 0. Quoting is allowed (set_config takes its third argument as a
// boolean, and clients commonly pass `'false'` / `'t'`). Returns the
// boolean value and a recognised flag.
func parseBoolLiteral(raw string) (bool, bool) {
	v := strings.ToLower(stripValueQuotes(raw))
	switch v {
	case "true", "t", "yes", "y", "on", "1":
		return true, true
	case "false", "f", "no", "n", "off", "0":
		return false, true
	}
	return false, false
}

// normalizeGUCName lowercases the GUC name and strips surrounding double
// quotes (PG treats `"app.user_id"` and `app.user_id` as the same
// configuration parameter; the quoted form just preserves case, which we
// discard).
func normalizeGUCName(token string) string {
	trimmed := strings.Trim(token, `"`)
	if trimmed == "" {
		return ""
	}
	return strings.ToLower(trimmed)
}

// stripValueQuotes peels a single layer of matching surrounding quotes
// (`'...'` or `"..."`) from a value. Multi-token quoted values arrive as
// the joined string already; this just removes the outer wrapper.
// Unquoted values are returned trimmed.
func stripValueQuotes(value string) string {
	v := strings.TrimSpace(value)
	if len(v) >= 2 {
		first := v[0]
		last := v[len(v)-1]
		if (first == '\'' && last == '\'') || (first == '"' && last == '"') {
			return v[1 : len(v)-1]
		}
	}
	return v
}

// ConnectionGucState is the per-connection GUC fingerprint. Stores values
// for unsafe GUCs only; harmless GUCs (timezone, application_name, planner
// cost knobs, etc.) never enter the map and never affect the hash.
//
// All public methods are safe for concurrent use. In the wrapper's
// architecture a single CachedConn binds to a single Querier (typically
// pgx.Conn or a single pgxpool acquisition), so contention is bounded —
// the mutex guards against the rare case where the same CachedConn is
// shared across goroutines without external coordination.
//
// The `dirty` flag is a separate atomic axis from the GUC value map: it
// signals "the wire layer might have missed a state mutation" (e.g. a
// stored function did `SET app.user_id` server-side, or a top-level
// `SELECT my_func()` may have done so). Callers can MarkDirty when they
// see a SQL shape that could have moved server state without us
// observing it on the wire, and MarkClean (or Reseed) when they
// confirm state via pg_settings or after a DISCARD ALL. Wrapper layers
// consult IsDirty() to decide whether to fire a verify-on-checkout
// query.
type ConnectionGucState struct {
	mu sync.RWMutex
	// Lowercased GUC name → raw value string. Only unsafe GUCs.
	values map[string]string
	// Cached hash of values mixed with dmlSeq, recomputed on every
	// mutation. 0 for the empty (default) state — a fresh CachedConn's
	// state hash matches "no GUCs set" cache slots from peer connections,
	// which is exactly what we want for the common case of a wrapper
	// user who never SETs anything unsafe.
	hash uint64
	// dmlSeq is a monotonic counter bumped by BumpDmlSeq after every
	// observed INSERT / UPDATE / DELETE / MERGE / TRUNCATE when the
	// aggressive-verify mode is on (Auto or On). Mixed into the state
	// hash so each post-DML cache lookup lands in a fresh slot —
	// preventing a cached pre-DML response from being served to this
	// connection if a server-side trigger SET-mutated the connection's
	// session GUCs (the trigger-internal-SET correctness gap documented
	// at docs/todos/aggressive-verify-flag.md). Reset to 0 whenever the
	// rest of the state is wiped (RESET ALL / DISCARD ALL / Reseed) so a
	// recycled connection re-converges to a peer-shareable baseline.
	dmlSeq uint64
	// dirty signals "wire-layer observation may be stale". Set when a
	// top-level SELECT <function>(...) lands (the function may have
	// done a server-side SET we didn't see); cleared by MarkClean (run
	// after a successful verify) or by an observed DISCARD / RESET ALL.
	// While dirty, the wrapper's read path bypasses L1 — every read
	// routes to the proxy until an authoritative refresh (Reseed) or
	// session-clearing command resolves the uncertainty. atomic.Bool so
	// IsDirty() / MarkDirty() / ClearDirty() never have to take s.mu —
	// they sit on a hot path (every wrapper call inspects the flag) and
	// we don't want to contend with concurrent state-hash readers.
	dirty atomic.Bool
}

// NewConnectionGucState returns a fresh state in the baseline (hash == 0,
// dirty == false) configuration.
func NewConnectionGucState() *ConnectionGucState {
	return &ConnectionGucState{values: make(map[string]string)}
}

// Hash returns the current state hash. 0 indicates the baseline (no
// unsafe GUCs set).
func (s *ConnectionGucState) Hash() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hash
}

// IsDirty reports whether the wire-layer fingerprint is suspected of
// being stale. See the type doc for when callers MarkDirty.
func (s *ConnectionGucState) IsDirty() bool {
	return s.dirty.Load()
}

// MarkDirty signals "wire-layer observation may be stale" — typically
// set after observing a top-level SELECT <function>(...) where the
// function body could have issued a server-side SET we don't see on
// the wire. The wrapper's verify path runs `SELECT name, setting FROM
// pg_settings WHERE source='session'` to reconstruct the truth and
// then calls MarkClean / Reseed.
func (s *ConnectionGucState) MarkDirty() {
	s.dirty.Store(true)
}

// MarkClean clears the dirty flag. Intended for callers that have
// just rebuilt state from authoritative server-side data (verify) or
// observed a session-clearing command (DISCARD ALL / RESET ALL).
func (s *ConnectionGucState) MarkClean() {
	s.dirty.Store(false)
}

// BumpDmlSeq advances the post-DML sequence counter so the next
// cache-key computation on this connection produces a fresh slot.
// Called from the wrapper's read path after every observed INSERT /
// UPDATE / DELETE / MERGE / TRUNCATE when aggressive-verify mode is on
// (Auto / On). The bump means: any subsequent cacheable read on this
// connection cannot share a cache slot with a pre-DML read from this
// same connection — closing the trigger-internal-SET correctness gap
// (a server-side trigger that did `SET app.user_id = ...` would
// otherwise be invisible to the wire-side state observer).
//
// This is the v1 mitigation: cache-key isolation, not actual
// observation of the new GUC values. A trigger that mutated state
// produces correct results from PG itself (PG always knows its own
// session state); the wrapper just guarantees the cache can't hand
// back a stale response keyed on the previous state.
//
// Wraparound at uint64 max is well-defined arithmetic and not a
// concern in practice — billions of DMLs per connection still produce
// unique slots in the wraparound window.
func (s *ConnectionGucState) BumpDmlSeq() {
	s.mu.Lock()
	s.dmlSeq++
	s.recomputeHashLocked()
	s.mu.Unlock()
}

// DmlSeq returns the current post-DML sequence counter. Exposed for
// tests; production callers don't need to introspect.
func (s *ConnectionGucState) DmlSeq() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dmlSeq
}

// Reseed replaces the entire unsafe-GUC value set in one critical
// section. Used by the verify path: the verify query yields a fresh
// authoritative snapshot of `pg_settings WHERE source='session'`, the
// caller passes the (name, value) pairs as a map, and Reseed swaps the
// whole map after filtering to unsafe-only names. Returns the new
// hash. Always clears the dirty flag — Reseed represents an
// authoritative refresh — and resets dmlSeq to 0 (authoritative state
// supersedes any post-DML uncertainty).
func (s *ConnectionGucState) Reseed(values map[string]string) uint64 {
	s.mu.Lock()
	if values == nil {
		s.values = make(map[string]string)
	} else {
		next := make(map[string]string, len(values))
		for k, v := range values {
			lk := strings.ToLower(k)
			if !IsUnsafeGUC(lk) {
				continue
			}
			next[lk] = v
		}
		s.values = next
	}
	s.dmlSeq = 0
	s.recomputeHashLocked()
	h := s.hash
	s.mu.Unlock()
	s.dirty.Store(false)
	return h
}

// Apply applies a parsed SET / RESET / DISCARD. No-op for SetCmdSetLocal
// (transient — cache is bypassed inside transactions anyway); no-op
// for safe GUC names; no-op for SetCmdNone. SetCmdDiscardOther
// (DISCARD PLANS / SEQUENCES / TEMP / TEMPORARY) is also a no-op for
// the state-hash map — those subcommands don't touch GUCs — but
// callers that wrap ConnectionGucState may use the kind for side-effects
// (clearing a prepared-statement cache, etc.).
func (s *ConnectionGucState) Apply(cmd SetCommand) {
	switch cmd.Kind {
	case SetCmdSet:
		if !IsUnsafeGUC(cmd.Name) {
			return
		}
		s.mu.Lock()
		s.values[cmd.Name] = cmd.Value
		s.recomputeHashLocked()
		s.mu.Unlock()
	case SetCmdReset:
		if !IsUnsafeGUC(cmd.Name) {
			return
		}
		s.mu.Lock()
		if _, ok := s.values[cmd.Name]; ok {
			delete(s.values, cmd.Name)
			s.recomputeHashLocked()
		}
		s.mu.Unlock()
	case SetCmdResetAll:
		s.mu.Lock()
		// RESET ALL / DISCARD ALL drops every non-default GUC AND
		// resets the post-DML sequence — a recycled connection
		// returns to the peer-shareable baseline so its cache slots
		// can be hit by any other connection.
		if len(s.values) > 0 || s.dmlSeq != 0 {
			s.values = make(map[string]string)
			s.dmlSeq = 0
			s.recomputeHashLocked()
		}
		s.mu.Unlock()
		// RESET ALL / DISCARD ALL is authoritative: the server has
		// dropped every non-default GUC, so any prior dirty suspicion
		// is moot.
		s.dirty.Store(false)
	case SetCmdDiscardOther:
		// PLANS / SEQUENCES / TEMP / TEMPORARY don't touch GUCs, but
		// PG will only execute them outside a transaction. Observing
		// one means the connection has been recycled by its pool —
		// any uncertainty about server-side SETs is, in practice,
		// resolved by the recycle.
		s.dirty.Store(false)
	case SetCmdSetLocal, SetCmdNone:
		// Intentionally ignored.
	}
}

// ObserveSQL parses a SQL string and applies every recognised SET / RESET
// it contains. Multi-statement bodies are split on top-level ';' (string
// literals respected) so a single Q like `SET app.user_id = '42'; SELECT 1`
// still updates state. Returns true if the call mutated the state hash.
//
// ObserveSQL is the eager / unconditional form — it commits state hash
// changes regardless of whether the underlying Querier later succeeds.
// Callers that need to defer mutation until they observe Querier success
// (the SET-actually-applied protocol) should use PendingObservation
// instead, which separates parse-time from commit-time.
func (s *ConnectionGucState) ObserveSQL(sql string) bool {
	before := s.Hash()
	for _, cmd := range parseSetCommands(sql) {
		s.Apply(cmd)
	}
	return s.Hash() != before
}

// parseSetCommands extracts every state-mutation command from a SQL
// string. Returns nil for SQL with no recognised SETs. Used by
// ObserveSQL and by Pending() so both share a single parse pass.
//
// Fast path: bodies with no inner ';' skip SplitStatements and call
// ParseSetCommand directly on the trimmed body. Bodies with at least
// one inner ';' walk every segment.
func parseSetCommands(sql string) []SetCommand {
	stripped := strings.TrimRight(strings.TrimRight(sql, " \t\r\n"), ";")
	if !strings.ContainsRune(stripped, ';') {
		cmd := ParseSetCommand(sql)
		if cmd.Kind == SetCmdNone {
			return nil
		}
		return []SetCommand{cmd}
	}
	var out []SetCommand
	for _, stmt := range SplitStatements(sql) {
		cmd := ParseSetCommand(stmt)
		if cmd.Kind != SetCmdNone {
			out = append(out, cmd)
		}
	}
	return out
}

// PendingObservation captures the state-mutation effect of a SQL string
// that has been parsed but NOT yet committed to the connection's GUC
// state hash. The caller commits via Apply() (Querier reported success)
// or rolls back via DiscardAfterError() (Querier reported an error).
//
// The motivation is the SET-actually-applied protocol: if the wrapper
// observed `SET app.user_id = '42'` and eagerly mutated the state hash
// before dispatch, but pgx/database/sql then returned an error from
// Exec, the wrapper's hash would diverge from the server's actual GUC
// state — every subsequent cache lookup would land in a "phantom"
// cache slot that the server's session never reaches. Worse, an
// errored RESET would drop the wrapper's record of an unsafe GUC the
// server still has set — a correctness gap (next read serves baseline
// rows under non-baseline server state, an RLS-shaped leak).
//
// PendingObservation moves mutation behind a commit barrier so the
// wrapper's hash only changes after the wire layer confirms the
// statement landed. On error, the wrapper drops the pending mutation
// AND marks the connection dirty — the next checkout's verify-against-
// pg_settings reconciles authoritatively, which is the right safety net
// for the multi-statement edge case (`SET ...; SELECT ...` where the
// SET succeeded server-side but the SELECT failed: we can't tell from
// pgx's single error which segments landed, so we punt to verify).
//
// The struct is not goroutine-safe by design: it is created, settled,
// and discarded entirely within a single Query/QueryRow/Exec call on
// the same goroutine. State is shared via the `state` pointer; the
// state's own methods are goroutine-safe.
type PendingObservation struct {
	state *ConnectionGucState
	cmds  []SetCommand
}

// Pending parses sql for state-mutation commands but does NOT apply
// them. The returned PendingObservation must be settled via either
// Apply() or DiscardAfterError() before the call returns; failing to
// settle is a wrapper bug (the parsed commands silently disappear).
//
// Pending is cheap to call on SQL with no SET / RESET / DISCARD —
// returns a zero-cmd PendingObservation that no-ops on settle. This
// keeps the per-call overhead in line with the bare ObserveSQL fast
// path.
func (s *ConnectionGucState) Pending(sql string) *PendingObservation {
	return &PendingObservation{state: s, cmds: parseSetCommands(sql)}
}

// HasMutation reports whether any state-mutation command was parsed
// out of the originating SQL. Used by callers (wrap.go) to decide
// whether DiscardAfterError should mark the connection dirty: if no
// SETs were parsed, there's nothing to roll back, and the dirty mark
// would just trigger an unnecessary verify on the next checkout.
func (p *PendingObservation) HasMutation() bool {
	return len(p.cmds) > 0
}

// Apply commits every parsed command to the underlying state. Called
// by the wrapper after the underlying Querier reports nil error. The
// state hash moves to reflect every recognised SET / RESET / DISCARD;
// SET LOCAL and SetCmdNone are filtered out at parse time so the
// commit list is already clean. Returns true if the apply changed the
// hash.
func (p *PendingObservation) Apply() bool {
	if p == nil || p.state == nil || len(p.cmds) == 0 {
		return false
	}
	before := p.state.Hash()
	for _, cmd := range p.cmds {
		p.state.Apply(cmd)
	}
	return p.state.Hash() != before
}

// DiscardAfterError drops the parsed commands without applying them
// AND marks the underlying state dirty (when at least one mutation was
// observed) so the next checkout runs verify-against-pg_settings.
//
// The dirty mark is mandatory — even on what looks like a single-
// statement error, pgx/database/sql may have batched the SQL across
// multiple PG protocol messages, and a partial-apply at the server
// would leave the wrapper's hash in a stale state that only verify
// can repair. The cost is one verify on the unhappy path, which is
// the right trade for cache-correctness.
//
// SetCmdNone-only Pending observations (no recognised mutation)
// short-circuit: discarding nothing requires no dirty mark.
func (p *PendingObservation) DiscardAfterError() {
	if p == nil || p.state == nil || len(p.cmds) == 0 {
		return
	}
	p.state.MarkDirty()
}

// IsTopLevelFunctionCall reports whether `sql` is a top-level
// `SELECT <ident>(...)` shape — i.e. the canonical "call a function and
// return its result" pattern. Used by the verify path to decide when
// to schedule an async post-call verify: if the function body did
// `SET app.user_id = ...` server-side, the wire layer never saw the
// SET, so we mark the connection dirty and re-read pg_settings.
//
// Recognised shapes:
//   - SELECT my_func()
//   - SELECT my_func(arg, ...)
//   - SELECT schema.func(...)
//
// Excluded shapes (return false):
//   - SELECT col, my_func() FROM t              (column projection)
//   - SELECT my_func() FROM t                   (FROM clause)
//   - SELECT my_func() WHERE ...                (WHERE clause)
//   - SELECT 1                                  (no function call)
//   - Anything not starting with SELECT
//
// Conservative — only matches the bare-function form that's the
// canonical "call this stored procedure" pattern. False negatives
// (real function calls that don't match) just skip the async verify;
// the dirty flag will eventually get re-evaluated on the next
// observed SET / RESET / verify trigger.
func IsTopLevelFunctionCall(sql string) bool {
	s := strings.TrimSpace(sql)
	s = strings.TrimRight(strings.TrimSuffix(s, ";"), " \t\r\n")
	if s == "" {
		return false
	}
	if !strings.HasPrefix(strings.ToUpper(s), "SELECT") {
		return false
	}
	// Must be SELECT followed by whitespace, not e.g. SELECTOR-something.
	if len(s) < 7 || (s[6] != ' ' && s[6] != '\t' && s[6] != '\n' && s[6] != '\r') {
		return false
	}
	rest := strings.TrimSpace(s[6:])
	if rest == "" {
		return false
	}

	// Find the first '(' — that's the function-arguments delimiter.
	openParen := strings.IndexByte(rest, '(')
	if openParen < 0 {
		return false
	}

	// The text before '(' must be a single qualified identifier:
	// `name` or `schema.name` (optionally `cat.schema.name`). No
	// commas, spaces, or quotes mid-identifier — those signal a
	// projection list (`col, fn(`) or a more complex expression.
	identPart := strings.TrimSpace(rest[:openParen])
	if identPart == "" {
		return false
	}
	for _, c := range identPart {
		// Letters / digits / underscores / dots only. Reject
		// anything else (commas, spaces, parens, operators, quotes).
		if !(c == '_' || c == '.' ||
			(c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9')) {
			return false
		}
	}

	// The argument list must close with a balanced ')' AND nothing
	// of substance can follow (other than whitespace / a trailing
	// semicolon). A trailing FROM / WHERE / etc. means the function
	// call is inside a larger query, not the whole statement.
	body := rest[openParen+1:]
	closeParen := findMatchingParen(body)
	if closeParen < 0 {
		return false
	}
	tail := strings.TrimSpace(body[closeParen+1:])
	if tail != "" && tail != ";" {
		return false
	}
	return true
}

// recomputeHashLocked must be called with s.mu held for write. Iterates
// the values map in sorted key order so the hash is stable across
// permutations of insertion order — the same set of (name, value) pairs
// always produces the same hash, regardless of the sequence in which the
// SETs arrived. Empty values + zero dmlSeq is the canonical "fresh
// connection" baseline and hashes to 0 by definition — keep the hash
// exactly 0 so cache-slot-sharing semantics hold for the common case (no
// unsafe SETs, no DMLs yet).
func (s *ConnectionGucState) recomputeHashLocked() {
	if len(s.values) == 0 && s.dmlSeq == 0 {
		s.hash = 0
		return
	}
	keys := make([]string, 0, len(s.values))
	for k := range s.values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := fnv.New64a()
	for _, k := range keys {
		// Length-prefix-and-separator each field so the hash distinguishes
		// adjacency (e.g. {a:bc} vs {ab:c}) — without separators those
		// would feed identical bytes into the hasher.
		h.Write([]byte(k))
		h.Write([]byte{0x00})
		h.Write([]byte(s.values[k]))
		h.Write([]byte{0x00})
	}
	// Mix dmlSeq AFTER the value bytes with its own separator so a
	// connection with no SETs but a non-zero dmlSeq still produces a
	// unique hash. Length-prefixed by the separator above for the same
	// adjacency-distinction reason.
	var seqBuf [8]byte
	for i := 0; i < 8; i++ {
		seqBuf[i] = byte(s.dmlSeq >> (8 * i))
	}
	h.Write(seqBuf[:])
	s.hash = h.Sum64()
}
