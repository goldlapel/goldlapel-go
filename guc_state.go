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
type ConnectionGucState struct {
	mu sync.RWMutex
	// Lowercased GUC name → raw value string. Only unsafe GUCs.
	values map[string]string
	// Cached hash of values, recomputed on every mutation. 0 for the
	// empty (default) state, which means a fresh CachedConn's state
	// hash matches "no GUCs set" cache slots from peer connections —
	// exactly what we want for the common case of a wrapper user who
	// never SETs anything unsafe.
	hash uint64
}

// NewConnectionGucState returns a fresh state in the baseline (hash == 0)
// configuration.
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
		if len(s.values) > 0 {
			s.values = make(map[string]string)
			s.recomputeHashLocked()
		}
		s.mu.Unlock()
	case SetCmdSetLocal, SetCmdDiscardOther, SetCmdNone:
		// Intentionally ignored for state-hash purposes.
	}
}

// ObserveSQL parses a SQL string and applies every recognised SET / RESET
// it contains. Multi-statement bodies are split on top-level ';' (string
// literals respected) so a single Q like `SET app.user_id = '42'; SELECT 1`
// still updates state. Returns true if the call mutated the state hash.
func (s *ConnectionGucState) ObserveSQL(sql string) bool {
	before := s.Hash()
	// Fast path: no inner ';' means a single statement; skip the
	// allocation in SplitStatements for the common wire-message case.
	stripped := strings.TrimRight(strings.TrimRight(sql, " \t\r\n"), ";")
	if !strings.ContainsRune(stripped, ';') {
		cmd := ParseSetCommand(sql)
		if cmd.Kind != SetCmdNone {
			s.Apply(cmd)
		}
	} else {
		for _, stmt := range SplitStatements(sql) {
			cmd := ParseSetCommand(stmt)
			if cmd.Kind != SetCmdNone {
				s.Apply(cmd)
			}
		}
	}
	return s.Hash() != before
}

// recomputeHashLocked must be called with s.mu held for write. Iterates
// the values map in sorted key order so the hash is stable across
// permutations of insertion order — the same set of (name, value) pairs
// always produces the same hash, regardless of the sequence in which the
// SETs arrived. Empty state hashes to 0 by definition.
func (s *ConnectionGucState) recomputeHashLocked() {
	if len(s.values) == 0 {
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
	s.hash = h.Sum64()
}
