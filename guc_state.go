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
// results without changing the SQL text. Matched case-insensitively. Any
// GUC with a '.' in the name is also treated as unsafe (namespaced GUCs
// are the canonical custom-RLS pattern).
var unsafeGUCShortList = map[string]bool{
	"search_path":                   true,
	"role":                          true,
	"session_authorization":         true,
	"default_transaction_isolation": true,
	"default_transaction_read_only": true,
	"transaction_isolation":         true,
	"row_security":                  true,
}

// SetCommandKind classifies a parsed SET / RESET. Callers care primarily
// about the (kind, name, value) triple; the kind disambiguates SET from
// SET LOCAL from RESET / RESET ALL.
type SetCommandKind int

const (
	// SetCmdNone marks a SQL string that did not parse as a SET / RESET.
	SetCmdNone SetCommandKind = iota
	// SetCmdSet is `SET name = value` / `SET name TO value` /
	// `SET SESSION name = value`.
	SetCmdSet
	// SetCmdSetLocal is `SET LOCAL name = value`. Tracked but ignored
	// for state-hash purposes.
	SetCmdSetLocal
	// SetCmdReset is `RESET name`.
	SetCmdReset
	// SetCmdResetAll is `RESET ALL`.
	SetCmdResetAll
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

// ParseSetCommand parses a SET / RESET command out of a single SQL
// statement. Returns SetCmdNone when the statement is not a recognised
// SET / RESET shape.
//
// Recognises:
//   - SET name = value, SET name TO value
//   - SET SESSION name = value, SET SESSION name TO value
//   - SET LOCAL name = value, SET LOCAL name TO value
//   - RESET name
//   - RESET ALL
//
// The legacy two-word `SET TIME ZONE 'UTC'` form returns SetCmdNone — the
// timezone GUC is harmless, so misclassifying it as not-a-tracked-SET is
// safe for cache correctness.
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

// Apply applies a parsed SET / RESET. No-op for SetCmdSetLocal (transient
// — cache is bypassed inside transactions anyway); no-op for safe GUC
// names; no-op for SetCmdNone.
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
	case SetCmdSetLocal, SetCmdNone:
		// Intentionally ignored.
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
