package goldlapel

import (
	"sync"
	"testing"
)

// --- IsUnsafeGUC ---

func TestIsUnsafeGUC_ShortListMembers(t *testing.T) {
	for _, name := range []string{
		"search_path",
		"role",
		"session_authorization",
		"default_transaction_isolation",
		"default_transaction_read_only",
		"transaction_isolation",
		"row_security",
	} {
		if !IsUnsafeGUC(name) {
			t.Errorf("expected %q to be unsafe", name)
		}
	}
}

func TestIsUnsafeGUC_CaseInsensitive(t *testing.T) {
	for _, name := range []string{"ROLE", "Search_Path", "SEARCH_PATH"} {
		if !IsUnsafeGUC(name) {
			t.Errorf("expected %q to be unsafe (case-insensitive)", name)
		}
	}
}

func TestIsUnsafeGUC_NamespacedAreUnsafe(t *testing.T) {
	for _, name := range []string{
		"app.user_id",
		"myapp.tenant",
		"rls.account",
		"a.b.c",
		"APP.USER",
	} {
		if !IsUnsafeGUC(name) {
			t.Errorf("expected namespaced %q to be unsafe", name)
		}
	}
}

func TestIsUnsafeGUC_SafeGUCsAreSafe(t *testing.T) {
	for _, name := range []string{
		"application_name",
		"statement_timeout",
		"work_mem",
		"client_encoding",
		"random_page_cost",
		"effective_cache_size",
		"jit",
	} {
		if IsUnsafeGUC(name) {
			t.Errorf("expected %q to be safe", name)
		}
	}
}

// TestIsUnsafeGUC_RenderingGUCsAreUnsafe locks in the rendering / locale
// GUCs as unsafe. These don't change which rows the server returns, but
// they DO change how those rows are textually rendered — so two
// connections sharing a cache slot under different rendering settings
// would observe each other's rendering. See the unsafeGUCShortList
// docstring for the full rationale.
func TestIsUnsafeGUC_RenderingGUCsAreUnsafe(t *testing.T) {
	for _, name := range []string{
		"DateStyle",
		"datestyle",
		"IntervalStyle",
		"intervalstyle",
		"TimeZone",
		"timezone",
		"bytea_output",
		"BYTEA_OUTPUT",
		"lc_messages",
		"lc_monetary",
		"lc_numeric",
		"lc_time",
	} {
		if !IsUnsafeGUC(name) {
			t.Errorf("expected rendering GUC %q to be unsafe", name)
		}
	}
}

// --- ParseSetCommand: shapes ---

func TestParseSetCommand_EqQuoted(t *testing.T) {
	cmd := ParseSetCommand("SET foo = 'bar'")
	if cmd.Kind != SetCmdSet || cmd.Name != "foo" || cmd.Value != "bar" {
		t.Fatalf("got %+v", cmd)
	}
}

func TestParseSetCommand_ToQuoted(t *testing.T) {
	cmd := ParseSetCommand("SET foo TO 'bar'")
	if cmd.Kind != SetCmdSet || cmd.Name != "foo" || cmd.Value != "bar" {
		t.Fatalf("got %+v", cmd)
	}
}

func TestParseSetCommand_Unquoted(t *testing.T) {
	cmd := ParseSetCommand("SET foo = 42")
	if cmd.Kind != SetCmdSet || cmd.Name != "foo" || cmd.Value != "42" {
		t.Fatalf("got %+v", cmd)
	}
}

func TestParseSetCommand_SessionModifier(t *testing.T) {
	cmd := ParseSetCommand("SET SESSION foo = 'bar'")
	if cmd.Kind != SetCmdSet || cmd.Name != "foo" || cmd.Value != "bar" {
		t.Fatalf("got %+v", cmd)
	}
}

func TestParseSetCommand_LocalModifier(t *testing.T) {
	cmd := ParseSetCommand("SET LOCAL foo = 'bar'")
	if cmd.Kind != SetCmdSetLocal || cmd.Name != "foo" || cmd.Value != "bar" {
		t.Fatalf("got %+v", cmd)
	}
}

func TestParseSetCommand_ResetNamed(t *testing.T) {
	cmd := ParseSetCommand("RESET foo")
	if cmd.Kind != SetCmdReset || cmd.Name != "foo" {
		t.Fatalf("got %+v", cmd)
	}
}

func TestParseSetCommand_ResetAll(t *testing.T) {
	cmd := ParseSetCommand("RESET ALL")
	if cmd.Kind != SetCmdResetAll {
		t.Fatalf("got %+v", cmd)
	}
}

// --- ParseSetCommand: case + whitespace + semicolon ---

func TestParseSetCommand_CaseInsensitiveKeywords(t *testing.T) {
	cmd := ParseSetCommand("set foo = 'bar'")
	if cmd.Kind != SetCmdSet || cmd.Value != "bar" {
		t.Fatalf("lowercase set: got %+v", cmd)
	}
	cmd = ParseSetCommand("Set Local foo To 'bar'")
	if cmd.Kind != SetCmdSetLocal || cmd.Value != "bar" {
		t.Fatalf("mixed-case Set Local ... To: got %+v", cmd)
	}
	cmd = ParseSetCommand("reset all")
	if cmd.Kind != SetCmdResetAll {
		t.Fatalf("lowercase reset all: got %+v", cmd)
	}
}

func TestParseSetCommand_LowercasesGUCName(t *testing.T) {
	cmd := ParseSetCommand("SET App.User_ID = '42'")
	if cmd.Kind != SetCmdSet || cmd.Name != "app.user_id" || cmd.Value != "42" {
		t.Fatalf("got %+v", cmd)
	}
}

func TestParseSetCommand_TrailingSemicolon(t *testing.T) {
	cmd := ParseSetCommand("SET foo = 'bar';")
	if cmd.Kind != SetCmdSet || cmd.Value != "bar" {
		t.Fatalf("got %+v", cmd)
	}
	cmd = ParseSetCommand("RESET foo ;")
	if cmd.Kind != SetCmdReset || cmd.Name != "foo" {
		t.Fatalf("got %+v", cmd)
	}
}

func TestParseSetCommand_ExtraWhitespace(t *testing.T) {
	cmd := ParseSetCommand("   SET    foo   =   'bar'   ")
	if cmd.Kind != SetCmdSet || cmd.Value != "bar" {
		t.Fatalf("got %+v", cmd)
	}
}

func TestParseSetCommand_GluedEquals(t *testing.T) {
	cmd := ParseSetCommand("SET app.user_id='42'")
	if cmd.Kind != SetCmdSet || cmd.Name != "app.user_id" || cmd.Value != "42" {
		t.Fatalf("got %+v", cmd)
	}
}

func TestParseSetCommand_DoubleQuotedValue(t *testing.T) {
	cmd := ParseSetCommand(`SET foo = "bar"`)
	if cmd.Kind != SetCmdSet || cmd.Value != "bar" {
		t.Fatalf("got %+v", cmd)
	}
}

func TestParseSetCommand_DoubleQuotedName(t *testing.T) {
	cmd := ParseSetCommand(`SET "app.user_id" = '42'`)
	if cmd.Kind != SetCmdSet || cmd.Name != "app.user_id" || cmd.Value != "42" {
		t.Fatalf("got %+v", cmd)
	}
}

// --- ParseSetCommand: rejects ---

func TestParseSetCommand_RejectsNonSetStatements(t *testing.T) {
	for _, sql := range []string{"SELECT 1", "BEGIN", "UPDATE t SET x = 1"} {
		if cmd := ParseSetCommand(sql); cmd.Kind != SetCmdNone {
			t.Errorf("%q should not parse: got %+v", sql, cmd)
		}
	}
}

func TestParseSetCommand_RejectsEmpty(t *testing.T) {
	for _, sql := range []string{"", "   ", ";"} {
		if cmd := ParseSetCommand(sql); cmd.Kind != SetCmdNone {
			t.Errorf("%q should not parse: got %+v", sql, cmd)
		}
	}
}

func TestParseSetCommand_RejectsSetWithoutValue(t *testing.T) {
	for _, sql := range []string{"SET foo =", "SET foo TO", "SET foo"} {
		if cmd := ParseSetCommand(sql); cmd.Kind != SetCmdNone {
			t.Errorf("%q should not parse: got %+v", sql, cmd)
		}
	}
}

func TestParseSetCommand_RejectsResetWithGarbage(t *testing.T) {
	if cmd := ParseSetCommand("RESET foo bar"); cmd.Kind != SetCmdNone {
		t.Errorf("RESET foo bar should not parse: got %+v", cmd)
	}
}

func TestParseSetCommand_RejectsSetTimeZoneTwoWordForm(t *testing.T) {
	// `SET TIME ZONE 'UTC'` is the legacy two-word form. The one-word
	// `timezone` GUC IS unsafe (it changes wire-rendering of timestamps),
	// but the two-word grammar doesn't fit our parser. Returning
	// SetCmdNone is acceptable in practice because the dirty flag +
	// verify-on-checkout fallback (see ConnectionGucState) will rebuild
	// state from pg_settings on next checkout if the unusual form ever
	// lands.
	if cmd := ParseSetCommand("SET TIME ZONE 'UTC'"); cmd.Kind != SetCmdNone {
		t.Errorf("got %+v", cmd)
	}
}

// --- ParseSetCommand: DISCARD ---

func TestParseSetCommand_DiscardAll(t *testing.T) {
	for _, sql := range []string{
		"DISCARD ALL",
		"discard all",
		"Discard All",
		"DISCARD ALL;",
	} {
		cmd := ParseSetCommand(sql)
		if cmd.Kind != SetCmdResetAll {
			t.Errorf("%q: expected SetCmdResetAll, got %+v", sql, cmd)
		}
	}
}

func TestParseSetCommand_DiscardOtherSubcommands(t *testing.T) {
	for _, sql := range []string{
		"DISCARD PLANS",
		"DISCARD SEQUENCES",
		"DISCARD TEMP",
		"DISCARD TEMPORARY",
		"discard plans",
	} {
		cmd := ParseSetCommand(sql)
		if cmd.Kind != SetCmdDiscardOther {
			t.Errorf("%q: expected SetCmdDiscardOther, got %+v", sql, cmd)
		}
	}
}

func TestParseSetCommand_RejectsDiscardWithUnknownSubcommand(t *testing.T) {
	for _, sql := range []string{
		"DISCARD",         // missing argument
		"DISCARD CACHE",   // not a real subcommand
		"DISCARD ALL FOO", // junk after ALL
	} {
		cmd := ParseSetCommand(sql)
		if cmd.Kind != SetCmdNone {
			t.Errorf("%q: expected SetCmdNone, got %+v", sql, cmd)
		}
	}
}

// --- ParseSetCommand: SELECT set_config(...) ---

func TestParseSetCommand_SetConfigBasic(t *testing.T) {
	cmd := ParseSetCommand("SELECT set_config('app.user_id', '42', false)")
	if cmd.Kind != SetCmdSet || cmd.Name != "app.user_id" || cmd.Value != "42" {
		t.Fatalf("got %+v", cmd)
	}
}

func TestParseSetCommand_SetConfigPgCatalogQualified(t *testing.T) {
	cmd := ParseSetCommand("SELECT pg_catalog.set_config('app.tenant', 'acme', false)")
	if cmd.Kind != SetCmdSet || cmd.Name != "app.tenant" || cmd.Value != "acme" {
		t.Fatalf("got %+v", cmd)
	}
}

func TestParseSetCommand_SetConfigIsLocalTrueMapsToSetLocal(t *testing.T) {
	cmd := ParseSetCommand("SELECT set_config('app.user_id', '42', true)")
	if cmd.Kind != SetCmdSetLocal || cmd.Name != "app.user_id" || cmd.Value != "42" {
		t.Fatalf("got %+v", cmd)
	}
}

func TestParseSetCommand_SetConfigQuotedBoolForms(t *testing.T) {
	cases := map[string]SetCommandKind{
		"SELECT set_config('app.x', '1', 't')":     SetCmdSetLocal,
		"SELECT set_config('app.x', '1', 'f')":     SetCmdSet,
		"SELECT set_config('app.x', '1', 'true')":  SetCmdSetLocal,
		"SELECT set_config('app.x', '1', 'false')": SetCmdSet,
		"SELECT set_config('app.x', '1', 'on')":    SetCmdSetLocal,
		"SELECT set_config('app.x', '1', 'off')":   SetCmdSet,
		"SELECT set_config('app.x', '1', 1)":       SetCmdSetLocal,
		"SELECT set_config('app.x', '1', 0)":       SetCmdSet,
	}
	for sql, want := range cases {
		cmd := ParseSetCommand(sql)
		if cmd.Kind != want {
			t.Errorf("%q: want %v, got %+v", sql, want, cmd)
		}
	}
}

func TestParseSetCommand_SetConfigCaseInsensitive(t *testing.T) {
	cmd := ParseSetCommand("select Set_Config('app.user_id', '42', false)")
	if cmd.Kind != SetCmdSet || cmd.Name != "app.user_id" || cmd.Value != "42" {
		t.Fatalf("got %+v", cmd)
	}
}

func TestParseSetCommand_SetConfigLowercasesGUCName(t *testing.T) {
	cmd := ParseSetCommand("SELECT set_config('App.User_ID', '42', false)")
	if cmd.Kind != SetCmdSet || cmd.Name != "app.user_id" {
		t.Fatalf("got %+v", cmd)
	}
}

func TestParseSetCommand_SetConfigEmptyValue(t *testing.T) {
	// Empty string is a valid value (resets the parameter to its default).
	cmd := ParseSetCommand("SELECT set_config('app.user_id', '', false)")
	if cmd.Kind != SetCmdSet || cmd.Name != "app.user_id" || cmd.Value != "" {
		t.Fatalf("got %+v", cmd)
	}
}

func TestParseSetCommand_SetConfigTrailingSemicolon(t *testing.T) {
	cmd := ParseSetCommand("SELECT set_config('app.user_id', '42', false);")
	if cmd.Kind != SetCmdSet || cmd.Name != "app.user_id" || cmd.Value != "42" {
		t.Fatalf("got %+v", cmd)
	}
}

func TestParseSetCommand_RejectsSetConfigWrongArgCount(t *testing.T) {
	for _, sql := range []string{
		"SELECT set_config('app.user_id', '42')",
		"SELECT set_config('app.user_id')",
		"SELECT set_config()",
		"SELECT set_config('app.user_id', '42', false, 'extra')",
	} {
		cmd := ParseSetCommand(sql)
		if cmd.Kind != SetCmdNone {
			t.Errorf("%q: expected SetCmdNone, got %+v", sql, cmd)
		}
	}
}

func TestParseSetCommand_RejectsSetConfigInsideLargerSelect(t *testing.T) {
	// We deliberately don't try to parse set_config when it's nested in
	// a larger expression. Rejecting these shapes is conservative — the
	// dirty flag + verify-on-checkout will catch any state that did
	// move.
	for _, sql := range []string{
		"SELECT col, set_config('a.b', '1', false) FROM t",
		"SELECT set_config('a.b', '1', false) FROM t",
		"SELECT set_config('a.b', '1', false) WHERE x = 1",
	} {
		cmd := ParseSetCommand(sql)
		if cmd.Kind != SetCmdNone {
			t.Errorf("%q: expected SetCmdNone, got %+v", sql, cmd)
		}
	}
}

func TestParseSetCommand_RejectsSetConfigWithBadBoolean(t *testing.T) {
	cmd := ParseSetCommand("SELECT set_config('app.user_id', '42', maybe)")
	if cmd.Kind != SetCmdNone {
		t.Errorf("expected SetCmdNone, got %+v", cmd)
	}
}

func TestParseSetCommand_RejectsSetConfigWithNonLiteralName(t *testing.T) {
	// Column references / identifiers aren't the canonical RLS shape.
	cmd := ParseSetCommand("SELECT set_config(some_col, '42', false)")
	if cmd.Kind != SetCmdNone {
		t.Errorf("expected SetCmdNone, got %+v", cmd)
	}
}

func TestParseSetCommand_SetConfigArgsContainParens(t *testing.T) {
	// Quoted values containing parens or commas must not confuse the
	// argument splitter.
	cmd := ParseSetCommand("SELECT set_config('app.tenant', 'foo(bar)baz', false)")
	if cmd.Kind != SetCmdSet || cmd.Name != "app.tenant" || cmd.Value != "foo(bar)baz" {
		t.Fatalf("got %+v", cmd)
	}
	cmd = ParseSetCommand("SELECT set_config('app.tenant', 'a, b, c', false)")
	if cmd.Kind != SetCmdSet || cmd.Name != "app.tenant" || cmd.Value != "a, b, c" {
		t.Fatalf("got %+v", cmd)
	}
}

// --- ConnectionGucState: set_config integration ---

func TestConnectionGucState_SetConfigUpdatesState(t *testing.T) {
	s := NewConnectionGucState()
	a := NewConnectionGucState()

	if !s.ObserveSQL("SELECT set_config('app.user_id', '42', false)") {
		t.Fatal("set_config should mutate state")
	}
	a.ObserveSQL("SET app.user_id = '42'")
	if s.Hash() != a.Hash() {
		t.Fatalf("set_config(false) should equal SET: %d vs %d", s.Hash(), a.Hash())
	}
}

func TestConnectionGucState_SetConfigIsLocalDoesNotMutateState(t *testing.T) {
	s := NewConnectionGucState()
	if s.ObserveSQL("SELECT set_config('app.user_id', '42', true)") {
		t.Error("set_config(..., true) is SET LOCAL — should not mutate state")
	}
	if s.Hash() != 0 {
		t.Fatalf("hash should remain baseline, got %d", s.Hash())
	}
}

func TestConnectionGucState_SetConfigPgCatalogPathIntegrates(t *testing.T) {
	s := NewConnectionGucState()
	s.ObserveSQL("SELECT pg_catalog.set_config('search_path', 'tenant_a, public', false)")
	if s.Hash() == 0 {
		t.Fatal("pg_catalog.set_config on unsafe GUC should mutate state")
	}
}

// --- ConnectionGucState: DISCARD integration ---

func TestConnectionGucState_DiscardAllClearsState(t *testing.T) {
	s := NewConnectionGucState()
	s.ObserveSQL("SET app.user_id = '42'")
	s.ObserveSQL("SET search_path TO 'tenant_a'")
	if s.Hash() == 0 {
		t.Fatal("setup: expected non-zero hash")
	}
	if !s.ObserveSQL("DISCARD ALL") {
		t.Error("DISCARD ALL on non-empty state should report changed")
	}
	if s.Hash() != 0 {
		t.Fatalf("DISCARD ALL should restore baseline, got %d", s.Hash())
	}
}

func TestConnectionGucState_DiscardOtherDoesNotChangeHash(t *testing.T) {
	s := NewConnectionGucState()
	s.ObserveSQL("SET app.user_id = '42'")
	h := s.Hash()
	for _, sub := range []string{"PLANS", "SEQUENCES", "TEMP", "TEMPORARY"} {
		if s.ObserveSQL("DISCARD "+sub) {
			t.Errorf("DISCARD %s should not move state hash", sub)
		}
		if s.Hash() != h {
			t.Errorf("DISCARD %s should leave hash unchanged: got %d, want %d",
				sub, s.Hash(), h)
		}
	}
}

// --- ConnectionGucState ---

func TestConnectionGucState_EmptyHashIsZero(t *testing.T) {
	s := NewConnectionGucState()
	if s.Hash() != 0 {
		t.Fatalf("expected baseline hash 0, got %d", s.Hash())
	}
}

func TestConnectionGucState_SafeSetDoesNotChangeHash(t *testing.T) {
	s := NewConnectionGucState()
	for _, sql := range []string{
		"SET application_name = 'foo'",
		"SET statement_timeout = 5000",
		"SET work_mem = '64MB'",
		"SET random_page_cost = 1.1",
		"SET jit = off",
	} {
		s.ObserveSQL(sql)
		if s.Hash() != 0 {
			t.Fatalf("after %q: expected hash 0, got %d", sql, s.Hash())
		}
	}
}

func TestConnectionGucState_UnsafeSetChangesHash(t *testing.T) {
	s := NewConnectionGucState()
	h0 := s.Hash()
	s.ObserveSQL("SET app.user_id = '42'")
	if s.Hash() == h0 {
		t.Fatal("unsafe SET must change the hash")
	}
}

func TestConnectionGucState_SameUnsafeSetYieldsSameHashOnTwoConnections(t *testing.T) {
	a := NewConnectionGucState()
	b := NewConnectionGucState()
	a.ObserveSQL("SET app.user_id = '42'")
	b.ObserveSQL("SET app.user_id = '42'")
	if a.Hash() != b.Hash() {
		t.Fatalf("identical state should hash identically: %d vs %d", a.Hash(), b.Hash())
	}
}

func TestConnectionGucState_DifferentUnsafeValuesYieldDifferentHashes(t *testing.T) {
	a := NewConnectionGucState()
	b := NewConnectionGucState()
	a.ObserveSQL("SET app.user_id = '42'")
	b.ObserveSQL("SET app.user_id = '43'")
	if a.Hash() == b.Hash() {
		t.Fatal("different values should hash differently")
	}
}

func TestConnectionGucState_InsertionOrderDoesNotMatter(t *testing.T) {
	a := NewConnectionGucState()
	a.ObserveSQL("SET app.user_id = '42'")
	a.ObserveSQL("SET app.tenant = 'alpha'")

	b := NewConnectionGucState()
	b.ObserveSQL("SET app.tenant = 'alpha'")
	b.ObserveSQL("SET app.user_id = '42'")

	if a.Hash() != b.Hash() {
		t.Fatalf("sorted-key hashing should be order-independent: %d vs %d", a.Hash(), b.Hash())
	}
}

func TestConnectionGucState_ResetReturnsToBaseline(t *testing.T) {
	s := NewConnectionGucState()
	baseline := s.Hash()
	s.ObserveSQL("SET app.user_id = '42'")
	if s.Hash() == baseline {
		t.Fatal("SET should have moved the hash off baseline")
	}
	s.ObserveSQL("RESET app.user_id")
	if s.Hash() != baseline {
		t.Fatalf("RESET should restore baseline: got %d, want %d", s.Hash(), baseline)
	}
}

func TestConnectionGucState_ResetAllClearsAllUnsafeState(t *testing.T) {
	s := NewConnectionGucState()
	s.ObserveSQL("SET app.user_id = '42'")
	s.ObserveSQL("SET search_path TO 'tenant_a'")
	s.ObserveSQL("SET role = 'app_user'")
	if s.Hash() == 0 {
		t.Fatal("expected non-zero hash after multiple unsafe SETs")
	}
	s.ObserveSQL("RESET ALL")
	if s.Hash() != 0 {
		t.Fatalf("RESET ALL should drop all unsafe state: got %d", s.Hash())
	}
}

func TestConnectionGucState_SetLocalDoesNotChangeHash(t *testing.T) {
	s := NewConnectionGucState()
	// Even an unsafe-named SET LOCAL must not move the hash — SET LOCAL
	// only takes effect inside a txn, and CachedConn already bypasses the
	// cache for inTransaction=true.
	s.ObserveSQL("SET LOCAL app.user_id = '42'")
	if s.Hash() != 0 {
		t.Fatalf("SET LOCAL should not move hash: got %d", s.Hash())
	}
}

func TestConnectionGucState_ObserveSQLReturnsChangeFlag(t *testing.T) {
	s := NewConnectionGucState()
	if !s.ObserveSQL("SET app.user_id = '42'") {
		t.Error("first set should report changed")
	}
	if s.ObserveSQL("SELECT 1") {
		t.Error("non-SET should report unchanged")
	}
	if s.ObserveSQL("SET work_mem = '64MB'") {
		t.Error("safe SET should report unchanged")
	}
	if !s.ObserveSQL("RESET app.user_id") {
		t.Error("RESET unsafe should report changed")
	}
}

func TestConnectionGucState_ResetSafeGUCIsNoOp(t *testing.T) {
	s := NewConnectionGucState()
	s.ObserveSQL("SET app.user_id = '42'")
	h := s.Hash()
	s.ObserveSQL("RESET work_mem") // safe — must not perturb.
	if s.Hash() != h {
		t.Fatalf("RESET on safe GUC should not move hash: got %d, want %d", s.Hash(), h)
	}
}

func TestConnectionGucState_OverwriteUnsafeValueChangesHash(t *testing.T) {
	s := NewConnectionGucState()
	s.ObserveSQL("SET app.user_id = '42'")
	h1 := s.Hash()
	s.ObserveSQL("SET app.user_id = '43'")
	if s.Hash() == h1 {
		t.Fatal("overwriting an unsafe value should change the hash")
	}
}

// --- stripStringLiterals ---

func TestStripStringLiterals_BlanksSingleQuotedBody(t *testing.T) {
	got := stripStringLiterals("SELECT 'INSERT INTO orders' FROM logs")
	want := "SELECT '                  ' FROM logs"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if len(got) != len("SELECT 'INSERT INTO orders' FROM logs") {
		t.Fatal("length must be preserved")
	}
}

func TestStripStringLiterals_BlanksDoubleQuotedBody(t *testing.T) {
	got := stripStringLiterals(`SELECT * FROM "into_table"`)
	want := `SELECT * FROM "          "`
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestStripStringLiterals_HandlesDoubledQuoteEscape(t *testing.T) {
	// Both delimiters of the escape pair are blanked; the scanner stays
	// inside the literal until the real closing quote. Body length 13.
	got := stripStringLiterals("SELECT 'it''s INTO ok' FROM x")
	want := "SELECT '             ' FROM x"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if len(got) != len("SELECT 'it''s INTO ok' FROM x") {
		t.Fatal("length must be preserved")
	}
}

func TestStripStringLiterals_LeavesUnquotedTextAlone(t *testing.T) {
	got := stripStringLiterals("SELECT * INTO new_table FROM source")
	if got != "SELECT * INTO new_table FROM source" {
		t.Fatalf("got %q, want passthrough", got)
	}
}

func TestStripStringLiterals_EmptyAndShort(t *testing.T) {
	if got := stripStringLiterals(""); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
	if got := stripStringLiterals("''"); got != "''" {
		t.Fatalf("empty literal: got %q, want %q", got, "''")
	}
}

// --- SplitStatements ---

func TestSplitStatements_SimpleTwoStatements(t *testing.T) {
	got := SplitStatements("SET foo = '42'; SELECT 1")
	want := []string{"SET foo = '42'", "SELECT 1"}
	if !stringSliceEq(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSplitStatements_DropsEmptySegments(t *testing.T) {
	got := SplitStatements("; SET foo = '42';;SELECT 1;")
	want := []string{"SET foo = '42'", "SELECT 1"}
	if !stringSliceEq(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSplitStatements_RespectsSingleQuotes(t *testing.T) {
	got := SplitStatements("SET foo = 'a;b'; SELECT 1")
	want := []string{"SET foo = 'a;b'", "SELECT 1"}
	if !stringSliceEq(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSplitStatements_RespectsDoubleQuotes(t *testing.T) {
	got := SplitStatements(`SET "app;guc" = 'x'; SELECT 1`)
	want := []string{`SET "app;guc" = 'x'`, "SELECT 1"}
	if !stringSliceEq(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSplitStatements_HandlesDoubledQuoteEscape(t *testing.T) {
	got := SplitStatements("SET foo = 'it''s; ok'; SELECT 1")
	want := []string{"SET foo = 'it''s; ok'", "SELECT 1"}
	if !stringSliceEq(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSplitStatements_SingleStatementPassThrough(t *testing.T) {
	got := SplitStatements("SET foo = '42'")
	want := []string{"SET foo = '42'"}
	if !stringSliceEq(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSplitStatements_Empty(t *testing.T) {
	for _, sql := range []string{"", "   ", ";;;"} {
		got := SplitStatements(sql)
		if len(got) != 0 {
			t.Errorf("%q: expected empty, got %v", sql, got)
		}
	}
}

// --- ConnectionGucState multi-statement ---

func TestConnectionGucState_ObserveMultiStatementAppliesAllSets(t *testing.T) {
	s := NewConnectionGucState()
	s.ObserveSQL("SET app.user_id = '42'; SELECT * FROM accounts")
	if s.Hash() == 0 {
		t.Fatal("multi-statement Q should still update state")
	}
}

func TestConnectionGucState_ObserveMultiStatementAppliesTwoUnsafeSets(t *testing.T) {
	a := NewConnectionGucState()
	a.ObserveSQL("SET app.user_id = '42'")
	a.ObserveSQL("SET app.tenant = 'alpha'")

	b := NewConnectionGucState()
	b.ObserveSQL("SET app.user_id = '42'; SET app.tenant = 'alpha'")

	if a.Hash() != b.Hash() {
		t.Fatalf("batched SETs should hash the same as separate SETs: %d vs %d", a.Hash(), b.Hash())
	}
}

func TestConnectionGucState_ObserveMultiStatementWithQuotedSemicolon(t *testing.T) {
	s := NewConnectionGucState()
	s.ObserveSQL("SET app.tenant = 'has;semicolon'; SELECT 1")
	if s.Hash() == 0 {
		t.Fatal("expected state to update; the quoted semicolon must not split")
	}
}

// --- Concurrency: ConnectionGucState is RWMutex-guarded ---

func TestConnectionGucState_ConcurrentObserveAndHashIsRaceClean(t *testing.T) {
	// The wrapper's CachedConn is typically bound to a single Querier that
	// is itself goroutine-confined, but we don't statically enforce that.
	// Confirm Apply / ObserveSQL / Hash are race-free under -race.
	s := NewConnectionGucState()
	var wg sync.WaitGroup
	const writers, readers, iters = 4, 4, 200
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				if i%3 == 0 {
					s.ObserveSQL("SET app.user_id = 'x'")
				} else {
					s.ObserveSQL("SET app.tenant = 'y'")
				}
			}
		}(w)
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				_ = s.Hash()
			}
		}()
	}
	wg.Wait()
	// Both unsafe GUCs should be set at the end.
	if s.Hash() == 0 {
		t.Fatal("expected non-zero hash after concurrent SETs")
	}
}

// stringSliceEq compares two []string for equality.
func stringSliceEq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
