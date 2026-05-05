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
		"timezone",
		"application_name",
		"statement_timeout",
		"work_mem",
		"client_encoding",
		"DateStyle",
	} {
		if IsUnsafeGUC(name) {
			t.Errorf("expected %q to be safe", name)
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
	// `SET TIME ZONE 'UTC'` is the legacy two-word form. We don't model
	// it because timezone is harmless. Returning SetCmdNone is correct.
	if cmd := ParseSetCommand("SET TIME ZONE 'UTC'"); cmd.Kind != SetCmdNone {
		t.Errorf("got %+v", cmd)
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
		"SET timezone = 'UTC'",
		"SET application_name = 'foo'",
		"SET statement_timeout = 5000",
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
	if s.ObserveSQL("SET timezone = 'UTC'") {
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
	s.ObserveSQL("RESET timezone") // safe — must not perturb.
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
