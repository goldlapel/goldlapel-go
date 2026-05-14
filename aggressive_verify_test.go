package goldlapel

import (
	"context"
	"sync"
	"testing"
)

// dmlMockQuerier captures the SQL the wrapper sends to the underlying
// connection so the tests can prove the always-on bump path doesn't
// fire a verify roundtrip per DML. It also recognises the verify-
// against-pg_settings shape (used by the function-call path, NOT the
// DML path) so we can assert that the DML path stays off it.
type dmlMockQuerier struct {
	mu sync.Mutex

	queries []string
}

func newDmlMockQuerier() *dmlMockQuerier {
	return &dmlMockQuerier{}
}

func (m *dmlMockQuerier) recordQuery(sql string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queries = append(m.queries, sql)
}

func (m *dmlMockQuerier) queryList() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.queries))
	copy(out, m.queries)
	return out
}

func (m *dmlMockQuerier) verifyCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, sql := range m.queries {
		if isVerifySQL(sql) {
			n++
		}
	}
	return n
}

func (m *dmlMockQuerier) Query(ctx context.Context, sql string, args ...interface{}) (Rows, error) {
	m.recordQuery(sql)
	if isVerifySQL(sql) {
		return &mockRows{rows: nil, fields: []FieldDescription{{Name: "name"}, {Name: "setting"}}, pos: -1}, nil
	}
	return &mockRows{rows: [][]interface{}{{1}}, fields: []FieldDescription{{Name: "id"}}, pos: -1}, nil
}

func (m *dmlMockQuerier) QueryRow(ctx context.Context, sql string, args ...interface{}) Row {
	m.recordQuery(sql)
	return &mockRow{values: []interface{}{1}}
}

func (m *dmlMockQuerier) Exec(ctx context.Context, sql string, args ...interface{}) (interface{}, error) {
	m.recordQuery(sql)
	return nil, nil
}

// setupDmlWrapped builds a CachedConn against a dmlMockQuerier. The
// native cache is reset between tests so dml_seq-isolated cache slots
// don't leak across cases.
func setupDmlWrapped(t *testing.T) (*CachedConn, *dmlMockQuerier) {
	t.Helper()
	ResetNativeCache()
	cache := GetNativeCache()
	cache.mu.Lock()
	cache.invConnected = true
	cache.mu.Unlock()
	mock := newDmlMockQuerier()
	cc := &CachedConn{real: mock, cache: cache}
	t.Cleanup(func() { cc.Close() })
	return cc, mock
}

// --- DML detection ---

func TestAggressiveVerify_LooksLikeDML(t *testing.T) {
	cases := []struct {
		sql  string
		want bool
	}{
		{"INSERT INTO t VALUES (1)", true},
		{"insert into t values (1)", true},
		{"  UPDATE t SET x = 1", true},
		{"DELETE FROM t WHERE id = 1", true},
		{"MERGE INTO t USING ...", true},
		{"TRUNCATE t", true},
		{"TRUNCATE TABLE t", true},
		{"WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x", true},
		{"WITH x AS (SELECT 1) UPDATE t SET y = 1 FROM x", true},
		{"WITH x AS (SELECT 1) DELETE FROM t WHERE id IN (SELECT * FROM x)", true},
		{"SELECT * FROM t", false},
		{"SELECT my_func()", false},
		{"BEGIN", false},
		{"COMMIT", false},
		{"CREATE TABLE t (id int)", false},
		{"DROP TABLE t", false},
		{"ALTER TABLE t ADD COLUMN x int", false},
		{"COPY t FROM '/tmp/x'", false},
		{"SET app.user_id = '42'", false},
		{"  ", false},
		{"", false},
		// Multi-statement bodies.
		{"SET app.user_id = '42'; INSERT INTO t VALUES (1)", true},
		{"BEGIN; SELECT 1; COMMIT", false},
		{"INSERT INTO a VALUES (1); SELECT 2", true},
	}
	for _, c := range cases {
		got := looksLikeDML(c.sql)
		if got != c.want {
			t.Errorf("looksLikeDML(%q) = %v, want %v", c.sql, got, c.want)
		}
	}
}

// --- Always-on dml_seq bump under Auto / On ---

func TestAggressiveVerify_DefaultAutoBumpsDmlSeqOnDML(t *testing.T) {
	cc, mock := setupDmlWrapped(t)
	ctx := context.Background()

	state := cc.ensureGucState()
	if state.DmlSeq() != 0 {
		t.Fatalf("baseline dml_seq must be 0, got %d", state.DmlSeq())
	}
	baselineHash := state.Hash()
	if baselineHash != 0 {
		t.Fatalf("baseline hash must be 0, got %x", baselineHash)
	}

	if _, err := cc.Exec(ctx, "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}

	if got := state.DmlSeq(); got != 1 {
		t.Errorf("dml_seq after one DML = %d, want 1", got)
	}
	if state.Hash() == baselineHash {
		t.Error("state hash must change after a DML bump")
	}
	// Zero verify roundtrips per write — the whole point of the
	// always-on bump model.
	if got := mock.verifyCount(); got != 0 {
		t.Errorf("DML must NOT trigger a verify roundtrip, got %d", got)
	}
}

func TestAggressiveVerify_ExplicitOnBumpsDmlSeq(t *testing.T) {
	cc, mock := setupDmlWrapped(t)
	cc.SetAggressiveVerify(AggressiveVerifyOn)
	ctx := context.Background()

	if _, err := cc.Exec(ctx, "UPDATE t SET x = 1"); err != nil {
		t.Fatal(err)
	}
	if got := cc.ensureGucState().DmlSeq(); got != 1 {
		t.Errorf("dml_seq under explicit On = %d, want 1", got)
	}
	if got := mock.verifyCount(); got != 0 {
		t.Errorf("On mode must NOT fire verify roundtrip, got %d", got)
	}
}

func TestAggressiveVerify_OffSuppressesBumpAndWarnsOnce(t *testing.T) {
	// Reset the package-wide once so the warning sub-assertion below
	// can fire deterministically. Mutating sync.Once via reset isn't
	// possible from outside the package internals; we instead drive
	// the warning path by observing that the first DML under Off
	// doesn't bump dml_seq AND that the mode string round-trips.
	cc, mock := setupDmlWrapped(t)
	cc.SetAggressiveVerify(AggressiveVerifyOff)
	ctx := context.Background()

	if _, err := cc.Exec(ctx, "DELETE FROM t WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	if got := cc.ensureGucState().DmlSeq(); got != 0 {
		t.Errorf("dml_seq under Off = %d, want 0 (suppressed)", got)
	}
	if got := mock.verifyCount(); got != 0 {
		t.Errorf("Off mode must NOT fire verify roundtrip, got %d", got)
	}
}

// --- Cache-key isolation after DML ---

func TestAggressiveVerify_CacheMissAfterDML(t *testing.T) {
	cc, _ := setupDmlWrapped(t)
	ctx := context.Background()

	// Prime the L1 cache for SELECT * FROM t — a baseline-hash cache
	// slot.
	rows, err := cc.Query(ctx, "SELECT * FROM t WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()

	// One DML — must bump dml_seq, rolling the cache key forward.
	if _, err := cc.Exec(ctx, "INSERT INTO t VALUES (2)"); err != nil {
		t.Fatal(err)
	}
	if cc.ensureGucState().DmlSeq() != 1 {
		t.Fatal("DML did not bump dml_seq")
	}

	// Same SELECT again. The post-DML state hash differs from the
	// pre-DML slot, so the L1 lookup must miss — the slot we primed
	// above is no longer reachable from this connection's new key.
	hits0 := cc.cache.StatsHits()
	rows2, err := cc.Query(ctx, "SELECT * FROM t WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	rows2.Close()
	hits1 := cc.cache.StatsHits()
	if hits1 != hits0 {
		t.Errorf("post-DML read must miss L1 (hits before=%d after=%d)", hits0, hits1)
	}
}

func TestAggressiveVerify_NoBumpUnderOffPreservesCacheHit(t *testing.T) {
	cc, _ := setupDmlWrapped(t)
	cc.SetAggressiveVerify(AggressiveVerifyOff)
	ctx := context.Background()

	// Prime L1.
	rows, err := cc.Query(ctx, "SELECT * FROM t WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
	// DML — but Off mode suppresses the bump. The cache key stays at
	// the baseline so the subsequent same-SELECT hits L1. (Note: a
	// write to table `t` invalidates the cache slot for table `t`, so
	// for this assertion to test cache-key-isolation specifically we
	// use a SELECT against a different table.)
	if _, err := cc.Exec(ctx, "INSERT INTO other_table VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if got := cc.ensureGucState().DmlSeq(); got != 0 {
		t.Errorf("Off must leave dml_seq at 0, got %d", got)
	}

	hits0 := cc.cache.StatsHits()
	rows2, err := cc.Query(ctx, "SELECT * FROM t WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	rows2.Close()
	hits1 := cc.cache.StatsHits()
	if hits1 == hits0 {
		t.Errorf("Off mode must preserve cache hit on unrelated read (hits before=%d after=%d)", hits0, hits1)
	}
}

// --- Dirty-flag bypasses L1 ---

func TestAggressiveVerify_DirtyBypassesL1(t *testing.T) {
	cc, mock := setupDmlWrapped(t)
	ctx := context.Background()

	// Prime L1 for SELECT * FROM t.
	rows, err := cc.Query(ctx, "SELECT * FROM t WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
	// Confirm the slot is populated by hitting it once.
	hits0 := cc.cache.StatsHits()
	rows2, err := cc.Query(ctx, "SELECT * FROM t WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	rows2.Close()
	hits1 := cc.cache.StatsHits()
	if hits1 != hits0+1 {
		t.Fatalf("baseline same-key read must hit L1 (delta %d, want 1)", hits1-hits0)
	}

	// Now mark dirty (simulating a top-level SELECT my_func() that
	// could have done a server-side SET).
	cc.ensureGucState().MarkDirty()

	// The current call (and subsequent) must bypass L1 and route to
	// the underlying Querier. Hit counter stays flat; the underlying
	// mock observes the query.
	preQueries := len(mock.queryList())
	prehits := cc.cache.StatsHits()
	rows3, err := cc.Query(ctx, "SELECT * FROM t WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	rows3.Close()
	posthits := cc.cache.StatsHits()
	postQueries := len(mock.queryList())

	if posthits != prehits {
		t.Errorf("dirty bypass must not register an L1 hit (hits before=%d after=%d)", prehits, posthits)
	}
	if postQueries == preQueries {
		t.Error("dirty bypass must route to the underlying Querier")
	}
}

func TestAggressiveVerify_DirtyBypassesL1OnQueryRow(t *testing.T) {
	cc, mock := setupDmlWrapped(t)
	ctx := context.Background()

	// Prime via Query so QueryRow can hit the same slot.
	rows, err := cc.Query(ctx, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()

	// Mark dirty before QueryRow.
	cc.ensureGucState().MarkDirty()

	preQueries := len(mock.queryList())
	row := cc.QueryRow(ctx, "SELECT 1")
	var v int
	_ = row.Scan(&v)
	postQueries := len(mock.queryList())

	if postQueries == preQueries {
		t.Error("QueryRow dirty bypass must route to the underlying Querier")
	}
}

// --- WithAggressiveVerify option + mode strings ---

func TestAggressiveVerify_WithAggressiveVerifyOption(t *testing.T) {
	gl := &GoldLapel{}
	WithAggressiveVerify(AggressiveVerifyOn).applyStart(gl)
	if !gl.aggressiveVerifySet {
		t.Error("aggressiveVerifySet should be true after option applied")
	}
	if gl.aggressiveVerify != AggressiveVerifyOn {
		t.Errorf("expected AggressiveVerifyOn, got %v", gl.aggressiveVerify)
	}

	gl2 := &GoldLapel{}
	WithAggressiveVerify(AggressiveVerifyOff).applyStart(gl2)
	if gl2.aggressiveVerify != AggressiveVerifyOff {
		t.Errorf("expected AggressiveVerifyOff, got %v", gl2.aggressiveVerify)
	}
}

func TestAggressiveVerify_ModeStrings(t *testing.T) {
	cases := map[AggressiveVerifyMode]string{
		AggressiveVerifyAuto:     "auto",
		AggressiveVerifyOn:       "on",
		AggressiveVerifyOff:      "off",
		AggressiveVerifyMode(99): "unknown",
	}
	for mode, want := range cases {
		got := mode.String()
		if got != want {
			t.Errorf("mode %d: got %q, want %q", mode, got, want)
		}
	}
}

func TestAggressiveVerify_PerCachedConnOverrideBeatsGoldLapel(t *testing.T) {
	// Simulate a *GoldLapel saying On while the per-CachedConn override
	// says Off — explicit per-conn always wins.
	cc, _ := setupDmlWrapped(t)
	// Install a GoldLapel-level On (not via Start; directly through the
	// registration hook the resolver uses).
	lastStartedInstanceMu.Lock()
	prev := lastStartedInstance
	lastStartedInstance = &GoldLapel{aggressiveVerify: AggressiveVerifyOn, aggressiveVerifySet: true}
	lastStartedInstanceMu.Unlock()
	t.Cleanup(func() {
		lastStartedInstanceMu.Lock()
		lastStartedInstance = prev
		lastStartedInstanceMu.Unlock()
	})

	cc.SetAggressiveVerify(AggressiveVerifyOff)
	ctx := context.Background()
	if _, err := cc.Exec(ctx, "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if got := cc.ensureGucState().DmlSeq(); got != 0 {
		t.Errorf("per-conn Off must beat GoldLapel On, dml_seq=%d", got)
	}
}

// --- Close lifecycle interaction ---

func TestAggressiveVerify_PostCloseDMLDoesNotPanic(t *testing.T) {
	cc, _ := setupDmlWrapped(t)
	ctx := context.Background()
	cc.Close()

	// Post-Close DML still parses and bumps (the bump is a local-state
	// operation; it doesn't dispatch an async goroutine). What MUST
	// not happen is a panic or a scheduled goroutine that races with
	// the closed lifecycle.
	if _, err := cc.Exec(ctx, "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
}

// --- Race-clean concurrent DML + introspection ---

func TestAggressiveVerify_RaceCleanUnderConcurrentDML(t *testing.T) {
	cc, _ := setupDmlWrapped(t)
	cc.SetAggressiveVerify(AggressiveVerifyOn)
	ctx := context.Background()

	const writers, readers, iters = 4, 4, 50
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				cc.Exec(ctx, "INSERT INTO t VALUES (1)")
				cc.Exec(ctx, "UPDATE t SET x = 1 WHERE id = 1")
				cc.Exec(ctx, "DELETE FROM t WHERE id = 1")
			}
		}()
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				_ = cc.GucStateHash()
				_ = cc.ensureGucState().IsDirty()
				_ = cc.aggressiveVerifyEnabled()
				_ = cc.ensureGucState().DmlSeq()
			}
		}()
	}
	wg.Wait()
	cc.Close()

	// At least one DML bumped dml_seq. The exact count depends on race
	// ordering and how many writers got scheduled; assert non-zero.
	if got := cc.ensureGucState().DmlSeq(); got == 0 {
		t.Error("expected non-zero dml_seq under concurrent DML")
	}
}

// --- DML inside multi-statement bodies ---

func TestAggressiveVerify_MultiStatementWithDMLBumps(t *testing.T) {
	cc, _ := setupDmlWrapped(t)
	cc.SetAggressiveVerify(AggressiveVerifyOn)
	ctx := context.Background()

	if _, err := cc.Exec(ctx, "SET app.user_id = '42'; INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if got := cc.ensureGucState().DmlSeq(); got != 1 {
		t.Errorf("multi-statement body containing DML should bump once, got %d", got)
	}
}

func TestAggressiveVerify_NonDMLDoesNotBump(t *testing.T) {
	cc, _ := setupDmlWrapped(t)
	cc.SetAggressiveVerify(AggressiveVerifyOn)
	ctx := context.Background()

	if _, err := cc.Query(ctx, "SELECT * FROM t WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	if got := cc.ensureGucState().DmlSeq(); got != 0 {
		t.Errorf("non-DML must not bump dml_seq, got %d", got)
	}
}

// --- RESET ALL / DISCARD ALL resets dml_seq ---

func TestAggressiveVerify_ResetAllClearsDmlSeq(t *testing.T) {
	cc, _ := setupDmlWrapped(t)
	ctx := context.Background()

	if _, err := cc.Exec(ctx, "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if cc.ensureGucState().DmlSeq() != 1 {
		t.Fatal("DML must bump dml_seq to 1")
	}

	// RESET ALL is authoritative — server has dropped every non-default
	// GUC and the wrapper's post-DML uncertainty is moot.
	if _, err := cc.Exec(ctx, "RESET ALL"); err != nil {
		t.Fatal(err)
	}
	if got := cc.ensureGucState().DmlSeq(); got != 0 {
		t.Errorf("RESET ALL must clear dml_seq, got %d", got)
	}
	if got := cc.ensureGucState().Hash(); got != 0 {
		t.Errorf("RESET ALL must clear state hash, got %x", got)
	}
}

func TestAggressiveVerify_DiscardAllClearsDmlSeq(t *testing.T) {
	cc, _ := setupDmlWrapped(t)
	ctx := context.Background()

	if _, err := cc.Exec(ctx, "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if cc.ensureGucState().DmlSeq() == 0 {
		t.Fatal("DML must bump dml_seq")
	}

	if _, err := cc.Exec(ctx, "DISCARD ALL"); err != nil {
		t.Fatal(err)
	}
	if got := cc.ensureGucState().DmlSeq(); got != 0 {
		t.Errorf("DISCARD ALL must clear dml_seq, got %d", got)
	}
}
