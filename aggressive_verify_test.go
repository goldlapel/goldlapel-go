package goldlapel

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// aggressiveVerifyMockQuerier mirrors verifyMockQuerier but ALSO
// recognises the detection probe SQL (`SELECT 1 FROM pg_trigger ...`)
// and returns a controlled "found / not found" answer. Verify SQL
// behaviour is identical to verifyMockQuerier.
type aggressiveVerifyMockQuerier struct {
	mu sync.Mutex

	queries []string

	// detectionFound is the canned answer to the detection probe.
	// True ↔ "we found a trigger that mutates session state".
	detectionFound bool
	// detectionErr, when non-nil, makes the probe return an error
	// from Query (simulates permission denied on pg_proc, transient
	// network failure, etc.).
	detectionErr error
	// detectionCount counts probe invocations. Atomic so concurrent
	// goroutines don't need to take mu.
	detectionCount atomic.Int64

	// verifyResult is the canned snapshot returned to the verify
	// SQL.
	verifyResult map[string]string
	verifyCount  atomic.Int64
}

func newAggressiveVerifyMockQuerier() *aggressiveVerifyMockQuerier {
	return &aggressiveVerifyMockQuerier{verifyResult: make(map[string]string)}
}

func (m *aggressiveVerifyMockQuerier) recordQuery(sql string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queries = append(m.queries, sql)
}

func (m *aggressiveVerifyMockQuerier) queryList() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.queries))
	copy(out, m.queries)
	return out
}

func (m *aggressiveVerifyMockQuerier) setDetection(found bool, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.detectionFound = found
	m.detectionErr = err
}

func (m *aggressiveVerifyMockQuerier) Query(ctx context.Context, sql string, args ...interface{}) (Rows, error) {
	m.recordQuery(sql)
	if isDetectionSQL(sql) {
		m.detectionCount.Add(1)
		m.mu.Lock()
		err := m.detectionErr
		found := m.detectionFound
		m.mu.Unlock()
		if err != nil {
			return nil, err
		}
		if found {
			return &mockRows{rows: [][]interface{}{{1}}, fields: []FieldDescription{{Name: "?column?"}}, pos: -1}, nil
		}
		return &mockRows{rows: nil, fields: []FieldDescription{{Name: "?column?"}}, pos: -1}, nil
	}
	if isVerifySQL(sql) {
		m.verifyCount.Add(1)
		m.mu.Lock()
		snapshot := make([][]interface{}, 0, len(m.verifyResult))
		for k, v := range m.verifyResult {
			snapshot = append(snapshot, []interface{}{k, v})
		}
		m.mu.Unlock()
		return &mockRows{rows: snapshot, fields: []FieldDescription{{Name: "name"}, {Name: "setting"}}, pos: -1}, nil
	}
	return &mockRows{rows: [][]interface{}{{1}}, fields: []FieldDescription{{Name: "id"}}, pos: -1}, nil
}

func (m *aggressiveVerifyMockQuerier) QueryRow(ctx context.Context, sql string, args ...interface{}) Row {
	m.recordQuery(sql)
	return &mockRow{values: []interface{}{1}}
}

func (m *aggressiveVerifyMockQuerier) Exec(ctx context.Context, sql string, args ...interface{}) (interface{}, error) {
	m.recordQuery(sql)
	return nil, nil
}

func isDetectionSQL(sql string) bool {
	return strings.Contains(sql, "pg_trigger") && strings.Contains(sql, "pg_proc")
}

// setupAggressiveVerifyWrapped builds a CachedConn against an
// aggressiveVerifyMockQuerier with a unique per-test upstream key so
// the package-level detection cache doesn't bleed between cases.
func setupAggressiveVerifyWrapped(t *testing.T, upstreamKey string) (*CachedConn, *aggressiveVerifyMockQuerier) {
	t.Helper()
	ResetNativeCache()
	ResetAggressiveVerifyDetectionCache()
	SetAggressiveVerifyActive(false)
	cache := GetNativeCache()
	cache.mu.Lock()
	cache.invConnected = true
	cache.mu.Unlock()
	mock := newAggressiveVerifyMockQuerier()
	cc := &CachedConn{real: mock, cache: cache}
	cc.SetAggressiveVerifyUpstream(upstreamKey)
	t.Cleanup(func() {
		cc.Close()
		// Restore process-level state so unrelated tests aren't
		// affected (license-active flag is process-wide; detection
		// cache likewise).
		SetAggressiveVerifyActive(false)
		ResetAggressiveVerifyDetectionCache()
	})
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

// --- Mode resolution ---

func TestAggressiveVerify_ExplicitOnFiresVerifyAfterDML(t *testing.T) {
	cc, mock := setupAggressiveVerifyWrapped(t, "tcp://test1")
	cc.SetAggressiveVerify(AggressiveVerifyOn)
	ctx := context.Background()

	if _, err := cc.Exec(ctx, "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	cc.Close()

	if mock.verifyCount.Load() < 1 {
		t.Errorf("expected at least 1 async verify after DML, got %d", mock.verifyCount.Load())
	}
	// Detection probe must NOT fire — explicit On bypasses it.
	if mock.detectionCount.Load() != 0 {
		t.Errorf("explicit On should bypass detection, got %d probes", mock.detectionCount.Load())
	}
}

func TestAggressiveVerify_ExplicitOffSuppressesVerifyAfterDML(t *testing.T) {
	cc, mock := setupAggressiveVerifyWrapped(t, "tcp://test2")
	cc.SetAggressiveVerify(AggressiveVerifyOff)
	// Set the license-active flag on so we'd hit verify under Auto;
	// explicit Off must still suppress.
	SetAggressiveVerifyActive(true)
	ctx := context.Background()

	if _, err := cc.Exec(ctx, "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	// Sleep briefly so any (incorrectly) scheduled verify goroutine
	// has time to fire.
	time.Sleep(20 * time.Millisecond)
	cc.Close()

	if mock.verifyCount.Load() != 0 {
		t.Errorf("explicit Off should suppress all verifies, got %d", mock.verifyCount.Load())
	}
	if mock.detectionCount.Load() != 0 {
		t.Errorf("explicit Off should bypass detection, got %d probes", mock.detectionCount.Load())
	}
}

func TestAggressiveVerify_AutoLicenseSignalEnables(t *testing.T) {
	cc, mock := setupAggressiveVerifyWrapped(t, "tcp://test3")
	// No explicit per-CachedConn mode — Auto is default.
	SetAggressiveVerifyActive(true)
	ctx := context.Background()

	if _, err := cc.Exec(ctx, "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	cc.Close()

	if mock.verifyCount.Load() < 1 {
		t.Errorf("license signal should enable verify under Auto, got %d", mock.verifyCount.Load())
	}
	// License signal short-circuits detection — probe must not fire.
	if mock.detectionCount.Load() != 0 {
		t.Errorf("license signal should bypass detection, got %d probes", mock.detectionCount.Load())
	}
}

func TestAggressiveVerify_AutoDetectionPositiveEnables(t *testing.T) {
	cc, mock := setupAggressiveVerifyWrapped(t, "tcp://test4")
	mock.setDetection(true, nil)
	ctx := context.Background()

	if _, err := cc.Exec(ctx, "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	cc.Close()

	if mock.detectionCount.Load() != 1 {
		t.Errorf("expected exactly 1 detection probe, got %d", mock.detectionCount.Load())
	}
	if mock.verifyCount.Load() < 1 {
		t.Errorf("positive detection should enable verify, got %d", mock.verifyCount.Load())
	}
}

func TestAggressiveVerify_AutoDetectionNegativeDisables(t *testing.T) {
	cc, mock := setupAggressiveVerifyWrapped(t, "tcp://test5")
	mock.setDetection(false, nil)
	ctx := context.Background()

	if _, err := cc.Exec(ctx, "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	cc.Close()

	if mock.detectionCount.Load() != 1 {
		t.Errorf("expected exactly 1 detection probe, got %d", mock.detectionCount.Load())
	}
	if mock.verifyCount.Load() != 0 {
		t.Errorf("negative detection should leave verify off, got %d", mock.verifyCount.Load())
	}
}

func TestAggressiveVerify_DetectionErrorDefaultsOff(t *testing.T) {
	cc, mock := setupAggressiveVerifyWrapped(t, "tcp://test6")
	mock.setDetection(false, context.DeadlineExceeded)
	ctx := context.Background()

	if _, err := cc.Exec(ctx, "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	cc.Close()

	if mock.detectionCount.Load() != 1 {
		t.Errorf("expected exactly 1 detection probe, got %d", mock.detectionCount.Load())
	}
	if mock.verifyCount.Load() != 0 {
		t.Errorf("detection error should default off, got %d", mock.verifyCount.Load())
	}
}

// --- Cache: detection result reused across CachedConns ---

func TestAggressiveVerify_DetectionCacheReusedAcrossCachedConns(t *testing.T) {
	ResetNativeCache()
	ResetAggressiveVerifyDetectionCache()
	SetAggressiveVerifyActive(false)
	cache := GetNativeCache()
	cache.mu.Lock()
	cache.invConnected = true
	cache.mu.Unlock()

	const upstream = "tcp://shared-db:5432"

	// First CachedConn — runs the detection probe.
	mock1 := newAggressiveVerifyMockQuerier()
	mock1.setDetection(true, nil)
	cc1 := &CachedConn{real: mock1, cache: cache}
	cc1.SetAggressiveVerifyUpstream(upstream)
	t.Cleanup(func() { cc1.Close() })
	if _, err := cc1.Exec(context.Background(), "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	cc1.Close()
	if mock1.detectionCount.Load() != 1 {
		t.Errorf("first cc: expected 1 probe, got %d", mock1.detectionCount.Load())
	}

	// Second CachedConn against the SAME upstream — must reuse the
	// cached verdict and skip the probe entirely. We construct a
	// fresh mock so any probe call would tick THIS mock's counter,
	// not the first one's.
	mock2 := newAggressiveVerifyMockQuerier()
	cc2 := &CachedConn{real: mock2, cache: cache}
	cc2.SetAggressiveVerifyUpstream(upstream)
	t.Cleanup(func() { cc2.Close() })
	if _, err := cc2.Exec(context.Background(), "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	cc2.Close()
	if mock2.detectionCount.Load() != 0 {
		t.Errorf("second cc: detection cache should have been reused, got %d probes", mock2.detectionCount.Load())
	}
	if mock2.verifyCount.Load() < 1 {
		t.Errorf("second cc: verify should still fire from cached positive, got %d", mock2.verifyCount.Load())
	}

	// Cleanup is registered above; explicit cache reset for
	// downstream tests.
	t.Cleanup(func() { ResetAggressiveVerifyDetectionCache() })
}

func TestAggressiveVerify_DetectionCacheKeyedByUpstream(t *testing.T) {
	ResetNativeCache()
	ResetAggressiveVerifyDetectionCache()
	SetAggressiveVerifyActive(false)
	cache := GetNativeCache()
	cache.mu.Lock()
	cache.invConnected = true
	cache.mu.Unlock()

	// Upstream A → positive detection.
	mockA := newAggressiveVerifyMockQuerier()
	mockA.setDetection(true, nil)
	ccA := &CachedConn{real: mockA, cache: cache}
	ccA.SetAggressiveVerifyUpstream("tcp://upstream-A")
	t.Cleanup(func() { ccA.Close() })
	if _, err := ccA.Exec(context.Background(), "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}

	// Upstream B → negative detection. Must run its OWN probe (the
	// cache is keyed by URL, not shared globally).
	mockB := newAggressiveVerifyMockQuerier()
	mockB.setDetection(false, nil)
	ccB := &CachedConn{real: mockB, cache: cache}
	ccB.SetAggressiveVerifyUpstream("tcp://upstream-B")
	t.Cleanup(func() { ccB.Close() })
	if _, err := ccB.Exec(context.Background(), "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)

	ccA.Close()
	ccB.Close()
	if mockA.detectionCount.Load() != 1 {
		t.Errorf("upstream A: expected 1 probe, got %d", mockA.detectionCount.Load())
	}
	if mockB.detectionCount.Load() != 1 {
		t.Errorf("upstream B: expected 1 probe, got %d", mockB.detectionCount.Load())
	}
	if mockA.verifyCount.Load() < 1 {
		t.Errorf("upstream A (positive): verify should fire, got %d", mockA.verifyCount.Load())
	}
	if mockB.verifyCount.Load() != 0 {
		t.Errorf("upstream B (negative): verify should be off, got %d", mockB.verifyCount.Load())
	}
	t.Cleanup(func() { ResetAggressiveVerifyDetectionCache() })
}

// --- Per-CachedConn resolution gate ---

func TestAggressiveVerify_DetectionRunsOnceMaxPerCachedConn(t *testing.T) {
	cc, mock := setupAggressiveVerifyWrapped(t, "tcp://once")
	mock.setDetection(true, nil)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if _, err := cc.Exec(ctx, "INSERT INTO t VALUES (1)"); err != nil {
			t.Fatal(err)
		}
	}
	cc.Close()

	if mock.detectionCount.Load() != 1 {
		t.Errorf("expected exactly 1 detection probe across 5 DML calls, got %d", mock.detectionCount.Load())
	}
}

// --- DML detection at the wrapper level ---

func TestAggressiveVerify_NonDMLSkipsScheduling(t *testing.T) {
	cc, mock := setupAggressiveVerifyWrapped(t, "tcp://nondml")
	cc.SetAggressiveVerify(AggressiveVerifyOn)
	ctx := context.Background()

	// Plain SELECT — never DML, must never schedule.
	if _, err := cc.Query(ctx, "SELECT * FROM t WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	cc.Close()

	if mock.verifyCount.Load() != 0 {
		t.Errorf("non-DML should never schedule verify, got %d", mock.verifyCount.Load())
	}
}

func TestAggressiveVerify_QueryRowDMLSchedulesVerify(t *testing.T) {
	cc, mock := setupAggressiveVerifyWrapped(t, "tcp://qrow")
	cc.SetAggressiveVerify(AggressiveVerifyOn)
	ctx := context.Background()

	// INSERT ... RETURNING is the canonical QueryRow-DML shape.
	row := cc.QueryRow(ctx, "INSERT INTO t VALUES (1) RETURNING id")
	var id int
	_ = row.Scan(&id)
	cc.Close()

	if mock.verifyCount.Load() < 1 {
		t.Errorf("QueryRow on DML should schedule verify, got %d", mock.verifyCount.Load())
	}
}

func TestAggressiveVerify_QueryDMLSchedulesVerify(t *testing.T) {
	cc, mock := setupAggressiveVerifyWrapped(t, "tcp://q")
	cc.SetAggressiveVerify(AggressiveVerifyOn)
	ctx := context.Background()

	if _, err := cc.Query(ctx, "DELETE FROM t WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	cc.Close()

	if mock.verifyCount.Load() < 1 {
		t.Errorf("Query on DML should schedule verify, got %d", mock.verifyCount.Load())
	}
}

// --- Multi-statement bodies ---

func TestAggressiveVerify_MultiStatementWithDMLSchedules(t *testing.T) {
	cc, mock := setupAggressiveVerifyWrapped(t, "tcp://multi")
	cc.SetAggressiveVerify(AggressiveVerifyOn)
	ctx := context.Background()

	if _, err := cc.Exec(ctx, "SET app.user_id = '42'; INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	cc.Close()

	if mock.verifyCount.Load() < 1 {
		t.Errorf("multi-statement body containing DML should schedule, got %d", mock.verifyCount.Load())
	}
}

// --- Close lifecycle interaction ---

func TestAggressiveVerify_PostCloseDMLDoesNotSpawn(t *testing.T) {
	cc, mock := setupAggressiveVerifyWrapped(t, "tcp://post")
	cc.SetAggressiveVerify(AggressiveVerifyOn)
	ctx := context.Background()
	cc.Close()

	if _, err := cc.Exec(ctx, "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)

	if mock.verifyCount.Load() != 0 {
		t.Errorf("post-Close DML must not spawn verify, got %d", mock.verifyCount.Load())
	}
}

// --- Mode override precedence ---

func TestAggressiveVerify_ExplicitOnBeatsAutoNegativeDetection(t *testing.T) {
	cc, mock := setupAggressiveVerifyWrapped(t, "tcp://prec1")
	mock.setDetection(false, nil) // detection would say off
	cc.SetAggressiveVerify(AggressiveVerifyOn)
	ctx := context.Background()

	if _, err := cc.Exec(ctx, "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	cc.Close()

	if mock.verifyCount.Load() < 1 {
		t.Errorf("explicit On should override negative detection, got %d", mock.verifyCount.Load())
	}
}

func TestAggressiveVerify_ExplicitOffBeatsAutoPositiveDetection(t *testing.T) {
	cc, mock := setupAggressiveVerifyWrapped(t, "tcp://prec2")
	mock.setDetection(true, nil) // detection would say on
	cc.SetAggressiveVerify(AggressiveVerifyOff)
	ctx := context.Background()

	if _, err := cc.Exec(ctx, "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	cc.Close()

	if mock.verifyCount.Load() != 0 {
		t.Errorf("explicit Off should override positive detection, got %d", mock.verifyCount.Load())
	}
}

// --- WithAggressiveVerify GoldLapel option ---

func TestAggressiveVerify_WithAggressiveVerifyOption(t *testing.T) {
	gl := &GoldLapel{}
	WithAggressiveVerify(AggressiveVerifyOn).applyStart(gl)
	if !gl.aggressiveVerifySet {
		t.Error("aggressiveVerifySet should be true after option applied")
	}
	if gl.aggressiveVerify != AggressiveVerifyOn {
		t.Errorf("expected AggressiveVerifyOn, got %v", gl.aggressiveVerify)
	}
}

func TestAggressiveVerify_ModeStrings(t *testing.T) {
	cases := map[AggressiveVerifyMode]string{
		AggressiveVerifyAuto: "auto",
		AggressiveVerifyOn:   "on",
		AggressiveVerifyOff:  "off",
		AggressiveVerifyMode(99): "unknown",
	}
	for mode, want := range cases {
		got := mode.String()
		if got != want {
			t.Errorf("mode %d: got %q, want %q", mode, got, want)
		}
	}
}

// --- Race-clean concurrent DML + introspection ---

func TestAggressiveVerify_RaceCleanUnderConcurrentDML(t *testing.T) {
	cc, mock := setupAggressiveVerifyWrapped(t, "tcp://race")
	mock.setDetection(true, nil)
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
				_ = cc.aggressiveVerifyEnabled(ctx)
			}
		}()
	}
	wg.Wait()
	cc.Close()

	// At least one verify should have fired (writes always trigger
	// the schedule + dirty mark; the in-flight gate may coalesce many
	// onto a single verify, but the count must be > 0).
	if mock.verifyCount.Load() == 0 {
		t.Error("expected non-zero verify count under concurrent DML")
	}
}

func TestAggressiveVerify_DetectionCacheRaceCleanFirstUse(t *testing.T) {
	// Concurrent first-use of multiple CachedConns against the same
	// upstream URL must race-cleanly through the detection probe.
	// sync.Map.LoadOrStore semantics let two probes race on first
	// access; we verify the result is sane (some probes ran, all
	// CachedConns end up with a consistent verdict).
	ResetNativeCache()
	ResetAggressiveVerifyDetectionCache()
	SetAggressiveVerifyActive(false)
	cache := GetNativeCache()
	cache.mu.Lock()
	cache.invConnected = true
	cache.mu.Unlock()

	const upstream = "tcp://race-shared"
	const N = 8
	mocks := make([]*aggressiveVerifyMockQuerier, N)
	ccs := make([]*CachedConn, N)
	for i := 0; i < N; i++ {
		mocks[i] = newAggressiveVerifyMockQuerier()
		mocks[i].setDetection(true, nil)
		ccs[i] = &CachedConn{real: mocks[i], cache: cache}
		ccs[i].SetAggressiveVerifyUpstream(upstream)
	}
	t.Cleanup(func() {
		for _, cc := range ccs {
			cc.Close()
		}
		ResetAggressiveVerifyDetectionCache()
	})

	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ccs[idx].Exec(context.Background(), "INSERT INTO t VALUES (1)")
		}(i)
	}
	wg.Wait()

	// At least one probe must have fired; race-cleanness is asserted
	// by -race itself (no error from the race detector implies pass).
	totalProbes := int64(0)
	for _, m := range mocks {
		totalProbes += m.detectionCount.Load()
	}
	if totalProbes == 0 {
		t.Error("expected at least one detection probe to fire")
	}
	if totalProbes > N {
		t.Errorf("probe count exceeds CachedConn count: %d > %d", totalProbes, N)
	}
}

// --- License-payload setter ---

func TestAggressiveVerify_LicensePayloadSetterAndGetter(t *testing.T) {
	t.Cleanup(func() { SetAggressiveVerifyActive(false) })
	if AggressiveVerifyActive() {
		t.Error("default should be false")
	}
	SetAggressiveVerifyActive(true)
	if !AggressiveVerifyActive() {
		t.Error("setter→getter round trip failed")
	}
	SetAggressiveVerifyActive(false)
	if AggressiveVerifyActive() {
		t.Error("setter→getter (false) round trip failed")
	}
}

// --- Detection cache reset clears memoised verdict ---

func TestAggressiveVerify_ResetDetectionCacheClearsVerdict(t *testing.T) {
	ResetNativeCache()
	ResetAggressiveVerifyDetectionCache()
	SetAggressiveVerifyActive(false)
	cache := GetNativeCache()
	cache.mu.Lock()
	cache.invConnected = true
	cache.mu.Unlock()

	const upstream = "tcp://clearable"

	mock1 := newAggressiveVerifyMockQuerier()
	mock1.setDetection(true, nil)
	cc1 := &CachedConn{real: mock1, cache: cache}
	cc1.SetAggressiveVerifyUpstream(upstream)
	t.Cleanup(func() { cc1.Close() })
	cc1.Exec(context.Background(), "INSERT INTO t VALUES (1)")

	if mock1.detectionCount.Load() != 1 {
		t.Fatalf("baseline: expected 1 probe, got %d", mock1.detectionCount.Load())
	}

	// Reset the cache; a fresh CachedConn should re-probe.
	ResetAggressiveVerifyDetectionCache()

	mock2 := newAggressiveVerifyMockQuerier()
	mock2.setDetection(false, nil)
	cc2 := &CachedConn{real: mock2, cache: cache}
	cc2.SetAggressiveVerifyUpstream(upstream)
	t.Cleanup(func() { cc2.Close() })
	cc2.Exec(context.Background(), "INSERT INTO t VALUES (1)")

	if mock2.detectionCount.Load() != 1 {
		t.Errorf("post-reset: expected fresh probe, got %d", mock2.detectionCount.Load())
	}
}
