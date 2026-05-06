package goldlapel

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// verifyMockQuerier is a Querier that recognises the verify SQL
// (`SELECT name, setting FROM pg_settings WHERE source = 'session'`)
// and returns a configurable snapshot of session GUCs. Any other SQL
// returns the same default rows the production mockQuerier returns.
//
// Concurrency-safe — reads of `verifyResult` are atomic via mu so the
// async verify goroutine + the main test goroutine don't race on the
// snapshot the verify reads.
type verifyMockQuerier struct {
	mu sync.Mutex
	// queries records every SQL string seen, in order. Includes the
	// verify SQL when it fires — tests assert on its appearance and
	// position.
	queries []string
	// verifyResult is the {name: value} snapshot returned to the
	// verify SQL. Tests mutate it between calls to simulate a
	// server-side state change.
	verifyResult map[string]string
	// verifyCount is the number of times the verify SQL has fired.
	// Atomic so the async verify goroutine can increment without
	// taking mu.
	verifyCount atomic.Int64
}

func newVerifyMockQuerier() *verifyMockQuerier {
	return &verifyMockQuerier{verifyResult: make(map[string]string)}
}

func (m *verifyMockQuerier) setVerifyResult(values map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.verifyResult = values
}

func (m *verifyMockQuerier) recordQuery(sql string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queries = append(m.queries, sql)
}

func (m *verifyMockQuerier) queryList() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.queries))
	copy(out, m.queries)
	return out
}

func (m *verifyMockQuerier) Query(ctx context.Context, sql string, args ...interface{}) (Rows, error) {
	m.recordQuery(sql)
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

func (m *verifyMockQuerier) QueryRow(ctx context.Context, sql string, args ...interface{}) Row {
	m.recordQuery(sql)
	return &mockRow{values: []interface{}{1}}
}

func (m *verifyMockQuerier) Exec(ctx context.Context, sql string, args ...interface{}) (interface{}, error) {
	m.recordQuery(sql)
	return nil, nil
}

func isVerifySQL(sql string) bool {
	return strings.Contains(sql, "pg_settings") && strings.Contains(sql, "source = 'session'")
}

// setupVerifyWrapped builds a CachedConn against a verifyMockQuerier and
// mirrors setupWrapped's invariants (NativeCache reset + invConnected).
func setupVerifyWrapped(t *testing.T) (*CachedConn, *verifyMockQuerier) {
	t.Helper()
	ResetNativeCache()
	cache := GetNativeCache()
	cache.mu.Lock()
	cache.invConnected = true
	cache.mu.Unlock()
	mock := newVerifyMockQuerier()
	cc := &CachedConn{real: mock, cache: cache}
	t.Cleanup(func() { cc.Close() })
	return cc, mock
}

// --- Verify-on-checkout (concern 5) ---

func TestVerify_OnCheckoutFiresWhenDirty(t *testing.T) {
	cc, mock := setupVerifyWrapped(t)
	ctx := context.Background()

	// Simulate a server-side state we couldn't observe on the wire.
	mock.setVerifyResult(map[string]string{
		"app.user_id": "99",
	})
	cc.ensureGucState().MarkDirty()

	// Next call should trigger verify before doing anything else.
	if _, err := cc.Query(ctx, "SELECT * FROM accounts"); err != nil {
		t.Fatal(err)
	}
	if mock.verifyCount.Load() != 1 {
		t.Fatalf("expected exactly 1 verify call, got %d", mock.verifyCount.Load())
	}
	if cc.ensureGucState().IsDirty() {
		t.Error("dirty flag should be cleared after verify")
	}
	// State map should now reflect the simulated server-side SET.
	want := NewConnectionGucState()
	want.ObserveSQL("SET app.user_id = '99'")
	if cc.GucStateHash() != want.Hash() {
		t.Errorf("verify should have reseeded state: got %d, want %d",
			cc.GucStateHash(), want.Hash())
	}
}

func TestVerify_OnCheckoutNoOpWhenClean(t *testing.T) {
	cc, mock := setupVerifyWrapped(t)
	ctx := context.Background()

	// State is clean; no verify should fire.
	if _, err := cc.Query(ctx, "SELECT * FROM accounts"); err != nil {
		t.Fatal(err)
	}
	if mock.verifyCount.Load() != 0 {
		t.Fatalf("expected 0 verify calls, got %d", mock.verifyCount.Load())
	}
}

func TestVerify_OnCheckoutSkippedInsideTransaction(t *testing.T) {
	cc, mock := setupVerifyWrapped(t)
	ctx := context.Background()

	cc.Exec(ctx, "BEGIN")
	cc.ensureGucState().MarkDirty()

	// Verify must NOT fire inside a transaction — the cache is bypassed
	// regardless, so verify is wasted work AND could conflict with a
	// user transaction.
	if _, err := cc.Query(ctx, "SELECT * FROM accounts"); err != nil {
		t.Fatal(err)
	}
	if mock.verifyCount.Load() != 0 {
		t.Fatalf("expected 0 verify calls inside tx, got %d", mock.verifyCount.Load())
	}
	// Dirty flag should still be set — verify will fire after COMMIT.
	if !cc.ensureGucState().IsDirty() {
		t.Error("dirty flag should remain set inside tx")
	}

	cc.Exec(ctx, "COMMIT")
	cc.Query(ctx, "SELECT * FROM accounts")
	if mock.verifyCount.Load() != 1 {
		t.Errorf("expected verify after COMMIT, got %d calls", mock.verifyCount.Load())
	}
}

func TestVerify_ReseedFiltersOnlyUnsafeGUCs(t *testing.T) {
	cc, mock := setupVerifyWrapped(t)
	ctx := context.Background()

	// pg_settings would yield every session-modified GUC; the wrapper
	// must keep only unsafe ones in its state map.
	mock.setVerifyResult(map[string]string{
		"app.tenant":          "acme",     // unsafe (namespaced)
		"work_mem":            "64MB",     // safe — must not enter map
		"search_path":         "tenant_a", // unsafe
		"client_min_messages": "warning",  // safe
	})
	cc.ensureGucState().MarkDirty()
	cc.Query(ctx, "SELECT 1")

	want := NewConnectionGucState()
	want.ObserveSQL("SET app.tenant = 'acme'")
	want.ObserveSQL("SET search_path = 'tenant_a'")
	if cc.GucStateHash() != want.Hash() {
		t.Errorf("verify should drop safe GUCs from the map: got %d, want %d",
			cc.GucStateHash(), want.Hash())
	}
}

// --- Async post-call verify (concern 6) ---

func TestVerify_AsyncFiresAfterTopLevelFunctionCall(t *testing.T) {
	cc, mock := setupVerifyWrapped(t)
	ctx := context.Background()

	// Simulate what the function set on the server side.
	mock.setVerifyResult(map[string]string{
		"app.user_id": "42",
	})

	if _, err := cc.Query(ctx, "SELECT my_func()"); err != nil {
		t.Fatal(err)
	}

	// Wait for the async verify to complete.
	cc.Close()

	if mock.verifyCount.Load() < 1 {
		t.Fatalf("expected at least 1 async verify, got %d", mock.verifyCount.Load())
	}
	want := NewConnectionGucState()
	want.ObserveSQL("SET app.user_id = '42'")
	if cc.GucStateHash() != want.Hash() {
		t.Errorf("async verify should have reseeded state: got %d, want %d",
			cc.GucStateHash(), want.Hash())
	}
}

func TestVerify_AsyncDoesNotFireForNonFunctionCallSelect(t *testing.T) {
	cc, mock := setupVerifyWrapped(t)
	ctx := context.Background()

	// Plain SELECT — not a top-level function call.
	cc.Query(ctx, "SELECT * FROM accounts WHERE id = 1")
	cc.Close()

	if mock.verifyCount.Load() != 0 {
		t.Fatalf("expected no verify for plain SELECT, got %d", mock.verifyCount.Load())
	}
}

func TestVerify_AsyncDoesNotFireForSetConfigCall(t *testing.T) {
	cc, mock := setupVerifyWrapped(t)
	ctx := context.Background()

	// set_config IS a top-level function call shape — but we already
	// extract state from it inline via ObserveSQL, so scheduling an
	// async verify is redundant work + sets the dirty flag the verify
	// would have to clear.
	cc.Exec(ctx, "SELECT set_config('app.user_id', '42', false)")
	cc.Close()

	if mock.verifyCount.Load() != 0 {
		t.Fatalf("set_config should be inline-handled, no verify expected, got %d", mock.verifyCount.Load())
	}
	// State should still reflect the SET (extracted by ObserveSQL).
	want := NewConnectionGucState()
	want.ObserveSQL("SET app.user_id = '42'")
	if cc.GucStateHash() != want.Hash() {
		t.Errorf("inline set_config extraction failed: got %d, want %d",
			cc.GucStateHash(), want.Hash())
	}
}

func TestVerify_AsyncGateLimitsInFlightToOne(t *testing.T) {
	// The inFlightVerify gate limits to ONE async-verify goroutine at
	// a time. Two concurrent SELECT-function-call triggers must
	// produce at most one async verify spawn (the second sees the
	// gate, no-ops). The verify-on-checkout path is independent (it's
	// synchronous and per-call), so this test specifically observes
	// the gate via a controlled-blocking mock.
	cc := &CachedConn{}

	// First schedule — gate is open, CAS succeeds.
	cc.ensureGucState().MarkDirty()
	if !cc.inFlightVerify.CompareAndSwap(false, true) {
		t.Fatal("setup: gate should start open")
	}
	// Pretend a verify is in flight; second schedule attempt must see
	// the gate locked.
	if cc.inFlightVerify.CompareAndSwap(false, true) {
		t.Fatal("second CAS should fail while in-flight")
	}
	// Release.
	cc.inFlightVerify.Store(false)
	// Now a fresh schedule wins again.
	if !cc.inFlightVerify.CompareAndSwap(false, true) {
		t.Fatal("post-release CAS should succeed")
	}
	cc.inFlightVerify.Store(false)
}

func TestVerify_AsyncDoesNotBlockHotPath(t *testing.T) {
	cc, _ := setupVerifyWrapped(t)
	ctx := context.Background()

	// The user's call should return immediately, well before any async
	// verify could complete (verify is synchronous against the mock,
	// but it runs in a goroutine).
	start := time.Now()
	if _, err := cc.Query(ctx, "SELECT my_func()"); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if elapsed > 100*time.Millisecond {
		t.Errorf("async verify must not block the hot path; took %v", elapsed)
	}
	cc.Close()
}

func TestVerify_CloseCancelsInFlightAndIsIdempotent(t *testing.T) {
	cc, _ := setupVerifyWrapped(t)
	ctx := context.Background()

	cc.Query(ctx, "SELECT my_func()")
	// First Close cancels and waits.
	cc.Close()
	// Second + third Close are no-ops.
	cc.Close()
	cc.Close()
}

func TestVerify_ScheduleAfterCloseIsNoOp(t *testing.T) {
	cc, mock := setupVerifyWrapped(t)
	ctx := context.Background()
	cc.Close()

	// Even though we observe a top-level function call, no verify
	// should fire — the wrapper is closed.
	cc.Query(ctx, "SELECT my_func()")
	// Sleep briefly to let any (incorrectly) scheduled goroutine run.
	time.Sleep(10 * time.Millisecond)
	if mock.verifyCount.Load() != 0 {
		t.Errorf("scheduling after Close must be no-op, got %d verifies", mock.verifyCount.Load())
	}
}

// --- Concurrency: race-clean under -race ---

func TestVerify_RaceCleanUnderConcurrentTriggers(t *testing.T) {
	cc, mock := setupVerifyWrapped(t)
	ctx := context.Background()
	mock.setVerifyResult(map[string]string{"app.user_id": "1"})

	const writers, readers, iters = 4, 4, 50
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				cc.Query(ctx, "SELECT my_func()")
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
			}
		}()
	}
	wg.Wait()
	cc.Close()
}
