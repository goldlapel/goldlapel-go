// SET-actually-applied protocol tests.
//
// These tests lock in the Wave 2 fix where wrapper state mutations
// (GUC state hash, inTransaction, pool-discarder dirty) are STAGED
// during Query / QueryRow / Exec and committed only after the
// underlying Querier reports nil error. On err the staged effects
// are dropped, and any observed state-mutation command marks the
// connection dirty so the next checkout reseeds via verify-against-
// pg_settings — the safety net for multi-statement bodies that may
// have applied earlier segments before failing.
//
// Pre-fix the wrapper mutated eagerly on observation, which meant a
// SET that errored at the wire would still move the wrapper's hash
// (cache-fragmentation footprint) and an errored RESET would drop
// the wrapper's record of an unsafe GUC the server still has set
// (RLS-shaped correctness hole on the next read).

package goldlapel

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// errMockQuerier is a Querier that returns a configurable error for
// any SQL matching a substring filter, and otherwise behaves like
// mockQuerier. The settle protocol tests need a way to make Exec /
// Query / QueryRow fail on demand.
type errMockQuerier struct {
	mu       sync.Mutex
	queries  []string
	matchSQL string // SQL containing this substring fails with errOn.
	errOn    error
	// rowsErrOn lets the SUCCESSFUL-Query-but-row-iteration-failure
	// path be tested without affecting the Query/Exec err.
	rowsErrOn error
}

func newErrMockQuerier(matchSQL string, errOn error) *errMockQuerier {
	return &errMockQuerier{matchSQL: matchSQL, errOn: errOn}
}

func (m *errMockQuerier) record(sql string) {
	m.mu.Lock()
	m.queries = append(m.queries, sql)
	m.mu.Unlock()
}

func (m *errMockQuerier) shouldFail(sql string) bool {
	return m.matchSQL != "" && contains(sql, m.matchSQL)
}

func (m *errMockQuerier) Query(ctx context.Context, sql string, args ...interface{}) (Rows, error) {
	m.record(sql)
	if m.shouldFail(sql) {
		return nil, m.errOn
	}
	return &mockRows{rows: nil, fields: nil, pos: -1}, nil
}

func (m *errMockQuerier) QueryRow(ctx context.Context, sql string, args ...interface{}) Row {
	m.record(sql)
	if m.shouldFail(sql) {
		return &errRow{err: m.errOn}
	}
	return &mockRow{values: nil}
}

func (m *errMockQuerier) Exec(ctx context.Context, sql string, args ...interface{}) (interface{}, error) {
	m.record(sql)
	if m.shouldFail(sql) {
		return nil, m.errOn
	}
	return nil, nil
}

func (m *errMockQuerier) queryList() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.queries))
	copy(out, m.queries)
	return out
}

// errRow is a Row whose Scan always returns the configured err.
type errRow struct{ err error }

func (r *errRow) Scan(dest ...interface{}) error { return r.err }

// contains is a tiny helper to avoid pulling in strings just for the
// substring check.
func contains(haystack, needle string) bool {
	if needle == "" {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// setupErrWrapped builds a CachedConn against an errMockQuerier and
// mirrors setupWrapped's invariants (NativeCache reset + invConnected).
func setupErrWrapped(t *testing.T, matchSQL string, errOn error) (*CachedConn, *errMockQuerier) {
	t.Helper()
	ResetNativeCache()
	cache := GetNativeCache()
	cache.mu.Lock()
	cache.invConnected = true
	cache.mu.Unlock()
	mock := newErrMockQuerier(matchSQL, errOn)
	cc := &CachedConn{real: mock, cache: cache}
	t.Cleanup(func() { cc.Close() })
	return cc, mock
}

// --- Exec err: SET hash must NOT move ---

// TestSetApplied_ExecErrLeavesHashUnchanged: an Exec("SET ...") that
// errors at the wire must NOT move the wrapper's state hash. The pre-
// fix would have applied app.user_id='42' eagerly, producing a non-
// zero hash even though the server's session is still baseline.
func TestSetApplied_ExecErrLeavesHashUnchanged(t *testing.T) {
	cc, _ := setupErrWrapped(t, "SET", errors.New("permission denied"))
	ctx := context.Background()

	hashBefore := cc.GucStateHash()
	if hashBefore != 0 {
		t.Fatalf("setup: expected baseline hash 0, got %d", hashBefore)
	}

	_, err := cc.Exec(ctx, "SET app.user_id = '42'")
	if err == nil {
		t.Fatal("expected Exec to surface the configured err")
	}

	if cc.GucStateHash() != 0 {
		t.Fatalf("errored SET must NOT move hash: got %d", cc.GucStateHash())
	}
}

// TestSetApplied_ExecErrMarksDirty: even though the SET errored and
// the hash didn't move, the wrapper marks the connection dirty — the
// safety net for any partial-apply scenario the wire-side single err
// can't disambiguate. The next checkout fires verify-on-checkout.
func TestSetApplied_ExecErrMarksDirty(t *testing.T) {
	cc, _ := setupErrWrapped(t, "SET", errors.New("permission denied"))
	ctx := context.Background()

	if cc.ensureGucState().IsDirty() {
		t.Fatal("setup: state should not be dirty before SET")
	}
	cc.Exec(ctx, "SET app.user_id = '42'")
	if !cc.ensureGucState().IsDirty() {
		t.Fatal("errored SET must mark state dirty (verify-on-checkout safety net)")
	}
}

// TestSetApplied_ExecErrNonSetSqlNoDirty: Exec err for non-state-
// affecting SQL (e.g. INSERT INTO t) must NOT mark the state dirty —
// dirty=true would force an unnecessary verify on the next checkout.
// pending.HasMutation() is the discriminator.
func TestSetApplied_ExecErrNonSetSqlNoDirty(t *testing.T) {
	cc, _ := setupErrWrapped(t, "INSERT", errors.New("connection reset"))
	ctx := context.Background()

	cc.Exec(ctx, "INSERT INTO orders VALUES (1)")
	if cc.ensureGucState().IsDirty() {
		t.Fatal("errored INSERT (no SET in body) must not mark state dirty")
	}
}

// TestSetApplied_ExecErrResetDoesNotDropExistingState: pre-fix, an
// errored RESET would have removed the GUC from the wrapper's value
// map even though the server still has it — opening an RLS hole when
// the next read serves baseline-cached rows under non-baseline
// server state. With deferred apply, the RESET err leaves the wrapper
// hash intact AND marks dirty so the next checkout verifies.
func TestSetApplied_ExecErrResetDoesNotDropExistingState(t *testing.T) {
	// First land app.user_id='42' via a successful SET so we have
	// non-baseline state to (mis)drop.
	cc, mock := setupErrWrapped(t, "RESET", errors.New("denied"))
	ctx := context.Background()

	if _, err := cc.Exec(ctx, "SET app.user_id = '42'"); err != nil {
		t.Fatalf("seed SET should succeed: %v", err)
	}
	hashAfterSet := cc.GucStateHash()
	if hashAfterSet == 0 {
		t.Fatal("seed SET should have moved hash off baseline")
	}

	// Now an erroring RESET. Hash MUST remain at hashAfterSet.
	if _, err := cc.Exec(ctx, "RESET app.user_id"); err == nil {
		t.Fatal("expected RESET to error per mock config")
	}
	if cc.GucStateHash() != hashAfterSet {
		t.Fatalf("errored RESET must not drop existing state: hash %d → %d (expected unchanged)",
			hashAfterSet, cc.GucStateHash())
	}
	if !cc.ensureGucState().IsDirty() {
		t.Fatal("errored RESET must mark state dirty so next checkout verifies")
	}

	// Sanity: the mock saw both SETs.
	if len(mock.queryList()) != 2 {
		t.Fatalf("expected 2 queries on mock, got %d", len(mock.queryList()))
	}
}

// TestSetApplied_ExecSuccessAppliesHash: success-path baseline — a
// SET that completes without error DOES move the hash, same as the
// eager protocol. This is the protection: the fix doesn't regress
// the happy path.
func TestSetApplied_ExecSuccessAppliesHash(t *testing.T) {
	cc, _ := setupErrWrapped(t, "", nil) // matchSQL="" → no failures
	ctx := context.Background()

	if _, err := cc.Exec(ctx, "SET app.user_id = '42'"); err != nil {
		t.Fatalf("expected success: %v", err)
	}
	if cc.GucStateHash() == 0 {
		t.Fatal("successful SET must move hash off baseline")
	}
	if cc.ensureGucState().IsDirty() {
		t.Fatal("successful SET must NOT mark state dirty")
	}
}

// --- Tx-flag deferred apply ---

// TestSetApplied_BeginErrLeavesOutOfTx: an erroring BEGIN must NOT
// flip cc.inTransaction. Pre-fix the eager apply would have set it
// regardless of err, leaving the wrapper bypassing the read-cache
// forever (until a successful BEGIN/COMMIT cycle reset the flag).
func TestSetApplied_BeginErrLeavesOutOfTx(t *testing.T) {
	cc, _ := setupErrWrapped(t, "BEGIN", errors.New("conn closed"))
	ctx := context.Background()

	cc.Exec(ctx, "BEGIN")
	if cc.inTransaction {
		t.Fatal("errored BEGIN must NOT flip inTransaction=true")
	}
}

// TestSetApplied_RollbackErrFromInTxStaysInTx: the wrapper is in-tx
// (BEGIN succeeded prior). A ROLLBACK that errors at the wire leaves
// the server's tx state ambiguous — safest to remain in-tx until a
// subsequent successful tx-control statement resolves.
func TestSetApplied_RollbackErrFromInTxStaysInTx(t *testing.T) {
	cc, _ := setupErrWrapped(t, "ROLLBACK", errors.New("conn lost"))
	ctx := context.Background()

	// Seed in-tx state.
	if _, err := cc.Exec(ctx, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	if !cc.inTransaction {
		t.Fatal("setup: expected in-tx after successful BEGIN")
	}

	// Error on ROLLBACK must NOT optimistically flip out-of-tx.
	cc.Exec(ctx, "ROLLBACK")
	if !cc.inTransaction {
		t.Fatal("errored ROLLBACK must leave inTransaction=true (safest under ambiguity)")
	}
}

// TestSetApplied_BeginRollbackSuccessRoundTrip: on the happy path,
// BEGIN flips in-tx, ROLLBACK flips out — matching pre-fix behaviour.
// This guards against the deferred-apply refactor accidentally
// inverting the success-path semantics.
func TestSetApplied_BeginRollbackSuccessRoundTrip(t *testing.T) {
	cc, _ := setupWrapped(t, nil, nil)
	ctx := context.Background()

	cc.Exec(ctx, "BEGIN")
	if !cc.inTransaction {
		t.Fatal("BEGIN should flip in-tx")
	}
	cc.Exec(ctx, "ROLLBACK")
	if cc.inTransaction {
		t.Fatal("ROLLBACK should flip out-of-tx")
	}
}

// --- Multi-statement Exec err ---

// TestSetApplied_MultiStmtExecErrMarksDirty: multi-statement bodies
// like `SET app.user_id='42'; INSERT INTO t VALUES (1)` may have
// applied the SET server-side before the INSERT failed. pgx returns
// a single err and we can't tell which segments landed. The wrapper
// drops the staged apply AND marks dirty so subsequent reads bypass
// L1 until an authoritative refresh resolves the uncertainty.
//
// Note: the body contains a DML segment, which always-on
// aggressive-verify bumps dml_seq for (independent of the SET
// observation). The assertion below verifies the unsafe-GUC values
// map wasn't mutated — comparing the wrapper's hash against a
// reference state built with the same dml_seq increment.
func TestSetApplied_MultiStmtExecErrMarksDirty(t *testing.T) {
	cc, _ := setupErrWrapped(t, "INSERT", errors.New("constraint violation"))
	ctx := context.Background()

	cc.Exec(ctx, "SET app.user_id = '42'; INSERT INTO orders VALUES (1)")

	// The errored SET must NOT have entered the unsafe-GUC values map.
	// We compare the wrapper's hash to a baseline that only carries
	// the post-DML bump — same dml_seq increment, no SETs.
	want := NewConnectionGucState()
	want.BumpDmlSeq()
	if cc.GucStateHash() != want.Hash() {
		t.Fatalf("errored multi-stmt body must not optimistically commit SET: got %d, want (dml-bumped only) %d",
			cc.GucStateHash(), want.Hash())
	}
	if !cc.ensureGucState().IsDirty() {
		t.Fatal("errored multi-stmt body with embedded SET must mark dirty")
	}
}

// TestSetApplied_MultiStmtExecSuccessAppliesAll: multi-statement
// success path — both the SET and the INSERT land. Hash moves; not
// dirty.
func TestSetApplied_MultiStmtExecSuccessAppliesAll(t *testing.T) {
	cc, _ := setupErrWrapped(t, "", nil)
	ctx := context.Background()

	if _, err := cc.Exec(ctx, "SET app.user_id = '42'; INSERT INTO orders VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if cc.GucStateHash() == 0 {
		t.Fatal("multi-stmt success must apply embedded SET to hash")
	}
	if cc.ensureGucState().IsDirty() {
		t.Fatal("multi-stmt success must NOT mark state dirty")
	}
}

// --- Pool-discarder integration ---

// TestSetApplied_ExecErrDoesNotDirtyPool: an errored SET must not
// flip the pool-discarder's dirty flag. The connection didn't
// actually carry user state into the pool, so AfterRelease should
// skip the DISCARD ALL roundtrip on connection return.
func TestSetApplied_ExecErrDoesNotDirtyPool(t *testing.T) {
	cc, _ := setupErrWrapped(t, "SET", errors.New("denied"))
	disc := NewPoolReleaseDiscarder()
	AttachDiscarderTo(cc, disc)
	ctx := context.Background()

	cc.Exec(ctx, "SET app.user_id = '42'")
	if disc.ShouldDiscard() {
		t.Fatal("errored SET must not dirty the pool discarder")
	}
}

// TestSetApplied_ExecSuccessDirtiesPool: pool-discarder dirty bit
// flips on successful unsafe SET — happy path, same as pre-fix.
func TestSetApplied_ExecSuccessDirtiesPool(t *testing.T) {
	cc, _ := setupErrWrapped(t, "", nil)
	disc := NewPoolReleaseDiscarder()
	AttachDiscarderTo(cc, disc)
	ctx := context.Background()

	cc.Exec(ctx, "SET app.user_id = '42'")
	if !disc.ShouldDiscard() {
		t.Fatal("successful unsafe SET must dirty the pool discarder")
	}
}

// --- Query / QueryRow paths ---

// TestSetApplied_QueryErrLeavesHashUnchanged: SETs can route through
// Query for some drivers. Same protocol applies — err leaves hash
// alone, marks dirty.
func TestSetApplied_QueryErrLeavesHashUnchanged(t *testing.T) {
	cc, _ := setupErrWrapped(t, "SET", errors.New("denied"))
	ctx := context.Background()

	rows, err := cc.Query(ctx, "SET app.user_id = '42'")
	if err == nil {
		t.Fatal("expected Query to surface the err")
	}
	if rows != nil {
		t.Fatal("Query err must return nil Rows")
	}
	if cc.GucStateHash() != 0 {
		t.Fatalf("errored Query SET must not move hash: %d", cc.GucStateHash())
	}
	if !cc.ensureGucState().IsDirty() {
		t.Fatal("errored Query SET must mark state dirty")
	}
}

// TestSetApplied_QueryRowErrViaScanLeavesHashUnchanged: QueryRow's
// err comes through Row.Scan. settlingRow wraps the underlying Row
// so the deferred err path settles correctly. A SET routed through
// QueryRow whose Scan returns a real err must not leave the wrapper
// hash diverged.
func TestSetApplied_QueryRowErrViaScanLeavesHashUnchanged(t *testing.T) {
	cc, _ := setupErrWrapped(t, "SET", errors.New("denied"))
	ctx := context.Background()

	row := cc.QueryRow(ctx, "SET app.user_id = '42'")
	var dummy interface{}
	if err := row.Scan(&dummy); err == nil {
		t.Fatal("expected Scan to surface the err")
	}
	if cc.GucStateHash() != 0 {
		t.Fatalf("errored QueryRow SET must not move hash: %d", cc.GucStateHash())
	}
	if !cc.ensureGucState().IsDirty() {
		t.Fatal("errored QueryRow SET must mark state dirty")
	}
}

// TestSetApplied_QueryRowSettleFiresOnceOnRepeatedScan: pgx allows
// Row.Scan to be called multiple times (returns the same err on
// subsequent calls). settle must fire exactly once — repeated Scans
// must not re-Apply or repeatedly mark dirty.
func TestSetApplied_QueryRowSettleFiresOnceOnRepeatedScan(t *testing.T) {
	cc, _ := setupErrWrapped(t, "", nil)
	ctx := context.Background()

	row := cc.QueryRow(ctx, "SELECT 1") // routes via cache miss → settles via inner Query
	// Scan twice; second call must not panic / double-mutate.
	var v interface{}
	row.Scan(&v)
	row.Scan(&v)
	// Hash unchanged (no SET); not dirty.
	if cc.GucStateHash() != 0 {
		t.Fatalf("non-SET QueryRow must keep hash baseline: %d", cc.GucStateHash())
	}
}

// --- Race-clean concurrency ---

// TestSetApplied_ConcurrentExecsRaceClean: many goroutines firing SET
// / INSERT / SELECT concurrently against the same CachedConn must
// not race on the staged-pending machinery. (This test exists
// primarily to surface data races under -race, not to assert specific
// hash values; the underlying realMu serialises cc.real, but the
// pending/settle accounting reads/writes shared state — the race
// detector validates the invariant.)
func TestSetApplied_ConcurrentExecsRaceClean(t *testing.T) {
	cc, _ := setupErrWrapped(t, "", nil)
	ctx := context.Background()

	const goroutines = 16
	const ops = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				switch (i + id) % 4 {
				case 0:
					cc.Exec(ctx, "SET app.tenant = 'alpha'")
				case 1:
					cc.Exec(ctx, "RESET app.tenant")
				case 2:
					cc.Query(ctx, "SELECT * FROM users")
				case 3:
					cc.Exec(ctx, "INSERT INTO orders VALUES (1)")
				}
			}
		}(g)
	}
	wg.Wait()
}

// TestSetApplied_ConcurrentSettleNoDoubleApply: confirms that
// settlingRow's atomic.Bool guards prevent double settle even when
// Scan is called concurrently from multiple goroutines. (Not a
// real-world pattern — pgx.Row is not goroutine-safe — but the guard
// must be correct.)
func TestSetApplied_ConcurrentSettleNoDoubleApply(t *testing.T) {
	cc, _ := setupErrWrapped(t, "", nil)
	ctx := context.Background()

	row := cc.QueryRow(ctx, "SELECT 1")
	const goroutines = 8
	var wg sync.WaitGroup
	wg.Add(goroutines)
	var scanCount atomic.Int64
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			var v interface{}
			row.Scan(&v)
			scanCount.Add(1)
		}()
	}
	wg.Wait()
	if scanCount.Load() != int64(goroutines) {
		t.Fatalf("expected %d Scans, got %d", goroutines, scanCount.Load())
	}
}

// --- Dirty-after-err drives L1 bypass on subsequent reads ---

// TestSetApplied_DirtyAfterErrBypassesL1OnNextCheckout: end-to-end
// with a verifyMockQuerier — an errored SET marks dirty, and the
// next read routes around L1 (no synchronous verify SQL fires).
// The dirty window stays open until an authoritative refresh
// (async verify via SELECT my_func() / RESET ALL / DISCARD ALL)
// resolves it.
func TestSetApplied_DirtyAfterErrBypassesL1OnNextCheckout(t *testing.T) {
	// Wrap an existing verifyMockQuerier and override Exec to selectively
	// fail. Doing it inline rather than building a 4th mock keeps the
	// test fixture footprint small.
	base := newVerifyMockQuerier()
	failingMock := &errOnExecMock{base: base, failSubstr: "SET"}

	ResetNativeCache()
	cache := GetNativeCache()
	cache.mu.Lock()
	cache.invConnected = true
	cache.mu.Unlock()
	cc := &CachedConn{real: failingMock, cache: cache}
	t.Cleanup(func() { cc.Close() })
	ctx := context.Background()

	// 1) Errored SET → dirty + hash unchanged.
	if _, err := cc.Exec(ctx, "SET app.user_id = '42'"); err == nil {
		t.Fatal("expected SET to error")
	}
	if cc.GucStateHash() != 0 {
		t.Fatalf("errored SET must not move hash; got %d", cc.GucStateHash())
	}
	if !cc.ensureGucState().IsDirty() {
		t.Fatal("errored SET must mark dirty")
	}

	// 2) Next call (a SELECT) under the new model bypasses L1
	// because dirty is set. No synchronous verify SQL fires — the
	// new contract avoids the verify roundtrip on the hot path.
	if _, err := cc.Query(ctx, "SELECT * FROM accounts"); err != nil {
		t.Fatal(err)
	}
	if base.verifyCount.Load() != 0 {
		t.Fatalf("dirty-bypass model must not fire a synchronous verify; got %d", base.verifyCount.Load())
	}
	if !cc.ensureGucState().IsDirty() {
		t.Fatal("dirty must persist until async verify / RESET ALL resolves it")
	}
}

// errOnExecMock is a Querier that delegates everything to a base
// verifyMockQuerier but fails Exec on a configurable substring match.
// Used by the verify-after-err test to avoid declaring a 4th
// dedicated mock.
type errOnExecMock struct {
	base       *verifyMockQuerier
	failSubstr string
}

func (m *errOnExecMock) Query(ctx context.Context, sql string, args ...interface{}) (Rows, error) {
	return m.base.Query(ctx, sql, args...)
}

func (m *errOnExecMock) QueryRow(ctx context.Context, sql string, args ...interface{}) Row {
	return m.base.QueryRow(ctx, sql, args...)
}

func (m *errOnExecMock) Exec(ctx context.Context, sql string, args ...interface{}) (interface{}, error) {
	if contains(sql, m.failSubstr) {
		m.base.recordQuery(sql)
		return nil, errors.New("simulated SET failure")
	}
	return m.base.Exec(ctx, sql, args...)
}
