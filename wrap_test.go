package goldlapel

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// --- Mock Querier ---

type mockQuerier struct {
	mu        sync.Mutex
	queries   []string
	execCount int
	rows      [][]interface{}
	fields    []FieldDescription
}

func newMockQuerier(rows [][]interface{}, fields []FieldDescription) *mockQuerier {
	return &mockQuerier{rows: rows, fields: fields}
}

func (m *mockQuerier) Query(ctx context.Context, sql string, args ...interface{}) (Rows, error) {
	m.mu.Lock()
	m.queries = append(m.queries, sql)
	m.mu.Unlock()
	return &mockRows{rows: m.rows, fields: m.fields, pos: -1}, nil
}

func (m *mockQuerier) QueryRow(ctx context.Context, sql string, args ...interface{}) Row {
	m.mu.Lock()
	m.queries = append(m.queries, sql)
	m.mu.Unlock()
	if len(m.rows) > 0 {
		return &mockRow{values: m.rows[0]}
	}
	return &mockRow{values: nil}
}

func (m *mockQuerier) Exec(ctx context.Context, sql string, args ...interface{}) (interface{}, error) {
	m.mu.Lock()
	m.queries = append(m.queries, sql)
	m.execCount++
	m.mu.Unlock()
	return nil, nil
}

func (m *mockQuerier) queryCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.queries)
}

type mockRows struct {
	rows   [][]interface{}
	fields []FieldDescription
	pos    int
}

func (r *mockRows) Next() bool {
	r.pos++
	return r.pos < len(r.rows)
}

func (r *mockRows) Scan(dest ...interface{}) error {
	if r.pos < 0 || r.pos >= len(r.rows) {
		return nil
	}
	for i := 0; i < len(dest) && i < len(r.rows[r.pos]); i++ {
		if p, ok := dest[i].(*interface{}); ok {
			*p = r.rows[r.pos][i]
		}
	}
	return nil
}

func (r *mockRows) Close()    {}
func (r *mockRows) Err() error { return nil }
func (r *mockRows) FieldDescriptions() []FieldDescription {
	return r.fields
}
func (r *mockRows) Values() ([]interface{}, error) {
	if r.pos < 0 || r.pos >= len(r.rows) {
		return nil, nil
	}
	return r.rows[r.pos], nil
}
func (r *mockRows) RawValues() [][]byte { return nil }

type mockRow struct {
	values []interface{}
}

func (r *mockRow) Scan(dest ...interface{}) error {
	if r.values == nil {
		return nil
	}
	for i := 0; i < len(dest) && i < len(r.values); i++ {
		if p, ok := dest[i].(*interface{}); ok {
			*p = r.values[i]
		}
	}
	return nil
}

// --- Helper ---

func setupWrapped(t *testing.T, rows [][]interface{}, fields []FieldDescription) (*CachedConn, *mockQuerier) {
	t.Helper()
	ResetNativeCache()
	cache := GetNativeCache()
	cache.mu.Lock()
	cache.invConnected = true
	cache.mu.Unlock()
	mock := newMockQuerier(rows, fields)
	cc := &CachedConn{real: mock, cache: cache}
	return cc, mock
}

// --- Query caching ---

func TestQuery_CacheHit(t *testing.T) {
	cc, mock := setupWrapped(t, [][]interface{}{{1, "alice"}}, []FieldDescription{{Name: "id"}, {Name: "name"}})
	ctx := context.Background()

	// First call: miss, queries real
	rows1, err := cc.Query(ctx, "SELECT * FROM users")
	if err != nil {
		t.Fatal(err)
	}
	count1 := 0
	for rows1.Next() {
		count1++
	}
	if count1 != 1 {
		t.Fatalf("expected 1 row, got %d", count1)
	}
	if mock.queryCount() != 1 {
		t.Fatalf("expected 1 real query, got %d", mock.queryCount())
	}

	// Second call: hit, no real query
	rows2, err := cc.Query(ctx, "SELECT * FROM users")
	if err != nil {
		t.Fatal(err)
	}
	count2 := 0
	for rows2.Next() {
		count2++
	}
	if count2 != 1 {
		t.Fatalf("expected 1 row on cache hit, got %d", count2)
	}
	if mock.queryCount() != 1 {
		t.Fatalf("expected still 1 real query, got %d", mock.queryCount())
	}
}

func TestQuery_WriteInvalidates(t *testing.T) {
	cc, mock := setupWrapped(t, [][]interface{}{{1, "alice"}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	// Populate cache
	cc.Query(ctx, "SELECT * FROM users")
	if mock.queryCount() != 1 {
		t.Fatalf("expected 1 query, got %d", mock.queryCount())
	}

	// Write invalidates
	cc.Exec(ctx, "DELETE FROM users WHERE id = 1")

	// Read again should miss
	cc.Query(ctx, "SELECT * FROM users")
	if mock.queryCount() != 3 {
		t.Fatalf("expected 3 queries (select, delete, select), got %d", mock.queryCount())
	}
}

func TestQuery_DDLInvalidatesAll(t *testing.T) {
	cc, mock := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	cc.Query(ctx, "SELECT * FROM users")
	cc.Query(ctx, "SELECT * FROM orders")
	if mock.queryCount() != 2 {
		t.Fatalf("expected 2 queries, got %d", mock.queryCount())
	}

	// DDL invalidates all
	cc.Exec(ctx, "CREATE TABLE foo (id int)")

	// Both should miss
	cc.Query(ctx, "SELECT * FROM users")
	cc.Query(ctx, "SELECT * FROM orders")
	if mock.queryCount() != 5 {
		t.Fatalf("expected 5 queries, got %d", mock.queryCount())
	}
}

func TestQuery_TransactionBypass(t *testing.T) {
	cc, mock := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	// Populate cache
	cc.Query(ctx, "SELECT * FROM users")

	// Enter transaction
	cc.Exec(ctx, "BEGIN")

	// Query inside transaction should bypass cache
	cc.Query(ctx, "SELECT * FROM users")
	if mock.queryCount() != 3 {
		t.Fatalf("expected 3 queries (select, begin, select), got %d", mock.queryCount())
	}

	// End transaction
	cc.Exec(ctx, "COMMIT")

	// Should hit cache again
	cc.Query(ctx, "SELECT * FROM users")
	if mock.queryCount() != 4 {
		t.Fatalf("expected 4 queries (no new real query), got %d", mock.queryCount())
	}
}

func TestQuery_RollbackReenablesCache(t *testing.T) {
	cc, mock := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	cc.Query(ctx, "SELECT * FROM users")
	cc.Exec(ctx, "BEGIN")
	cc.Query(ctx, "SELECT * FROM users") // bypass
	cc.Exec(ctx, "ROLLBACK")
	cc.Query(ctx, "SELECT * FROM users") // should hit cache

	// select(1) + begin(2) + select-bypass(3) + rollback(4) + select-cache(no query)
	if mock.queryCount() != 4 {
		t.Fatalf("expected 4 real queries, got %d", mock.queryCount())
	}
}

// --- QueryRow ---

func TestQueryRow_CacheHit(t *testing.T) {
	cc, mock := setupWrapped(t, [][]interface{}{{1, "alice"}}, []FieldDescription{{Name: "id"}, {Name: "name"}})
	ctx := context.Background()

	// Populate cache via Query first
	rows, _ := cc.Query(ctx, "SELECT * FROM users WHERE id = 1")
	for rows.Next() {
	}

	// QueryRow should hit cache
	row := cc.QueryRow(ctx, "SELECT * FROM users WHERE id = 1")
	var id, name interface{}
	row.Scan(&id, &name)
	if id != 1 {
		t.Fatalf("expected id=1, got %v", id)
	}
	if mock.queryCount() != 1 {
		t.Fatalf("expected 1 real query, got %d", mock.queryCount())
	}
}

// --- Exec ---

func TestExec_WriteDetection(t *testing.T) {
	cc, mock := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	cc.Query(ctx, "SELECT * FROM orders")
	cc.Exec(ctx, "INSERT INTO orders VALUES (2)")

	// Cache should be invalidated for orders
	cc.Query(ctx, "SELECT * FROM orders")
	// select(1) + insert(2) + select(3)
	if mock.queryCount() != 3 {
		t.Fatalf("expected 3 queries, got %d", mock.queryCount())
	}
}

func TestExec_TransactionTracking(t *testing.T) {
	cc, _ := setupWrapped(t, nil, nil)
	ctx := context.Background()

	if cc.inTransaction {
		t.Fatal("should not be in transaction initially")
	}

	cc.Exec(ctx, "BEGIN")
	if !cc.inTransaction {
		t.Fatal("should be in transaction after BEGIN")
	}

	cc.Exec(ctx, "COMMIT")
	if cc.inTransaction {
		t.Fatal("should not be in transaction after COMMIT")
	}
}

// --- Unwrap ---

func TestUnwrap(t *testing.T) {
	cc, mock := setupWrapped(t, nil, nil)
	if cc.Unwrap() != mock {
		t.Fatal("Unwrap should return the real connection")
	}
}

// --- Different params ---

func TestQuery_DifferentParamsCacheSeparately(t *testing.T) {
	cc, mock := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	cc.Query(ctx, "SELECT * FROM users WHERE id = $1", 1)
	cc.Query(ctx, "SELECT * FROM users WHERE id = $1", 2)

	// Both should miss (different args)
	if mock.queryCount() != 2 {
		t.Fatalf("expected 2 queries, got %d", mock.queryCount())
	}

	// But repeating should hit
	cc.Query(ctx, "SELECT * FROM users WHERE id = $1", 1)
	if mock.queryCount() != 2 {
		t.Fatalf("expected still 2 queries (cache hit), got %d", mock.queryCount())
	}
}

// --- Write via Query ---

func TestQuery_WriteViaQueryInvalidates(t *testing.T) {
	cc, mock := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	cc.Query(ctx, "SELECT * FROM users")
	// Write via Query (some drivers support this)
	cc.Query(ctx, "DELETE FROM users WHERE id = 1")

	// Should be invalidated
	cc.Query(ctx, "SELECT * FROM users")
	if mock.queryCount() != 3 {
		t.Fatalf("expected 3 queries, got %d", mock.queryCount())
	}
}

// --- Cached rows behavior ---

func TestCachedRows_Iteration(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{1, "a"}, {2, "b"}, {3, "c"}}, []FieldDescription{{Name: "id"}, {Name: "val"}})
	ctx := context.Background()

	// Populate cache
	rows1, _ := cc.Query(ctx, "SELECT * FROM items")
	count := 0
	for rows1.Next() {
		count++
	}
	if count != 3 {
		t.Fatalf("expected 3 rows, got %d", count)
	}

	// Second call: iterate from cache
	rows2, _ := cc.Query(ctx, "SELECT * FROM items")
	var ids []interface{}
	for rows2.Next() {
		vals, _ := rows2.Values()
		ids = append(ids, vals[0])
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3 cached rows, got %d", len(ids))
	}
	if ids[0] != 1 || ids[1] != 2 || ids[2] != 3 {
		t.Fatalf("unexpected values: %v", ids)
	}
}

func TestCachedRows_FieldDescriptions(t *testing.T) {
	fields := []FieldDescription{{Name: "id"}, {Name: "name"}}
	cc, _ := setupWrapped(t, [][]interface{}{{1, "alice"}}, fields)
	ctx := context.Background()

	// Populate + re-fetch from cache
	r, _ := cc.Query(ctx, "SELECT * FROM users")
	for r.Next() {
	}
	r2, _ := cc.Query(ctx, "SELECT * FROM users")
	fds := r2.FieldDescriptions()
	if len(fds) != 2 || fds[0].Name != "id" || fds[1].Name != "name" {
		t.Fatalf("unexpected field descriptions: %v", fds)
	}
}

func TestCachedRows_Scan(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{42, "test"}}, []FieldDescription{{Name: "id"}, {Name: "val"}})
	ctx := context.Background()

	// Populate
	r, _ := cc.Query(ctx, "SELECT * FROM items")
	for r.Next() {
	}

	// Re-fetch from cache and scan
	r2, _ := cc.Query(ctx, "SELECT * FROM items")
	r2.Next()
	var id, val interface{}
	r2.Scan(&id, &val)
	if id != 42 || val != "test" {
		t.Fatalf("scan: got id=%v val=%v", id, val)
	}
}

// --- CachedRow scan ---

func TestCachedRow_Scan(t *testing.T) {
	row := &cachedRow{values: []interface{}{1, "alice"}}
	var id, name interface{}
	row.Scan(&id, &name)
	if id != 1 || name != "alice" {
		t.Fatalf("got id=%v name=%v", id, name)
	}
}

func TestCachedRow_ScanNil_ReturnsErrNoRows(t *testing.T) {
	row := &cachedRow{values: nil}
	var id interface{}
	err := row.Scan(&id)
	if !errors.Is(err, ErrNoRows) {
		t.Fatalf("expected ErrNoRows, got: %v", err)
	}
}

func TestCachedRow_ScanWithError_ReturnsError(t *testing.T) {
	testErr := errors.New("query failed")
	row := &cachedRow{err: testErr}
	var id interface{}
	err := row.Scan(&id)
	if !errors.Is(err, testErr) {
		t.Fatalf("expected query error, got: %v", err)
	}
}

func TestQueryRow_NoRows_ReturnsErrNoRows(t *testing.T) {
	// Mock returns no rows
	cc, _ := setupWrapped(t, nil, nil)
	ctx := context.Background()

	row := cc.QueryRow(ctx, "SELECT * FROM users WHERE id = 999")
	var id interface{}
	err := row.Scan(&id)
	if !errors.Is(err, ErrNoRows) {
		t.Fatalf("expected ErrNoRows, got: %v", err)
	}
}

func TestQueryRow_NoRows_CacheHit_ReturnsErrNoRows(t *testing.T) {
	// Mock returns no rows; first call caches empty result, second call is cache hit
	cc, _ := setupWrapped(t, nil, nil)
	ctx := context.Background()

	// First call: cache miss, queries real, gets no rows, caches empty result
	cc.Query(ctx, "SELECT * FROM users WHERE id = 999")

	// Second call via QueryRow: cache hit with zero rows
	row := cc.QueryRow(ctx, "SELECT * FROM users WHERE id = 999")
	var id interface{}
	err := row.Scan(&id)
	if !errors.Is(err, ErrNoRows) {
		t.Fatalf("expected ErrNoRows on cache hit with zero rows, got: %v", err)
	}
}

func TestQueryRow_WithRows_NoError(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{42, "alice"}}, []FieldDescription{{Name: "id"}, {Name: "name"}})
	ctx := context.Background()

	row := cc.QueryRow(ctx, "SELECT * FROM users WHERE id = 42")
	var id, name interface{}
	err := row.Scan(&id, &name)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != 42 || name != "alice" {
		t.Fatalf("expected id=42 name=alice, got id=%v name=%v", id, name)
	}
}

// --- Wrap function ---

func TestWrap_DefaultPort(t *testing.T) {
	ResetNativeCache()
	defer ResetNativeCache()

	mock := newMockQuerier(nil, nil)
	cc := Wrap(mock, 0)
	if cc.real != mock {
		t.Fatal("expected real to be mock")
	}
	// Cache should have started invalidation
	cache := GetNativeCache()
	_ = cache // invalidation goroutine was started (port detection defaults)
	cc.cache.StopInvalidation()
}

func TestWrap_CustomPort(t *testing.T) {
	ResetNativeCache()
	defer ResetNativeCache()

	mock := newMockQuerier(nil, nil)
	cc := Wrap(mock, 9999)
	if cc.real != mock {
		t.Fatal("expected real to be mock")
	}
	cc.cache.StopInvalidation()
}

// --- detectInvalidationPort ---

func TestDetectInvalidationPort_Default(t *testing.T) {
	lastStartedInstanceMu.Lock()
	old := lastStartedInstance
	lastStartedInstance = nil
	lastStartedInstanceMu.Unlock()
	defer func() {
		lastStartedInstanceMu.Lock()
		lastStartedInstance = old
		lastStartedInstanceMu.Unlock()
	}()

	port := detectInvalidationPort()
	if port != DefaultProxyPort+2 {
		t.Fatalf("expected %d, got %d", DefaultProxyPort+2, port)
	}
}

func TestDetectInvalidationPort_FromInstance(t *testing.T) {
	lastStartedInstanceMu.Lock()
	old := lastStartedInstance
	lastStartedInstance = &GoldLapel{proxyPort: 8000, invalidationPort: 8002}
	lastStartedInstanceMu.Unlock()
	defer func() {
		lastStartedInstanceMu.Lock()
		lastStartedInstance = old
		lastStartedInstanceMu.Unlock()
	}()

	port := detectInvalidationPort()
	if port != 8002 {
		t.Fatalf("expected 8002, got %d", port)
	}
}

func TestDetectInvalidationPort_FromExplicitOverride(t *testing.T) {
	lastStartedInstanceMu.Lock()
	old := lastStartedInstance
	lastStartedInstance = &GoldLapel{
		proxyPort:           8000,
		invalidationPort:    9999,
		invalidationPortSet: true,
	}
	lastStartedInstanceMu.Unlock()
	defer func() {
		lastStartedInstanceMu.Lock()
		lastStartedInstance = old
		lastStartedInstanceMu.Unlock()
	}()

	port := detectInvalidationPort()
	if port != 9999 {
		t.Fatalf("expected 9999, got %d", port)
	}
}

// --- Integration: write self-invalidation patterns ---

func TestSelfInvalidation_InsertInvalidatesTable(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	cc.Query(ctx, "SELECT * FROM users")
	if cc.cache.Get("SELECT * FROM users", nil, 0) == nil {
		t.Fatal("expected cache entry after query")
	}

	cc.Exec(ctx, "INSERT INTO users VALUES (2)")
	if cc.cache.Get("SELECT * FROM users", nil, 0) != nil {
		t.Fatal("expected cache entry to be invalidated after INSERT")
	}
}

func TestSelfInvalidation_UpdateInvalidatesTable(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	cc.Query(ctx, "SELECT * FROM users")
	cc.Exec(ctx, "UPDATE users SET name = 'bob'")
	if cc.cache.Get("SELECT * FROM users", nil, 0) != nil {
		t.Fatal("expected cache invalidation after UPDATE")
	}
}

func TestSelfInvalidation_TruncateInvalidatesTable(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	cc.Query(ctx, "SELECT * FROM users")
	cc.Exec(ctx, "TRUNCATE users")
	if cc.cache.Get("SELECT * FROM users", nil, 0) != nil {
		t.Fatal("expected cache invalidation after TRUNCATE")
	}
}

func TestSelfInvalidation_DropInvalidatesAll(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	cc.Query(ctx, "SELECT * FROM users")
	cc.Query(ctx, "SELECT * FROM orders")
	cc.Exec(ctx, "DROP TABLE foo")
	if cc.cache.Size() != 0 {
		t.Fatalf("expected cache to be fully cleared after DDL, got %d entries", cc.cache.Size())
	}
}

// --- Edge cases ---

func TestQuery_EmptySql(t *testing.T) {
	cc, mock := setupWrapped(t, nil, nil)
	ctx := context.Background()
	_, err := cc.Query(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if mock.queryCount() != 1 {
		t.Fatalf("expected 1 query, got %d", mock.queryCount())
	}
}

func TestQuery_SelectNotCachedWhenNotConnected(t *testing.T) {
	ResetNativeCache()
	cache := GetNativeCache()
	// Not connected - don't set invConnected
	mock := newMockQuerier([][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	cc := &CachedConn{real: mock, cache: cache}
	ctx := context.Background()

	cc.Query(ctx, "SELECT * FROM users")
	cc.Query(ctx, "SELECT * FROM users")
	// Both should hit real since cache is not connected
	if mock.queryCount() != 2 {
		t.Fatalf("expected 2 real queries when cache not connected, got %d", mock.queryCount())
	}
}

// --- Multiple writes to same table ---

func TestMultipleWritesSameTable(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		cc.Query(ctx, "SELECT * FROM users")
		cc.Exec(ctx, fmt.Sprintf("INSERT INTO users VALUES (%d)", i))
	}
	// After final insert, cache should be empty for users
	if cc.cache.Get("SELECT * FROM users", nil, 0) != nil {
		t.Fatal("expected no cache entry after multiple writes")
	}
}

// --- L1 state-hash integration (Option Y wrapper-side) ---
//
// These tests wire CachedConn end-to-end through Query/QueryRow/Exec
// against a mock Querier and verify that unsafe-GUC SETs change the
// per-connection cache key — preventing cache leaks when two CachedConns
// share the singleton NativeCache but differ in their RLS-shaping GUC
// state.

// selectCount returns how many of the SQLs the mock saw start with
// SELECT — i.e. it filters out the SET / RESET / BEGIN / COMMIT noise
// that every state-hash test produces. Existing queryCount() lumps Query
// + QueryRow + Exec together, which inflates the number we care about
// (real fetches against the backend).
func selectCount(m *mockQuerier) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, q := range m.queries {
		t := strings.TrimSpace(q)
		if strings.HasPrefix(strings.ToUpper(t), "SELECT") {
			n++
		}
	}
	return n
}

func TestStateHash_SafeSetDoesNotChangeCacheKey(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	// Cache a SELECT under the baseline state.
	cc.Query(ctx, "SELECT * FROM users")
	hashBefore := cc.GucStateHash()

	// SET timezone is harmless — the hash must not move.
	if _, err := cc.Exec(ctx, "SET timezone = 'UTC'"); err != nil {
		t.Fatal(err)
	}
	if cc.GucStateHash() != hashBefore {
		t.Fatalf("safe SET must not change state hash: %d → %d",
			hashBefore, cc.GucStateHash())
	}
}

func TestStateHash_UnsafeSetChangesCacheKey(t *testing.T) {
	cc, mock := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	// Cache a SELECT under the baseline state. First call: real query.
	cc.Query(ctx, "SELECT * FROM accounts")
	if selectCount(mock) != 1 {
		t.Fatalf("expected 1 real SELECT, got %d", selectCount(mock))
	}
	hashBefore := cc.GucStateHash()

	// SET an unsafe GUC — the hash must move.
	if _, err := cc.Exec(ctx, "SET app.user_id = '42'"); err != nil {
		t.Fatal(err)
	}
	hashAfter := cc.GucStateHash()
	if hashBefore == hashAfter {
		t.Fatalf("unsafe SET must change state hash; both are %d", hashBefore)
	}

	// The same SELECT under the new state hash must miss the cache and
	// route through to the real Querier — the previous user's cached rows
	// must NOT be served.
	cc.Query(ctx, "SELECT * FROM accounts")
	if selectCount(mock) != 2 {
		t.Fatalf("expected 2 real SELECTs (cache key separated by state hash), got %d", selectCount(mock))
	}
}

func TestStateHash_TwoConnectionsWithDifferentStateNeverCrossShare(t *testing.T) {
	// The wrapper's NativeCache is a process-wide singleton. Two
	// CachedConns built against it must not serve each other's RLS-shaped
	// rows when their unsafe-GUC state differs — this is the cache
	// security invariant Option Y enforces.
	ResetNativeCache()
	cache := GetNativeCache()
	cache.mu.Lock()
	cache.invConnected = true
	cache.mu.Unlock()

	mockA := newMockQuerier([][]interface{}{{"alice-only"}}, []FieldDescription{{Name: "name"}})
	mockB := newMockQuerier([][]interface{}{{"bob-only"}}, []FieldDescription{{Name: "name"}})

	connA := &CachedConn{real: mockA, cache: cache, gucState: NewConnectionGucState()}
	connB := &CachedConn{real: mockB, cache: cache, gucState: NewConnectionGucState()}
	ctx := context.Background()

	// connA SETs app.user_id='alice', then queries — caches under
	// (sql=SELECT *, hash=H(app.user_id=alice)).
	connA.Exec(ctx, "SET app.user_id = 'alice'")
	connA.Query(ctx, "SELECT * FROM accounts")

	// connB SETs app.user_id='bob', then queries the same SQL. With the
	// state hash folded into the cache key, connB MUST miss connA's
	// cached row (different hash slot) and route through to its own
	// mock for fresh data.
	connB.Exec(ctx, "SET app.user_id = 'bob'")
	connB.Query(ctx, "SELECT * FROM accounts")

	if selectCount(mockB) != 1 {
		t.Fatalf("connB should have hit its real backend (different state hash from connA); got %d real SELECTs on B",
			selectCount(mockB))
	}
}

func TestStateHash_SameStateOnTwoConnectionsCanShareCache(t *testing.T) {
	// Inverse of the security invariant: when two CachedConns hold the
	// SAME unsafe-GUC state, the singleton native cache may serve a hit
	// across them — same key, same hash, same slot.
	ResetNativeCache()
	cache := GetNativeCache()
	cache.mu.Lock()
	cache.invConnected = true
	cache.mu.Unlock()

	mockA := newMockQuerier([][]interface{}{{"shared"}}, []FieldDescription{{Name: "v"}})
	mockB := newMockQuerier([][]interface{}{{"would-be-served-but-shouldnt-be"}}, []FieldDescription{{Name: "v"}})

	connA := &CachedConn{real: mockA, cache: cache, gucState: NewConnectionGucState()}
	connB := &CachedConn{real: mockB, cache: cache, gucState: NewConnectionGucState()}
	ctx := context.Background()

	connA.Exec(ctx, "SET app.user_id = '42'")
	connB.Exec(ctx, "SET app.user_id = '42'")

	connA.Query(ctx, "SELECT shared_value()")
	// connB should hit the cache populated by connA — same SQL, same
	// state hash. selectCount(mockB) stays at 0.
	connB.Query(ctx, "SELECT shared_value()")

	if selectCount(mockB) != 0 {
		t.Fatalf("connB should have hit the singleton cache (same state hash as connA); got %d real SELECTs on B",
			selectCount(mockB))
	}
	if selectCount(mockA) != 1 {
		t.Fatalf("connA should have hit real backend exactly once (priming the cache); got %d", selectCount(mockA))
	}
}

func TestStateHash_ResetReturnsToBaselineCacheKey(t *testing.T) {
	// After SET app.user_id = '42'; ... ; RESET app.user_id, the cache
	// key returns to the baseline (state hash 0) — so any rows previously
	// cached at the baseline are once again accessible.
	cc, mock := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	// Prime the baseline cache slot.
	cc.Query(ctx, "SELECT * FROM t")
	if selectCount(mock) != 1 {
		t.Fatalf("expected 1 real SELECT priming baseline, got %d", selectCount(mock))
	}

	// Move off baseline.
	cc.Exec(ctx, "SET app.user_id = '42'")
	cc.Query(ctx, "SELECT * FROM t")
	if selectCount(mock) != 2 {
		t.Fatalf("expected 2 real SELECTs (baseline + GUC-state slot), got %d", selectCount(mock))
	}

	// Return to baseline via RESET. The cached entry from the first call
	// should still be reachable.
	cc.Exec(ctx, "RESET app.user_id")
	if cc.GucStateHash() != 0 {
		t.Fatalf("RESET should restore baseline hash 0, got %d", cc.GucStateHash())
	}
	cc.Query(ctx, "SELECT * FROM t")
	if selectCount(mock) != 2 {
		t.Fatalf("expected RESET to restore baseline cache hit (still 2 real SELECTs), got %d", selectCount(mock))
	}
}

func TestStateHash_SetLocalDoesNotMoveHash(t *testing.T) {
	// SET LOCAL is observed but never moves the state hash — its effect
	// is bounded to the current transaction, and CachedConn already
	// bypasses the cache while inTransaction=true.
	cc, _ := setupWrapped(t, nil, nil)
	ctx := context.Background()

	cc.Exec(ctx, "BEGIN")
	cc.Exec(ctx, "SET LOCAL app.user_id = '42'")
	if cc.GucStateHash() != 0 {
		t.Fatalf("SET LOCAL must not move hash: got %d", cc.GucStateHash())
	}
	cc.Exec(ctx, "COMMIT")
	if cc.GucStateHash() != 0 {
		t.Fatal("hash should still be 0 after COMMIT")
	}
}

func TestStateHash_QueryAndQueryRowAlsoObserve(t *testing.T) {
	// SET typically arrives via Exec, but defensive: any of the three
	// entry points must observe SQL — some drivers route bare SET through
	// Query / QueryRow.
	cc, _ := setupWrapped(t, nil, nil)
	ctx := context.Background()

	cc.Query(ctx, "SET app.tenant = 'alpha'")
	if cc.GucStateHash() == 0 {
		t.Fatal("Query path must observe SET commands")
	}

	cc2, _ := setupWrapped(t, nil, nil)
	cc2.QueryRow(ctx, "SET app.tenant = 'alpha'")
	if cc2.GucStateHash() == 0 {
		t.Fatal("QueryRow path must observe SET commands")
	}
}

// --- Bug 1: multi-statement write detection ---

// A multi-statement Q like `SET app.tenant='x'; INSERT INTO orders VALUES (1)`
// previously fell through detectWrite (first token was SET) and never
// invalidated the orders cache. detectWriteMulti walks every segment so
// the INSERT now drives an invalidation regardless of which path
// (Query / QueryRow / Exec) the body arrives on.
func TestMultiStatement_SetThenInsertInvalidatesViaQuery(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	cc.Query(ctx, "SELECT * FROM orders")
	if cc.cache.Get("SELECT * FROM orders", nil, cc.GucStateHash()) == nil {
		t.Fatal("expected cache entry after baseline SELECT")
	}

	// Multi-statement body routed through Query.
	cc.Query(ctx, "SET app.tenant = 'x'; INSERT INTO orders VALUES (1)")

	// The new state hash applies after the SET, so check both the old
	// baseline key (must be gone) and confirm Size dropped.
	if cc.cache.Size() != 0 {
		t.Fatalf("expected cache invalidation after multi-statement INSERT via Query, size=%d", cc.cache.Size())
	}
}

func TestMultiStatement_SetThenInsertInvalidatesViaExec(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	cc.Query(ctx, "SELECT * FROM orders")
	if cc.cache.Size() != 1 {
		t.Fatalf("expected 1 cache entry, got %d", cc.cache.Size())
	}

	cc.Exec(ctx, "SET app.tenant = 'x'; INSERT INTO orders VALUES (1)")

	if cc.cache.Size() != 0 {
		t.Fatalf("expected cache invalidation after multi-statement INSERT via Exec, size=%d", cc.cache.Size())
	}
}

func TestMultiStatement_SetThenInsertInvalidatesViaQueryRow(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	cc.Query(ctx, "SELECT * FROM orders")
	if cc.cache.Size() != 1 {
		t.Fatalf("expected 1 cache entry, got %d", cc.cache.Size())
	}

	cc.QueryRow(ctx, "SET app.tenant = 'x'; INSERT INTO orders VALUES (1)")

	if cc.cache.Size() != 0 {
		t.Fatalf("expected cache invalidation after multi-statement INSERT via QueryRow, size=%d", cc.cache.Size())
	}
}

// `BEGIN; INSERT INTO t ...; COMMIT` is the explicit case called out in
// the spec — txStartRe matches BEGIN and previously short-circuited
// before write detection. detectWriteMulti now runs ahead of the
// short-circuit.
func TestMultiStatement_BeginInsertCommitInvalidates(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	cc.Query(ctx, "SELECT * FROM orders")
	if cc.cache.Size() != 1 {
		t.Fatalf("expected 1 cache entry, got %d", cc.cache.Size())
	}

	cc.Exec(ctx, "BEGIN; INSERT INTO orders VALUES (1); COMMIT")

	if cc.cache.Size() != 0 {
		t.Fatalf("expected cache invalidation after BEGIN/INSERT/COMMIT body, size=%d", cc.cache.Size())
	}
}

func TestMultiStatement_DDLAnywhereInvalidatesAll(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	cc.Query(ctx, "SELECT * FROM users")
	cc.Query(ctx, "SELECT * FROM orders")
	if cc.cache.Size() != 2 {
		t.Fatalf("expected 2 cache entries, got %d", cc.cache.Size())
	}

	cc.Exec(ctx, "INSERT INTO users VALUES (1); CREATE TABLE foo (id int)")

	if cc.cache.Size() != 0 {
		t.Fatalf("expected full cache wipe after DDL in multi-statement body, size=%d", cc.cache.Size())
	}
}

func TestMultiStatement_TwoWritesUnionInvalidations(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	cc.Query(ctx, "SELECT * FROM users")
	cc.Query(ctx, "SELECT * FROM orders")
	cc.Query(ctx, "SELECT * FROM products")
	if cc.cache.Size() != 3 {
		t.Fatalf("expected 3 cache entries, got %d", cc.cache.Size())
	}

	cc.Exec(ctx, "INSERT INTO users VALUES (1); UPDATE orders SET x = 1")

	// Both users and orders should be invalidated; products survives.
	if cc.cache.Size() != 1 {
		t.Fatalf("expected 1 cache entry (products) to survive, got %d", cc.cache.Size())
	}
	if cc.cache.Get("SELECT * FROM products", nil, 0) == nil {
		t.Fatal("expected products cache entry to survive multi-statement write")
	}
}

// --- Bug 2: SET / RESET / LISTEN responses must not be cached ---

// Routed through Query, a bare SET produces an empty rows response. The
// truthy-tail Put in pre-fix wrap.go inserted that empty entry into the
// LRU. After the fix the Put is suppressed for first-token session-state
// commands, so cache.Size() stays at 0.
func TestSessionStateCommand_SetViaQueryNotCached(t *testing.T) {
	cc, _ := setupWrapped(t, nil, nil)
	ctx := context.Background()

	if cc.cache.Size() != 0 {
		t.Fatalf("expected empty cache, got %d", cc.cache.Size())
	}
	cc.Query(ctx, "SET app.user_id = '42'")
	if cc.cache.Size() != 0 {
		t.Fatalf("expected SET response NOT to be cached, size=%d", cc.cache.Size())
	}
}

func TestSessionStateCommand_ResetViaQueryNotCached(t *testing.T) {
	cc, _ := setupWrapped(t, nil, nil)
	ctx := context.Background()
	cc.Query(ctx, "RESET app.user_id")
	if cc.cache.Size() != 0 {
		t.Fatalf("expected RESET response NOT to be cached, size=%d", cc.cache.Size())
	}
}

func TestSessionStateCommand_ListenViaQueryNotCached(t *testing.T) {
	cc, _ := setupWrapped(t, nil, nil)
	ctx := context.Background()
	cc.Query(ctx, "LISTEN my_channel")
	if cc.cache.Size() != 0 {
		t.Fatalf("expected LISTEN response NOT to be cached, size=%d", cc.cache.Size())
	}
	cc.Query(ctx, "UNLISTEN my_channel")
	if cc.cache.Size() != 0 {
		t.Fatalf("expected UNLISTEN response NOT to be cached, size=%d", cc.cache.Size())
	}
	cc.Query(ctx, "NOTIFY my_channel, 'payload'")
	if cc.cache.Size() != 0 {
		t.Fatalf("expected NOTIFY response NOT to be cached, size=%d", cc.cache.Size())
	}
}

// SELECTs still cache normally — ensure the new gate isn't over-broad.
func TestSessionStateCommand_SelectStillCaches(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()
	cc.Query(ctx, "SELECT * FROM users")
	if cc.cache.Size() != 1 {
		t.Fatalf("expected SELECT to be cached, size=%d", cc.cache.Size())
	}
}

// SELECTs that return zero rows still cache (empty result is a valid
// negative-cache entry, distinct from session-state commands which are
// not even queries).
func TestSessionStateCommand_EmptyResultSelectStillCaches(t *testing.T) {
	cc, _ := setupWrapped(t, nil, []FieldDescription{{Name: "id"}})
	ctx := context.Background()
	cc.Query(ctx, "SELECT * FROM users WHERE 1=0")
	if cc.cache.Size() != 1 {
		t.Fatalf("expected zero-row SELECT to still cache, size=%d", cc.cache.Size())
	}
}
