package goldlapel

import (
	"context"
	"errors"
	"fmt"
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

func (m *mockQuerier) lastQuery() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.queries) == 0 {
		return ""
	}
	return m.queries[len(m.queries)-1]
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
	if port != DefaultPort+2 {
		t.Fatalf("expected %d, got %d", DefaultPort+2, port)
	}
}

func TestDetectInvalidationPort_FromInstance(t *testing.T) {
	lastStartedInstanceMu.Lock()
	old := lastStartedInstance
	lastStartedInstance = &GoldLapel{port: 8000}
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

func TestDetectInvalidationPort_FromConfig(t *testing.T) {
	lastStartedInstanceMu.Lock()
	old := lastStartedInstance
	lastStartedInstance = &GoldLapel{
		port:   8000,
		config: map[string]interface{}{"invalidation_port": 9999},
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
	if cc.cache.Get("SELECT * FROM users", nil) == nil {
		t.Fatal("expected cache entry after query")
	}

	cc.Exec(ctx, "INSERT INTO users VALUES (2)")
	if cc.cache.Get("SELECT * FROM users", nil) != nil {
		t.Fatal("expected cache entry to be invalidated after INSERT")
	}
}

func TestSelfInvalidation_UpdateInvalidatesTable(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	cc.Query(ctx, "SELECT * FROM users")
	cc.Exec(ctx, "UPDATE users SET name = 'bob'")
	if cc.cache.Get("SELECT * FROM users", nil) != nil {
		t.Fatal("expected cache invalidation after UPDATE")
	}
}

func TestSelfInvalidation_TruncateInvalidatesTable(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	ctx := context.Background()

	cc.Query(ctx, "SELECT * FROM users")
	cc.Exec(ctx, "TRUNCATE users")
	if cc.cache.Get("SELECT * FROM users", nil) != nil {
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
	if cc.cache.Get("SELECT * FROM users", nil) != nil {
		t.Fatal("expected no cache entry after multiple writes")
	}
}
