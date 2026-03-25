package goldlapel

import (
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

func makeTestCache(t *testing.T, maxEntries int, enabled bool, connected bool) *NativeCache {
	t.Helper()
	ResetNativeCache()
	if !enabled {
		t.Setenv("GOLDLAPEL_NATIVE_CACHE", "false")
	} else {
		os.Unsetenv("GOLDLAPEL_NATIVE_CACHE")
	}
	t.Setenv("GOLDLAPEL_NATIVE_CACHE_SIZE", "100")
	if maxEntries > 0 {
		t.Setenv("GOLDLAPEL_NATIVE_CACHE_SIZE", string(rune(maxEntries+'0')))
	}
	// Use raw constructor to avoid singleton
	cache := newNativeCache()
	cache.maxEntries = maxEntries
	cache.enabled = enabled
	cache.invConnected = connected
	return cache
}

// --- makeKey ---

func TestMakeKey_NoArgs(t *testing.T) {
	key := makeKey("SELECT 1", nil)
	if key != "SELECT 1\x00" {
		t.Fatalf("got %q", key)
	}
}

func TestMakeKey_WithArgs(t *testing.T) {
	key := makeKey("SELECT $1", []interface{}{42})
	want := "SELECT $1\x00[42]"
	if key != want {
		t.Fatalf("got %q, want %q", key, want)
	}
}

func TestMakeKey_DifferentArgs(t *testing.T) {
	k1 := makeKey("SELECT $1", []interface{}{1})
	k2 := makeKey("SELECT $1", []interface{}{2})
	if k1 == k2 {
		t.Fatal("different args should produce different keys")
	}
}

func TestMakeKey_SameArgsSameKey(t *testing.T) {
	k1 := makeKey("SELECT $1", []interface{}{42})
	k2 := makeKey("SELECT $1", []interface{}{42})
	if k1 != k2 {
		t.Fatal("same args should produce same key")
	}
}

// --- detectWrite ---

func TestDetectWrite_Insert(t *testing.T) {
	if got := detectWrite("INSERT INTO orders VALUES (1)"); got != "orders" {
		t.Fatalf("got %q", got)
	}
}

func TestDetectWrite_InsertSchema(t *testing.T) {
	if got := detectWrite("INSERT INTO public.orders VALUES (1)"); got != "orders" {
		t.Fatalf("got %q", got)
	}
}

func TestDetectWrite_Update(t *testing.T) {
	if got := detectWrite("UPDATE orders SET name = 'x'"); got != "orders" {
		t.Fatalf("got %q", got)
	}
}

func TestDetectWrite_Delete(t *testing.T) {
	if got := detectWrite("DELETE FROM orders WHERE id = 1"); got != "orders" {
		t.Fatalf("got %q", got)
	}
}

func TestDetectWrite_Truncate(t *testing.T) {
	if got := detectWrite("TRUNCATE orders"); got != "orders" {
		t.Fatalf("got %q", got)
	}
}

func TestDetectWrite_TruncateTable(t *testing.T) {
	if got := detectWrite("TRUNCATE TABLE orders"); got != "orders" {
		t.Fatalf("got %q", got)
	}
}

func TestDetectWrite_CreateDDL(t *testing.T) {
	if got := detectWrite("CREATE TABLE foo (id int)"); got != ddlSentinel {
		t.Fatalf("got %q", got)
	}
}

func TestDetectWrite_AlterDDL(t *testing.T) {
	if got := detectWrite("ALTER TABLE foo ADD COLUMN bar int"); got != ddlSentinel {
		t.Fatalf("got %q", got)
	}
}

func TestDetectWrite_DropDDL(t *testing.T) {
	if got := detectWrite("DROP TABLE foo"); got != ddlSentinel {
		t.Fatalf("got %q", got)
	}
}

func TestDetectWrite_SelectReturnsEmpty(t *testing.T) {
	if got := detectWrite("SELECT * FROM orders"); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}

func TestDetectWrite_CaseInsensitive(t *testing.T) {
	if got := detectWrite("insert INTO Orders VALUES (1)"); got != "orders" {
		t.Fatalf("got %q", got)
	}
}

func TestDetectWrite_CopyFrom(t *testing.T) {
	if got := detectWrite("COPY orders FROM '/tmp/data.csv'"); got != "orders" {
		t.Fatalf("got %q", got)
	}
}

func TestDetectWrite_CopyToReturnsEmpty(t *testing.T) {
	if got := detectWrite("COPY orders TO '/tmp/data.csv'"); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}

func TestDetectWrite_CopySubqueryReturnsEmpty(t *testing.T) {
	if got := detectWrite("COPY (SELECT * FROM orders) TO '/tmp/data.csv'"); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}

func TestDetectWrite_WithCTEInsert(t *testing.T) {
	if got := detectWrite("WITH x AS (SELECT 1) INSERT INTO foo SELECT * FROM x"); got != ddlSentinel {
		t.Fatalf("got %q", got)
	}
}

func TestDetectWrite_WithCTESelect(t *testing.T) {
	if got := detectWrite("WITH x AS (SELECT 1) SELECT * FROM x"); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}

func TestDetectWrite_Empty(t *testing.T) {
	if got := detectWrite(""); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}

func TestDetectWrite_WhitespaceOnly(t *testing.T) {
	if got := detectWrite("   "); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}

func TestDetectWrite_CopyWithColumns(t *testing.T) {
	if got := detectWrite("COPY orders(id, name) FROM '/tmp/data.csv'"); got != "orders" {
		t.Fatalf("got %q", got)
	}
}

// --- extractTables ---

func TestExtractTables_SimpleFrom(t *testing.T) {
	tables := extractTables("SELECT * FROM orders")
	if !tables["orders"] {
		t.Fatal("expected 'orders'")
	}
}

func TestExtractTables_Join(t *testing.T) {
	tables := extractTables("SELECT * FROM orders o JOIN customers c ON o.cid = c.id")
	if !tables["orders"] || !tables["customers"] {
		t.Fatalf("got %v", tables)
	}
}

func TestExtractTables_SchemaQualified(t *testing.T) {
	tables := extractTables("SELECT * FROM public.orders")
	if !tables["orders"] {
		t.Fatal("expected 'orders'")
	}
}

func TestExtractTables_MultipleJoins(t *testing.T) {
	tables := extractTables("SELECT * FROM orders JOIN items ON 1=1 JOIN products ON 1=1")
	if len(tables) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(tables))
	}
}

func TestExtractTables_CaseInsensitive(t *testing.T) {
	tables := extractTables("SELECT * FROM ORDERS")
	if !tables["orders"] {
		t.Fatal("expected 'orders'")
	}
}

func TestExtractTables_NoTables(t *testing.T) {
	tables := extractTables("SELECT 1")
	if len(tables) != 0 {
		t.Fatalf("expected 0 tables, got %d", len(tables))
	}
}

func TestExtractTables_Subquery(t *testing.T) {
	tables := extractTables("SELECT * FROM orders WHERE id IN (SELECT oid FROM users)")
	if !tables["orders"] || !tables["users"] {
		t.Fatalf("got %v", tables)
	}
}

// --- Transaction detection ---

func TestIsTxStart_Begin(t *testing.T) {
	if !isTxStart("BEGIN") {
		t.Fatal("expected true")
	}
}

func TestIsTxStart_StartTransaction(t *testing.T) {
	if !isTxStart("START TRANSACTION") {
		t.Fatal("expected true")
	}
}

func TestIsTxEnd_Commit(t *testing.T) {
	if !isTxEnd("COMMIT") {
		t.Fatal("expected true")
	}
}

func TestIsTxEnd_Rollback(t *testing.T) {
	if !isTxEnd("ROLLBACK") {
		t.Fatal("expected true")
	}
}

func TestIsTxEnd_End(t *testing.T) {
	if !isTxEnd("END") {
		t.Fatal("expected true")
	}
}

func TestIsTxStart_SavepointNotStart(t *testing.T) {
	if isTxStart("SAVEPOINT x") {
		t.Fatal("expected false")
	}
}

func TestIsTxStart_SetTransactionNotStart(t *testing.T) {
	if isTxStart("SET TRANSACTION ISOLATION LEVEL") {
		t.Fatal("expected false")
	}
}

func TestIsTxStart_SelectNotStart(t *testing.T) {
	if isTxStart("SELECT 1") {
		t.Fatal("expected false")
	}
}

// --- Cache operations ---

func TestCache_PutAndGet(t *testing.T) {
	cache := makeTestCache(t, 100, true, true)
	rows := [][]interface{}{{1, "alice"}}
	cache.Put("SELECT * FROM users", nil, rows, nil)
	entry := cache.Get("SELECT * FROM users", nil)
	if entry == nil {
		t.Fatal("expected non-nil entry")
	}
	gotRows := entry.Rows.([][]interface{})
	if len(gotRows) != 1 || gotRows[0][1] != "alice" {
		t.Fatalf("unexpected rows: %v", gotRows)
	}
}

func TestCache_MissReturnsNil(t *testing.T) {
	cache := makeTestCache(t, 100, true, true)
	if entry := cache.Get("SELECT 1", nil); entry != nil {
		t.Fatal("expected nil for miss")
	}
}

func TestCache_DisabledReturnsNil(t *testing.T) {
	cache := makeTestCache(t, 100, false, true)
	cache.Put("SELECT 1", nil, []int{1}, nil)
	if entry := cache.Get("SELECT 1", nil); entry != nil {
		t.Fatal("expected nil when disabled")
	}
}

func TestCache_NotConnectedReturnsNil(t *testing.T) {
	cache := makeTestCache(t, 100, true, false)
	cache.Put("SELECT 1", nil, []int{1}, nil)
	if entry := cache.Get("SELECT 1", nil); entry != nil {
		t.Fatal("expected nil when not connected")
	}
}

func TestCache_ParamsDifferentiateKeys(t *testing.T) {
	cache := makeTestCache(t, 100, true, true)
	cache.Put("SELECT * FROM users WHERE id = $1", []interface{}{1}, []interface{}{"alice"}, nil)
	cache.Put("SELECT * FROM users WHERE id = $1", []interface{}{2}, []interface{}{"bob"}, nil)
	e1 := cache.Get("SELECT * FROM users WHERE id = $1", []interface{}{1})
	e2 := cache.Get("SELECT * FROM users WHERE id = $1", []interface{}{2})
	if e1 == nil || e2 == nil {
		t.Fatal("expected non-nil entries")
	}
	r1 := e1.Rows.([]interface{})
	r2 := e2.Rows.([]interface{})
	if r1[0] != "alice" || r2[0] != "bob" {
		t.Fatalf("got %v and %v", r1, r2)
	}
}

func TestCache_StatsTracking(t *testing.T) {
	cache := makeTestCache(t, 100, true, true)
	cache.Put("SELECT 1", nil, []int{1}, nil)
	cache.Get("SELECT 1", nil)
	cache.Get("SELECT 2", nil)
	if cache.StatsHits() != 1 {
		t.Fatalf("expected 1 hit, got %d", cache.StatsHits())
	}
	if cache.StatsMisses() != 1 {
		t.Fatalf("expected 1 miss, got %d", cache.StatsMisses())
	}
}

// --- LRU eviction ---

func TestLRU_EvictionAtCapacity(t *testing.T) {
	cache := makeTestCache(t, 3, true, true)
	cache.Put("SELECT 1", nil, []int{1}, nil)
	cache.Put("SELECT 2", nil, []int{2}, nil)
	cache.Put("SELECT 3", nil, []int{3}, nil)
	cache.Put("SELECT 4", nil, []int{4}, nil)
	if cache.Get("SELECT 1", nil) != nil {
		t.Fatal("expected SELECT 1 to be evicted")
	}
	if cache.Get("SELECT 4", nil) == nil {
		t.Fatal("expected SELECT 4 to be present")
	}
}

func TestLRU_AccessRefreshes(t *testing.T) {
	cache := makeTestCache(t, 3, true, true)
	cache.Put("SELECT 1", nil, []int{1}, nil)
	cache.Put("SELECT 2", nil, []int{2}, nil)
	cache.Put("SELECT 3", nil, []int{3}, nil)
	cache.Get("SELECT 1", nil) // refresh SELECT 1
	cache.Put("SELECT 4", nil, []int{4}, nil) // evicts SELECT 2 (oldest)
	if cache.Get("SELECT 1", nil) == nil {
		t.Fatal("expected SELECT 1 to be present after refresh")
	}
	if cache.Get("SELECT 2", nil) != nil {
		t.Fatal("expected SELECT 2 to be evicted")
	}
}

func TestLRU_EvictionCleansTableIndex(t *testing.T) {
	cache := makeTestCache(t, 2, true, true)
	cache.Put("SELECT * FROM orders", nil, []int{1}, nil)
	cache.Put("SELECT * FROM users", nil, []int{2}, nil)
	cache.Put("SELECT * FROM products", nil, []int{3}, nil)
	// orders should be evicted and removed from table index
	if keys, ok := cache.tableIndex["orders"]; ok && len(keys) > 0 {
		t.Fatal("expected orders table index to be cleaned")
	}
}

// --- Invalidation ---

func TestInvalidation_Table(t *testing.T) {
	cache := makeTestCache(t, 100, true, true)
	cache.Put("SELECT * FROM orders", nil, []int{1}, nil)
	cache.Put("SELECT * FROM users", nil, []int{2}, nil)
	cache.InvalidateTable("orders")
	if cache.Get("SELECT * FROM orders", nil) != nil {
		t.Fatal("expected orders to be invalidated")
	}
	if cache.Get("SELECT * FROM users", nil) == nil {
		t.Fatal("expected users to still be present")
	}
}

func TestInvalidation_All(t *testing.T) {
	cache := makeTestCache(t, 100, true, true)
	cache.Put("SELECT * FROM orders", nil, []int{1}, nil)
	cache.Put("SELECT * FROM users", nil, []int{2}, nil)
	cache.InvalidateAll()
	if cache.Get("SELECT * FROM orders", nil) != nil {
		t.Fatal("expected orders to be invalidated")
	}
	if cache.Get("SELECT * FROM users", nil) != nil {
		t.Fatal("expected users to be invalidated")
	}
}

func TestInvalidation_CrossReferenced(t *testing.T) {
	cache := makeTestCache(t, 100, true, true)
	cache.Put("SELECT * FROM orders JOIN users ON 1=1", nil, []int{1}, nil)
	cache.InvalidateTable("orders")
	if cache.Get("SELECT * FROM orders JOIN users ON 1=1", nil) != nil {
		t.Fatal("expected joined query to be invalidated")
	}
	if keys, ok := cache.tableIndex["users"]; ok && len(keys) > 0 {
		t.Fatal("expected users table index to be cleaned")
	}
}

func TestInvalidation_Stats(t *testing.T) {
	cache := makeTestCache(t, 100, true, true)
	cache.Put("SELECT * FROM orders", nil, []int{1}, nil)
	cache.InvalidateTable("orders")
	if cache.StatsInvalidations() != 1 {
		t.Fatalf("expected 1 invalidation, got %d", cache.StatsInvalidations())
	}
}

// --- Signal processing ---

func TestSignal_TableInvalidates(t *testing.T) {
	cache := makeTestCache(t, 100, true, true)
	cache.Put("SELECT * FROM orders", nil, []int{1}, nil)
	cache.processSignal("I:orders")
	if cache.Get("SELECT * FROM orders", nil) != nil {
		t.Fatal("expected invalidation via signal")
	}
}

func TestSignal_WildcardInvalidatesAll(t *testing.T) {
	cache := makeTestCache(t, 100, true, true)
	cache.Put("SELECT * FROM orders", nil, []int{1}, nil)
	cache.processSignal("I:*")
	if cache.Get("SELECT * FROM orders", nil) != nil {
		t.Fatal("expected wildcard invalidation")
	}
}

func TestSignal_KeepalivePreservesCache(t *testing.T) {
	cache := makeTestCache(t, 100, true, true)
	cache.Put("SELECT * FROM orders", nil, []int{1}, nil)
	cache.processSignal("P:")
	if cache.Get("SELECT * FROM orders", nil) == nil {
		t.Fatal("keepalive should not invalidate")
	}
}

func TestSignal_UnknownPreservesCache(t *testing.T) {
	cache := makeTestCache(t, 100, true, true)
	cache.Put("SELECT * FROM orders", nil, []int{1}, nil)
	cache.processSignal("X:something")
	if cache.Get("SELECT * FROM orders", nil) == nil {
		t.Fatal("unknown signal should not invalidate")
	}
}

// --- Push invalidation via socket ---

func TestPushInvalidation_RemoteSignalClearsCache(t *testing.T) {
	cache := makeTestCache(t, 100, true, false) // not connected yet

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port

	cache.ConnectInvalidation(port)

	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Wait for connection
	time.Sleep(100 * time.Millisecond)
	if !cache.Connected() {
		t.Fatal("expected connected")
	}

	// Put into cache (must do after connected)
	cache.Put("SELECT * FROM orders", nil, []int{1}, nil)

	conn.Write([]byte("I:orders\n"))
	time.Sleep(200 * time.Millisecond)

	if cache.Get("SELECT * FROM orders", nil) != nil {
		t.Fatal("expected orders to be invalidated via push")
	}

	cache.StopInvalidation()
}

func TestPushInvalidation_ConnectionDropClearsCache(t *testing.T) {
	cache := makeTestCache(t, 100, true, false) // not connected yet

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port

	cache.ConnectInvalidation(port)

	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	if !cache.Connected() {
		t.Fatal("expected connected")
	}

	// Put into cache (after connected)
	cache.Put("SELECT * FROM orders", nil, []int{1}, nil)

	// Drop connection
	conn.Close()
	ln.Close()
	time.Sleep(500 * time.Millisecond)

	if cache.Connected() {
		t.Fatal("expected disconnected after drop")
	}
	if cache.Size() != 0 {
		t.Fatalf("expected cache to be cleared, got %d entries", cache.Size())
	}

	cache.StopInvalidation()
}

func TestStopInvalidation_DoesNotBlock(t *testing.T) {
	cache := makeTestCache(t, 100, true, false)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port

	cache.ConnectInvalidation(port)

	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Wait for connected
	time.Sleep(100 * time.Millisecond)
	if !cache.Connected() {
		t.Fatal("expected connected")
	}

	// StopInvalidation should return quickly (not block for 30s read deadline)
	done := make(chan struct{})
	go func() {
		cache.StopInvalidation()
		close(done)
	}()

	select {
	case <-done:
		// Good — returned quickly
	case <-time.After(2 * time.Second):
		t.Fatal("StopInvalidation blocked for too long")
	}
}

// --- Thread safety ---

func TestConcurrentPutAndGet(t *testing.T) {
	cache := makeTestCache(t, 1000, true, true)
	var wg sync.WaitGroup
	errCh := make(chan error, 4)

	writer := func(start, count int) {
		defer wg.Done()
		for i := start; i < start+count; i++ {
			cache.Put("SELECT "+string(rune('A'+i%26)), nil, []int{i}, nil)
		}
	}

	reader := func(start, count int) {
		defer wg.Done()
		for i := start; i < start+count; i++ {
			cache.Get("SELECT "+string(rune('A'+i%26)), nil)
		}
	}

	wg.Add(4)
	go func() { writer(0, 200); errCh <- nil }()
	go func() { writer(200, 200); errCh <- nil }()
	go func() { reader(0, 200); errCh <- nil }()
	go func() { reader(200, 200); errCh <- nil }()
	wg.Wait()
}

func TestConcurrentInvalidation(t *testing.T) {
	cache := makeTestCache(t, 1000, true, true)
	for i := 0; i < 100; i++ {
		cache.Put("SELECT * FROM t"+string(rune('0'+i%10)), []interface{}{i}, []int{i}, nil)
	}

	var wg sync.WaitGroup

	invalidator := func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			cache.InvalidateTable("t" + string(rune('0'+i)))
		}
	}

	reader := func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			cache.Get("SELECT * FROM t"+string(rune('0'+i%10)), []interface{}{i})
		}
	}

	wg.Add(3)
	go invalidator()
	go reader()
	go reader()
	wg.Wait()
}

func TestStats_ConcurrentAccess(t *testing.T) {
	cache := makeTestCache(t, 1000, true, true)
	cache.Put("SELECT 1", nil, []int{1}, nil)

	var wg sync.WaitGroup
	wg.Add(3)

	// Concurrent reads (cache hits + misses)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			cache.Get("SELECT 1", nil)
			cache.Get("SELECT 2", nil)
		}
	}()

	// Concurrent stats reads
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = cache.StatsHits()
			_ = cache.StatsMisses()
			_ = cache.StatsInvalidations()
		}
	}()

	// Concurrent invalidations
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			cache.Put("SELECT temp", nil, []int{1}, nil)
			cache.InvalidateAll()
		}
	}()

	wg.Wait()
	// No race condition — test passes if -race doesn't flag anything
}

// --- Singleton ---

func TestGetNativeCache_ReturnsSame(t *testing.T) {
	ResetNativeCache()
	defer ResetNativeCache()
	c1 := GetNativeCache()
	c2 := GetNativeCache()
	if c1 != c2 {
		t.Fatal("expected same instance")
	}
}

func TestResetNativeCache_ClearsInstance(t *testing.T) {
	ResetNativeCache()
	c1 := GetNativeCache()
	ResetNativeCache()
	c2 := GetNativeCache()
	if c1 == c2 {
		t.Fatal("expected different instances after reset")
	}
}
