package goldlapel

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

// --- DB() getter tests ---

func TestDB_NilBeforeStart(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	if gl.DB() != nil {
		t.Fatal("expected DB() to return nil before Start()")
	}
}

func TestDB_NilAfterStop(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.Stop(context.Background())
	if gl.DB() != nil {
		t.Fatal("expected DB() to return nil after Stop()")
	}
}

// --- ErrNotConnected tests ---

// docPatternsFor returns a pattern entry whose tables.main is set to
// `collection` itself. Pre-populating gl.ddlCache with this entry makes
// gl.Documents.* run against the user-supplied collection name (instead
// of the proxy's canonical _goldlapel.doc_<name>), so test SQL assertions
// that expect "INSERT INTO users" / "SELECT … FROM users" keep matching
// without standing up a fake dashboard for every test.
func docPatternsFor(collection string) *DdlEntry {
	return &DdlEntry{
		Tables: map[string]string{"main": collection},
		// The doc-store family only consumes tables.main today; query
		// patterns are reserved for future use.
		QueryPatterns: map[string]string{},
	}
}

// When the proxy never started, gl.Documents.* still tries to fetch DDL
// patterns first — which fails with a "No dashboard token / port"
// RuntimeError. We accept either ErrNotConnected (cache pre-populated +
// resolveExec fails on nil db) or the dashboard-not-ready text.
func isDocStoreNotReady(t *testing.T, name string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected an error, got nil", name)
	}
	if err == ErrNotConnected {
		return
	}
	msg := err.Error()
	if strings.Contains(msg, "No dashboard token") ||
		strings.Contains(msg, "No dashboard port") ||
		strings.Contains(msg, "dashboard not reachable") {
		return
	}
	t.Fatalf("%s: expected ErrNotConnected or dashboard-not-ready error, got %v", name, err)
}

// When no database is connected, every namespace method must surface
// ErrNotConnected (or the dashboard-not-ready variant — see
// isDocStoreNotReady) rather than panicking on a nil pool.
func TestDocumentsNamespace_ErrNotConnected(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	// Pre-populate the cache so the doc-store path skips its HTTP fetch
	// and we exercise the actual ErrNotConnected branch — without this
	// every assertion below would surface the dashboard-not-ready error
	// instead of the connection error we want to verify.
	gl.ddlCache.Store("doc_store:test", docPatternsFor("test"))
	ctx := context.Background()

	_, err := gl.Documents.Insert(ctx, "test", map[string]interface{}{"a": 1})
	if err != ErrNotConnected {
		t.Fatalf("Documents.Insert: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Documents.InsertMany(ctx, "test", []interface{}{map[string]interface{}{"a": 1}})
	if err != ErrNotConnected {
		t.Fatalf("Documents.InsertMany: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Documents.Find(ctx, "test", nil)
	if err != ErrNotConnected {
		t.Fatalf("Documents.Find: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Documents.FindOne(ctx, "test", nil)
	if err != ErrNotConnected {
		t.Fatalf("Documents.FindOne: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Documents.Update(ctx, "test", map[string]interface{}{"a": 1}, map[string]interface{}{"a": 2})
	if err != ErrNotConnected {
		t.Fatalf("Documents.Update: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Documents.UpdateOne(ctx, "test", map[string]interface{}{"a": 1}, map[string]interface{}{"a": 2})
	if err != ErrNotConnected {
		t.Fatalf("Documents.UpdateOne: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Documents.Delete(ctx, "test", map[string]interface{}{"a": 1})
	if err != ErrNotConnected {
		t.Fatalf("Documents.Delete: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Documents.DeleteOne(ctx, "test", map[string]interface{}{"a": 1})
	if err != ErrNotConnected {
		t.Fatalf("Documents.DeleteOne: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Documents.Count(ctx, "test", nil)
	if err != ErrNotConnected {
		t.Fatalf("Documents.Count: expected ErrNotConnected, got %v", err)
	}

	err = gl.Documents.CreateIndex(ctx, "test", []string{"a"})
	if err != ErrNotConnected {
		t.Fatalf("Documents.CreateIndex: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Documents.Aggregate(ctx, "test", nil)
	if err != ErrNotConnected {
		t.Fatalf("Documents.Aggregate: expected ErrNotConnected, got %v", err)
	}
}

func TestReceiverMethods_Search_ErrNotConnected(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	ctx := context.Background()

	_, err := gl.Search(ctx, "test", "col", "query")
	if err != ErrNotConnected {
		t.Fatalf("Search: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.SearchFuzzy(ctx, "test", "col", "query")
	if err != ErrNotConnected {
		t.Fatalf("SearchFuzzy: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.SearchPhonetic(ctx, "test", "col", "query")
	if err != ErrNotConnected {
		t.Fatalf("SearchPhonetic: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Similar(ctx, "test", "col", []float64{0.1, 0.2})
	if err != ErrNotConnected {
		t.Fatalf("Similar: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Suggest(ctx, "test", "col", "pre")
	if err != ErrNotConnected {
		t.Fatalf("Suggest: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Facets(ctx, "test", "col")
	if err != ErrNotConnected {
		t.Fatalf("Facets: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Aggregate(ctx, "test", "col", "count")
	if err != ErrNotConnected {
		t.Fatalf("Aggregate: expected ErrNotConnected, got %v", err)
	}

	err = gl.CreateSearchConfig(ctx, "myconf", "english")
	if err != ErrNotConnected {
		t.Fatalf("CreateSearchConfig: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Analyze(ctx, "some text")
	if err != ErrNotConnected {
		t.Fatalf("Analyze: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.ExplainScore(ctx, "test", "col", "query", "id", 1)
	if err != ErrNotConnected {
		t.Fatalf("ExplainScore: expected ErrNotConnected, got %v", err)
	}
}

func TestReceiverMethods_Utils_ErrNotConnected(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	ctx := context.Background()

	err := gl.Publish(ctx, "ch", "msg")
	if err != ErrNotConnected {
		t.Fatalf("Publish: expected ErrNotConnected, got %v", err)
	}

	err = gl.Subscribe(ctx, "ch", func(ch, p string) {})
	if err != ErrNotConnected {
		t.Fatalf("Subscribe: expected ErrNotConnected, got %v", err)
	}

	errCh := gl.SubscribeAsync(ctx, "ch", func(ch, p string) {})
	err = <-errCh
	if err != ErrNotConnected {
		t.Fatalf("SubscribeAsync: expected ErrNotConnected, got %v", err)
	}

	// Phase 5 helper namespaces (Counters / Zsets / Hashes / Queues / Geos)
	// fetch DDL patterns before touching the connection — on an unstarted
	// instance they raise a dashboard-not-ready error rather than
	// ErrNotConnected. Accept either via the same helper that covers
	// Streams below.
	isStreamNotReady := func(name string, err error) {
		if err == nil {
			t.Fatalf("%s: expected an error, got nil", name)
		}
		if err == ErrNotConnected {
			return
		}
		msg := err.Error()
		if strings.Contains(msg, "No dashboard token") ||
			strings.Contains(msg, "No dashboard port") ||
			strings.Contains(msg, "dashboard not reachable") {
			return
		}
		t.Fatalf("%s: expected ErrNotConnected or dashboard-not-ready error, got %v", name, err)
	}

	_, err = gl.Counters.Incr(ctx, "counters", "k", 1)
	isStreamNotReady("Counters.Incr", err)

	_, err = gl.Counters.Get(ctx, "counters", "k")
	isStreamNotReady("Counters.Get", err)

	_, err = gl.Counters.Set(ctx, "counters", "k", 1)
	isStreamNotReady("Counters.Set", err)

	_, err = gl.Counters.Delete(ctx, "counters", "k")
	isStreamNotReady("Counters.Delete", err)

	_, err = gl.Counters.CountKeys(ctx, "counters")
	isStreamNotReady("Counters.CountKeys", err)

	_, err = gl.Hashes.Set(ctx, "h", "k", "f", "v")
	isStreamNotReady("Hashes.Set", err)

	_, err = gl.Hashes.Get(ctx, "h", "k", "f")
	isStreamNotReady("Hashes.Get", err)

	_, err = gl.Hashes.GetAll(ctx, "h", "k")
	isStreamNotReady("Hashes.GetAll", err)

	_, err = gl.Hashes.Delete(ctx, "h", "k", "f")
	isStreamNotReady("Hashes.Delete", err)

	_, err = gl.Zsets.Add(ctx, "z", "k", "m", 1.0)
	isStreamNotReady("Zsets.Add", err)

	_, err = gl.Zsets.IncrBy(ctx, "z", "k", "m", 1.0)
	isStreamNotReady("Zsets.IncrBy", err)

	_, err = gl.Zsets.Range(ctx, "z", "k", 0, 10, false)
	isStreamNotReady("Zsets.Range", err)

	_, _, err = gl.Zsets.Rank(ctx, "z", "k", "m", false)
	isStreamNotReady("Zsets.Rank", err)

	_, _, err = gl.Zsets.Score(ctx, "z", "k", "m")
	isStreamNotReady("Zsets.Score", err)

	_, err = gl.Zsets.Remove(ctx, "z", "k", "m")
	isStreamNotReady("Zsets.Remove", err)

	_, err = gl.Queues.Enqueue(ctx, "q", map[string]interface{}{"x": 1})
	isStreamNotReady("Queues.Enqueue", err)

	_, err = gl.Queues.Claim(ctx, "q", 30000)
	isStreamNotReady("Queues.Claim", err)

	_, err = gl.Queues.Ack(ctx, "q", 1)
	isStreamNotReady("Queues.Ack", err)

	_, err = gl.Geos.Radius(ctx, "g", 0, 0, 100, "m", 10)
	isStreamNotReady("Geos.Radius", err)

	_, err = gl.Geos.Add(ctx, "g", "place", 0, 0)
	isStreamNotReady("Geos.Add", err)

	_, _, err = gl.Geos.Dist(ctx, "g", "a", "b", "m")
	isStreamNotReady("Geos.Dist", err)

	_, err = gl.CountDistinct(ctx, "t", "c")
	if err != ErrNotConnected {
		t.Fatalf("CountDistinct: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Script(ctx, "return 1")
	if err != ErrNotConnected {
		t.Fatalf("Script: expected ErrNotConnected, got %v", err)
	}

	// gl.Streams.* and gl.Documents.* now fetch DDL patterns before touching
	// the connection; an unstarted instance raises a RuntimeError-equivalent
	// ("No dashboard token / port") rather than ErrNotConnected. Accept
	// either via the same isStreamNotReady helper defined above.
	_, err = gl.Streams.Add(ctx, "s", `{"x":1}`)
	isStreamNotReady("Streams.Add", err)

	err = gl.Streams.CreateGroup(ctx, "s", "g")
	isStreamNotReady("Streams.CreateGroup", err)

	_, err = gl.Streams.Read(ctx, "s", "g", "c", 10)
	isStreamNotReady("Streams.Read", err)

	_, err = gl.Streams.Ack(ctx, "s", "g", 1)
	isStreamNotReady("Streams.Ack", err)

	_, err = gl.Streams.Claim(ctx, "s", "g", "c", 1000)
	isStreamNotReady("Streams.Claim", err)

	err = gl.PercolateAdd(ctx, "p", "q1", "test query")
	if err != ErrNotConnected {
		t.Fatalf("PercolateAdd: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Percolate(ctx, "p", "some text")
	if err != ErrNotConnected {
		t.Fatalf("Percolate: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.PercolateDelete(ctx, "p", "q1")
	if err != ErrNotConnected {
		t.Fatalf("PercolateDelete: expected ErrNotConnected, got %v", err)
	}

	// Documents.Watch/Unwatch fetches doc-store DDL first, then opens the
	// LISTEN connection — so on an unstarted instance it surfaces the
	// dashboard-not-ready error before reaching the URL check.
	_, err = gl.Documents.Watch(ctx, "test", func(op, data string) {})
	isDocStoreNotReady(t, "Documents.Watch", err)

	err = gl.Documents.Unwatch(ctx, "test")
	isDocStoreNotReady(t, "Documents.Unwatch", err)

	err = gl.Documents.CreateTtlIndex(ctx, "test", 3600)
	isDocStoreNotReady(t, "Documents.CreateTtlIndex", err)

	err = gl.Documents.RemoveTtlIndex(ctx, "test")
	isDocStoreNotReady(t, "Documents.RemoveTtlIndex", err)

	err = gl.Documents.CreateCapped(ctx, "test", 100)
	isDocStoreNotReady(t, "Documents.CreateCapped", err)

	err = gl.Documents.RemoveCap(ctx, "test")
	isDocStoreNotReady(t, "Documents.RemoveCap", err)
}

func TestErrNotConnected_ErrorMessage(t *testing.T) {
	want := "goldlapel: proxy not started or database connection unavailable"
	if ErrNotConnected.Error() != want {
		t.Fatalf("expected error message %q, got %q", want, ErrNotConnected.Error())
	}
}

// --- Option resolution & WithTx ---

func TestWithTx_OverridesPool(t *testing.T) {
	// Build a *sql.DB + *sql.Tx backed by the mock driver so we can
	// prove WithTx(tx) routes a call at the transaction instead of the pool.
	// The enhanced mock tags every capture with inTx=true iff the statement
	// fired while a transaction was open on its conn — so we can assert
	// the happy-path routing, not just "a query happened".
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		[][]driver.Value{{"11111111-1111-1111-1111-111111111111", `{"name":"alice"}`, "2026-01-01T00:00:00Z"}})

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	// Set gl.db so resolveExec doesn't fall through to ErrNotConnected.
	gl.db = db
	// Pre-populate the doc-store DDL cache so the proxy round-trip is
	// skipped — Documents.Insert resolves "users" → "users" and emits
	// the same INSERT INTO users SQL the legacy gl.DocInsert did.
	gl.ddlCache.Store("doc_store:users", docPatternsFor("users"))

	ctx := context.Background()
	if _, err := gl.Documents.Insert(ctx, "users", map[string]interface{}{"name": "alice"}, WithTx(tx)); err != nil {
		t.Fatalf("Documents.Insert: %v", err)
	}

	// The Documents.Insert above should have fired through the transaction.
	// Find the INSERT among the captures.
	captures := drv.allCaptures()
	if len(captures) == 0 {
		t.Fatal("expected at least one captured query")
	}
	foundInsertInTx := false
	for _, c := range captures {
		if strings.Contains(strings.ToUpper(c.query), "INSERT INTO") && c.inTx {
			foundInsertInTx = true
			break
		}
	}
	if !foundInsertInTx {
		t.Fatalf("expected INSERT INTO to execute inside tx, but all captures were on the pool:\n%+v", captures)
	}
}

// TestWithTx_NoOverrideRoutesThroughPool is the negative of
// TestWithTx_OverridesPool — without WithTx, the Documents.Insert must
// hit the pool, not any transaction. Guards against a regression where
// resolveExec leaks tx state across calls.
func TestWithTx_NoOverrideRoutesThroughPool(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		[][]driver.Value{{"11111111-1111-1111-1111-111111111111", `{"name":"bob"}`, "2026-01-01T00:00:00Z"}})

	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("doc_store:users", docPatternsFor("users"))

	ctx := context.Background()
	if _, err := gl.Documents.Insert(ctx, "users", map[string]interface{}{"name": "bob"}); err != nil {
		t.Fatalf("Documents.Insert: %v", err)
	}

	for _, c := range drv.allCaptures() {
		if c.inTx {
			t.Fatalf("expected all captures on the pool (inTx=false), got inTx=true for: %q", c.query)
		}
	}
}

func TestResolveExec_WithTx(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db

	// With no override, we should get the pool.
	got, err := gl.resolveExec(nil)
	if err != nil {
		t.Fatalf("resolveExec(nil): %v", err)
	}
	if got != db {
		t.Fatalf("expected db pool, got %T", got)
	}

	// With WithTx override, we should get the transaction.
	got, err = gl.resolveExec([]Option{WithTx(tx)})
	if err != nil {
		t.Fatalf("resolveExec(WithTx): %v", err)
	}
	if got != tx {
		t.Fatalf("expected *sql.Tx, got %T", got)
	}
}

func TestResolveExec_ErrNotConnected(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	if _, err := gl.resolveExec(nil); err != ErrNotConnected {
		t.Fatalf("expected ErrNotConnected, got %v", err)
	}
}

// --- InTx tests ---

func TestInTx_CommitsOnSuccess(t *testing.T) {
	db := openTxDB(t)

	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db

	ctx := context.Background()
	called := false
	err := gl.InTx(ctx, db, func(scoped *GoldLapel) error {
		called = true
		if scoped.tx == nil {
			t.Fatal("expected scoped gl to carry a tx")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("InTx: %v", err)
	}
	if !called {
		t.Fatal("expected callback to run")
	}
}

// TestWithTx_HappyPathCommit is the end-to-end happy-path test the v0.2
// review flagged as missing. Previous coverage only had the mock-level
// 'a query ran' smoke test (TestWithTx_OverridesPool) and the integration
// suite — neither verified the full lifecycle inside InTx. With the
// enhanced mock driver (captures tagged inTx, commit/rollback counters
// on the driver), we can now assert:
//
//  1. InTx calls Begin on the conn (inTx flips to true).
//  2. Every gl.* call inside the closure routes through the tx — every
//     capture carries inTx=true.
//  3. On successful return, Commit fires exactly once; Rollback never
//     fires.
func TestWithTx_HappyPathCommit(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		[][]driver.Value{{"11111111-1111-1111-1111-111111111111", `{"x":1}`, "2026-01-01T00:00:00Z"}})

	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("doc_store:users", docPatternsFor("users"))

	ctx := context.Background()

	// Reset driver captures between any prior setup and the measurement we
	// care about — Documents.Insert/Update no longer emits its own CREATE
	// TABLE (the proxy owns that), but resetting before the measurement
	// keeps the assertions hermetic.
	drv.reset()

	err := gl.InTx(ctx, db, func(scoped *GoldLapel) error {
		if _, err := scoped.Documents.Insert(ctx, "users", map[string]interface{}{"name": "alice"}); err != nil {
			return err
		}
		if _, err := scoped.Documents.Update(ctx, "users",
			map[string]interface{}{"name": "alice"},
			map[string]interface{}{"name": "alice2"}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("InTx returned error: %v", err)
	}

	captures := drv.allCaptures()
	if len(captures) == 0 {
		t.Fatal("expected queries to have fired inside InTx")
	}

	// Every captured statement must be inside the tx.
	for _, c := range captures {
		if !c.inTx {
			t.Errorf("expected inTx=true for all statements, got inTx=false for: %q", c.query)
		}
	}

	// Commit fired exactly once; Rollback never fired.
	drv.mu.Lock()
	commits := drv.commits
	rollbacks := drv.rollbacks
	drv.mu.Unlock()
	if commits != 1 {
		t.Fatalf("expected 1 commit, got %d", commits)
	}
	if rollbacks != 0 {
		t.Fatalf("expected 0 rollbacks, got %d", rollbacks)
	}
}

// TestWithTx_RollbackOnError is the symmetric negative: on error, Commit
// must NOT fire; Rollback must fire exactly once. Complements
// TestInTx_RollsBackOnError (which only checks the error value, not the
// driver-level lifecycle).
func TestWithTx_RollbackOnError(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db

	sentinel := errors.New("boom")
	err := gl.InTx(context.Background(), db, func(scoped *GoldLapel) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel, got %v", err)
	}

	drv.mu.Lock()
	commits := drv.commits
	rollbacks := drv.rollbacks
	drv.mu.Unlock()
	if commits != 0 {
		t.Fatalf("expected 0 commits on error path, got %d", commits)
	}
	if rollbacks != 1 {
		t.Fatalf("expected 1 rollback on error path, got %d", rollbacks)
	}
}

func TestInTx_RollsBackOnError(t *testing.T) {
	db := openTxDB(t)

	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db

	sentinel := errors.New("boom")
	err := gl.InTx(context.Background(), db, func(scoped *GoldLapel) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
}

func TestInTx_ErrNotConnected(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	err := gl.InTx(context.Background(), nil, func(scoped *GoldLapel) error { return nil })
	if err != ErrNotConnected {
		t.Fatalf("expected ErrNotConnected, got %v", err)
	}
}

func TestInTx_RollsBackOnPanic(t *testing.T) {
	db := openTxDB(t)
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic to propagate")
		}
	}()
	_ = gl.InTx(context.Background(), db, func(scoped *GoldLapel) error {
		panic("nope")
	})
}

// openTxDB returns a *sql.DB wired to the mock driver with Begin support.
func openTxDB(t *testing.T) *sql.DB {
	t.Helper()
	db, _ := newTestDB(t, nil, nil)
	return db
}

// --- v0.2 WithTx regression on specialised option methods ---

// TestWithTxOnSearch proves that WithTx is accepted — and honoured — by the
// search-family methods that used to take `...SearchOption` only. Before the
// unification these methods had no way to route at a specific transaction.
func TestWithTxOnSearch(t *testing.T) {
	db, drv := newTestDB(t, []string{"id", "title", "_score"}, nil)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db

	ctx := context.Background()

	// Search with both a search option AND WithTx — both must be accepted
	// and both must take effect.
	_, err = gl.Search(ctx, "articles", "title", "postgres", WithLimit(7), WithTx(tx))
	if err != nil {
		t.Fatalf("Search with WithTx: %v", err)
	}

	captures := drv.allCaptures()
	if len(captures) == 0 {
		t.Fatal("expected a captured query")
	}
	last := captures[len(captures)-1]
	// WithLimit(7) must have been applied as the trailing LIMIT parameter.
	if last.args[len(last.args)-1] != int64(7) {
		t.Fatalf("expected limit=7 as last param, got %v", last.args[len(last.args)-1])
	}
}

// TestWithTxOnDocFind proves that WithTx is accepted alongside DocSort/
// DocLimit/DocSkip on Documents.Find, which used to take `...DocFindOption`
// only.
func TestWithTxOnDocFind(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("doc_store:users", docPatternsFor("users"))

	ctx := context.Background()

	// Documents.Find used to reject WithTx at compile time. Mixing doc
	// options and WithTx in one call must now be accepted.
	_, err = gl.Documents.Find(ctx, "users", nil,
		DocSort(map[string]int{"name": 1}),
		DocLimit(5),
		WithTx(tx))
	if err != nil {
		t.Fatalf("Documents.Find with WithTx: %v", err)
	}
}

// TestWithTxOnDocCreateIndex confirms Documents.CreateIndex accepts WithTx
// as a trailing functional option.
func TestWithTxOnDocCreateIndex(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("doc_store:users", docPatternsFor("users"))

	ctx := context.Background()
	if err := gl.Documents.CreateIndex(ctx, "users", []string{"email"}, WithTx(tx)); err != nil {
		t.Fatalf("Documents.CreateIndex with WithTx: %v", err)
	}

	// Sanity: the statement actually ran.
	if len(drv.allCaptures()) == 0 {
		t.Fatal("expected a captured CREATE INDEX query")
	}
}

// TestResolveExec_WithTxOnSearchOptions verifies that at the resolveExec
// level, passing WithTx together with a search-only option picks up the
// transaction. This catches any regression where search-only options would
// shadow the WithTx apply-path.
func TestResolveExec_WithTxAndSearchOption(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db

	got, err := gl.resolveExec([]Option{WithLimit(10), WithTx(tx), WithLang("french")})
	if err != nil {
		t.Fatalf("resolveExec: %v", err)
	}
	if got != tx {
		t.Fatalf("expected tx, got %T", got)
	}
}

// --- Scoped Stop/Close no-op regression ---

// TestScopedStopIsNoop guards against the footgun where a caller accidentally
// calls Stop on the scoped *GoldLapel passed into an InTx closure. Before the
// fix, that would SIGTERM the parent proxy process and close the shared pool,
// nuking the parent instance. Now it must be a no-op: the parent's done
// channel stays open and its db pool stays usable.
func TestScopedStopIsNoop(t *testing.T) {
	db := openTxDB(t)
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db

	// Simulate a running parent: give it a non-nil done channel so we can
	// detect whether scoped.Stop would have closed it.
	gl.done = make(chan struct{})
	defer close(gl.done) // cleanup after the test

	ctx := context.Background()
	err := gl.InTx(ctx, db, func(scoped *GoldLapel) error {
		// The scoped instance must carry the tx and share the parent's pool.
		if scoped.tx == nil {
			t.Fatal("scoped gl missing tx")
		}
		if scoped.db != db {
			t.Fatal("scoped gl should share the parent's db")
		}

		// Calling Stop on the scoped instance must NOT kill the parent.
		if err := scoped.Stop(ctx); err != nil {
			t.Fatalf("scoped.Stop: %v", err)
		}
		// Calling Close on the scoped instance must also be a no-op.
		if err := scoped.Close(); err != nil {
			t.Fatalf("scoped.Close: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("InTx: %v", err)
	}

	// Parent's done channel must still be open — scoped.Stop must not
	// have touched it.
	select {
	case <-gl.done:
		t.Fatal("parent done was closed by scoped Stop — parent got torn down")
	default:
	}

	// Parent's db pool must still be usable.
	if gl.db == nil {
		t.Fatal("parent db was nilled by scoped Stop")
	}
	if err := gl.db.PingContext(ctx); err != nil {
		t.Fatalf("parent db pool unusable after scoped Stop: %v", err)
	}
}

// TestScopedStop_AfterInTxCommit ensures a scoped instance retained past the
// InTx callback (e.g. captured into a variable) still cannot affect the
// parent if Stop is called on it.
func TestScopedStop_RetainedInstanceIsNoop(t *testing.T) {
	db := openTxDB(t)
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.done = make(chan struct{})
	defer close(gl.done)

	var captured *GoldLapel
	ctx := context.Background()
	if err := gl.InTx(ctx, db, func(scoped *GoldLapel) error {
		captured = scoped
		return nil
	}); err != nil {
		t.Fatalf("InTx: %v", err)
	}

	if captured == nil || captured.tx == nil {
		t.Fatal("expected captured scoped instance with tx set")
	}

	if err := captured.Stop(ctx); err != nil {
		t.Fatalf("captured.Stop: %v", err)
	}

	// Parent still alive.
	select {
	case <-gl.done:
		t.Fatal("parent done closed by retained scoped Stop")
	default:
	}
	if gl.db == nil {
		t.Fatal("parent db nilled by retained scoped Stop")
	}
}
