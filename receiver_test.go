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

// When no database is connected, every receiver method must surface
// ErrNotConnected rather than panicking on a nil pool.
func TestReceiverMethods_ErrNotConnected(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	ctx := context.Background()

	_, err := gl.DocInsert(ctx, "test", map[string]interface{}{"a": 1})
	if err != ErrNotConnected {
		t.Fatalf("DocInsert: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.DocInsertMany(ctx, "test", []interface{}{map[string]interface{}{"a": 1}})
	if err != ErrNotConnected {
		t.Fatalf("DocInsertMany: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.DocFind(ctx, "test", nil)
	if err != ErrNotConnected {
		t.Fatalf("DocFind: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.DocFindOne(ctx, "test", nil)
	if err != ErrNotConnected {
		t.Fatalf("DocFindOne: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.DocUpdate(ctx, "test", map[string]interface{}{"a": 1}, map[string]interface{}{"a": 2})
	if err != ErrNotConnected {
		t.Fatalf("DocUpdate: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.DocUpdateOne(ctx, "test", map[string]interface{}{"a": 1}, map[string]interface{}{"a": 2})
	if err != ErrNotConnected {
		t.Fatalf("DocUpdateOne: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.DocDelete(ctx, "test", map[string]interface{}{"a": 1})
	if err != ErrNotConnected {
		t.Fatalf("DocDelete: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.DocDeleteOne(ctx, "test", map[string]interface{}{"a": 1})
	if err != ErrNotConnected {
		t.Fatalf("DocDeleteOne: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.DocCount(ctx, "test", nil)
	if err != ErrNotConnected {
		t.Fatalf("DocCount: expected ErrNotConnected, got %v", err)
	}

	err = gl.DocCreateIndex(ctx, "test", []string{"a"})
	if err != ErrNotConnected {
		t.Fatalf("DocCreateIndex: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.DocAggregate(ctx, "test", nil)
	if err != ErrNotConnected {
		t.Fatalf("DocAggregate: expected ErrNotConnected, got %v", err)
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

	err = gl.Enqueue(ctx, "q", map[string]interface{}{"x": 1})
	if err != ErrNotConnected {
		t.Fatalf("Enqueue: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Dequeue(ctx, "q")
	if err != ErrNotConnected {
		t.Fatalf("Dequeue: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Incr(ctx, "counters", "k", 1)
	if err != ErrNotConnected {
		t.Fatalf("Incr: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.GetCounter(ctx, "counters", "k")
	if err != ErrNotConnected {
		t.Fatalf("GetCounter: expected ErrNotConnected, got %v", err)
	}

	err = gl.Hset(ctx, "h", "k", "f", "v")
	if err != ErrNotConnected {
		t.Fatalf("Hset: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Hget(ctx, "h", "k", "f")
	if err != ErrNotConnected {
		t.Fatalf("Hget: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Hgetall(ctx, "h", "k")
	if err != ErrNotConnected {
		t.Fatalf("Hgetall: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Hdel(ctx, "h", "k", "f")
	if err != ErrNotConnected {
		t.Fatalf("Hdel: expected ErrNotConnected, got %v", err)
	}

	err = gl.Zadd(ctx, "z", "m", 1.0)
	if err != ErrNotConnected {
		t.Fatalf("Zadd: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Zincrby(ctx, "z", "m", 1.0)
	if err != ErrNotConnected {
		t.Fatalf("Zincrby: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Zrange(ctx, "z", 0, 10, false)
	if err != ErrNotConnected {
		t.Fatalf("Zrange: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Zrank(ctx, "z", "m", false)
	if err != ErrNotConnected {
		t.Fatalf("Zrank: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Zscore(ctx, "z", "m")
	if err != ErrNotConnected {
		t.Fatalf("Zscore: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Zrem(ctx, "z", "m")
	if err != ErrNotConnected {
		t.Fatalf("Zrem: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Georadius(ctx, "g", "geom", 0, 0, 100, 10)
	if err != ErrNotConnected {
		t.Fatalf("Georadius: expected ErrNotConnected, got %v", err)
	}

	err = gl.Geoadd(ctx, "g", "name", "geom", "place", 0, 0)
	if err != ErrNotConnected {
		t.Fatalf("Geoadd: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Geodist(ctx, "g", "geom", "name", "a", "b")
	if err != ErrNotConnected {
		t.Fatalf("Geodist: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.CountDistinct(ctx, "t", "c")
	if err != ErrNotConnected {
		t.Fatalf("CountDistinct: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Script(ctx, "return 1")
	if err != ErrNotConnected {
		t.Fatalf("Script: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.StreamAdd(ctx, "s", `{"x":1}`)
	if err != ErrNotConnected {
		t.Fatalf("StreamAdd: expected ErrNotConnected, got %v", err)
	}

	err = gl.StreamCreateGroup(ctx, "s", "g")
	if err != ErrNotConnected {
		t.Fatalf("StreamCreateGroup: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.StreamRead(ctx, "s", "g", "c", 10)
	if err != ErrNotConnected {
		t.Fatalf("StreamRead: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.StreamAck(ctx, "s", "g", 1)
	if err != ErrNotConnected {
		t.Fatalf("StreamAck: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.StreamClaim(ctx, "s", "g", "c", 1000)
	if err != ErrNotConnected {
		t.Fatalf("StreamClaim: expected ErrNotConnected, got %v", err)
	}

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

	_, err = gl.DocWatch(ctx, "test", func(op, data string) {})
	if err != ErrNotConnected {
		t.Fatalf("DocWatch: expected ErrNotConnected, got %v", err)
	}

	err = gl.DocUnwatch(ctx, "test")
	if err != ErrNotConnected {
		t.Fatalf("DocUnwatch: expected ErrNotConnected, got %v", err)
	}

	err = gl.DocCreateTtlIndex(ctx, "test", 3600)
	if err != ErrNotConnected {
		t.Fatalf("DocCreateTtlIndex: expected ErrNotConnected, got %v", err)
	}

	err = gl.DocRemoveTtlIndex(ctx, "test")
	if err != ErrNotConnected {
		t.Fatalf("DocRemoveTtlIndex: expected ErrNotConnected, got %v", err)
	}

	err = gl.DocCreateCapped(ctx, "test", 100)
	if err != ErrNotConnected {
		t.Fatalf("DocCreateCapped: expected ErrNotConnected, got %v", err)
	}

	err = gl.DocRemoveCap(ctx, "test")
	if err != ErrNotConnected {
		t.Fatalf("DocRemoveCap: expected ErrNotConnected, got %v", err)
	}
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

	ctx := context.Background()
	if _, err := gl.DocInsert(ctx, "users", map[string]interface{}{"name": "alice"}, WithTx(tx)); err != nil {
		t.Fatalf("DocInsert: %v", err)
	}

	// The DocInsert above should have fired through the transaction. Find
	// the INSERT among the captures (earlier captures include any schema
	// DDL DocInsert emits; we only assert the user-visible INSERT tag).
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
// TestWithTx_OverridesPool — without WithTx, the DocInsert must hit the
// pool, not any transaction. Guards against a regression where resolveExec
// leaks tx state across calls.
func TestWithTx_NoOverrideRoutesThroughPool(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		[][]driver.Value{{"11111111-1111-1111-1111-111111111111", `{"name":"bob"}`, "2026-01-01T00:00:00Z"}})

	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db

	ctx := context.Background()
	if _, err := gl.DocInsert(ctx, "users", map[string]interface{}{"name": "bob"}); err != nil {
		t.Fatalf("DocInsert: %v", err)
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

	ctx := context.Background()

	// Reset driver captures between the initial schema dance (if any)
	// and the measurement we care about. DocInsert/DocUpdate emit a few
	// CREATE TABLE IF NOT EXISTS statements the first time they touch a
	// collection — we don't want those muddying the assertions.
	drv.reset()

	err := gl.InTx(ctx, db, func(scoped *GoldLapel) error {
		if _, err := scoped.DocInsert(ctx, "users", map[string]interface{}{"name": "alice"}); err != nil {
			return err
		}
		if _, err := scoped.DocUpdate(ctx, "users",
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
// DocLimit/DocSkip on DocFind, which used to take `...DocFindOption` only.
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

	ctx := context.Background()

	// DocFind used to reject WithTx at compile time. Mixing doc options and
	// WithTx in one call must now be accepted.
	_, err = gl.DocFind(ctx, "users", nil,
		DocSort(map[string]int{"name": 1}),
		DocLimit(5),
		WithTx(tx))
	if err != nil {
		t.Fatalf("DocFind with WithTx: %v", err)
	}
}

// TestWithTxOnDocCreateIndex confirms DocCreateIndex (which previously took
// `keys ...string`) now accepts WithTx as a trailing functional option.
func TestWithTxOnDocCreateIndex(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db

	ctx := context.Background()
	if err := gl.DocCreateIndex(ctx, "users", []string{"email"}, WithTx(tx)); err != nil {
		t.Fatalf("DocCreateIndex with WithTx: %v", err)
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
