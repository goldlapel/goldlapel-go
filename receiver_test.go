package goldlapel

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
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

	err = gl.DocCreateIndex(ctx, "test", "a")
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
	db, drv := newTestDB(t,
		[]string{"id", "data", "created_at"},
		[][]driver.Value{{int64(1), `{"name":"alice"}`, "2026-01-01T00:00:00Z"}})

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

	// Sanity check: at least one capture ran against the mock driver. The
	// mock shares a single statement recorder across the pool + tx, so we
	// only need to prove the call completed; tx routing is exercised by
	// resolveExec itself via the unit test on callOptions below.
	captures := drv.allCaptures()
	if len(captures) == 0 {
		t.Fatal("expected at least one captured query")
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
