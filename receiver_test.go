package goldlapel

import (
	"testing"
)

// --- DB() getter tests ---

func TestDB_NilBeforeStart(t *testing.T) {
	gl := New("postgresql://user:pass@localhost:5432/mydb")
	if gl.DB() != nil {
		t.Fatal("expected DB() to return nil before Start()")
	}
}

func TestDB_NilAfterStop(t *testing.T) {
	gl := New("postgresql://user:pass@localhost:5432/mydb")
	// Manually set db to simulate post-start state, then clear it via Stop path
	gl.Stop()
	if gl.DB() != nil {
		t.Fatal("expected DB() to return nil after Stop()")
	}
}

// --- ErrNotConnected tests ---

func TestReceiverMethods_ErrNotConnected(t *testing.T) {
	gl := New("postgresql://user:pass@localhost:5432/mydb")

	// DocInsert
	_, err := gl.DocInsert("test", map[string]interface{}{"a": 1})
	if err != ErrNotConnected {
		t.Fatalf("DocInsert: expected ErrNotConnected, got %v", err)
	}

	// DocInsertMany
	_, err = gl.DocInsertMany("test", []interface{}{map[string]interface{}{"a": 1}})
	if err != ErrNotConnected {
		t.Fatalf("DocInsertMany: expected ErrNotConnected, got %v", err)
	}

	// DocFind
	_, err = gl.DocFind("test", nil)
	if err != ErrNotConnected {
		t.Fatalf("DocFind: expected ErrNotConnected, got %v", err)
	}

	// DocFindOne
	_, err = gl.DocFindOne("test", nil)
	if err != ErrNotConnected {
		t.Fatalf("DocFindOne: expected ErrNotConnected, got %v", err)
	}

	// DocUpdate
	_, err = gl.DocUpdate("test", map[string]interface{}{"a": 1}, map[string]interface{}{"a": 2})
	if err != ErrNotConnected {
		t.Fatalf("DocUpdate: expected ErrNotConnected, got %v", err)
	}

	// DocUpdateOne
	_, err = gl.DocUpdateOne("test", map[string]interface{}{"a": 1}, map[string]interface{}{"a": 2})
	if err != ErrNotConnected {
		t.Fatalf("DocUpdateOne: expected ErrNotConnected, got %v", err)
	}

	// DocDelete
	_, err = gl.DocDelete("test", map[string]interface{}{"a": 1})
	if err != ErrNotConnected {
		t.Fatalf("DocDelete: expected ErrNotConnected, got %v", err)
	}

	// DocDeleteOne
	_, err = gl.DocDeleteOne("test", map[string]interface{}{"a": 1})
	if err != ErrNotConnected {
		t.Fatalf("DocDeleteOne: expected ErrNotConnected, got %v", err)
	}

	// DocCount
	_, err = gl.DocCount("test", nil)
	if err != ErrNotConnected {
		t.Fatalf("DocCount: expected ErrNotConnected, got %v", err)
	}

	// DocCreateIndex
	err = gl.DocCreateIndex("test", "a")
	if err != ErrNotConnected {
		t.Fatalf("DocCreateIndex: expected ErrNotConnected, got %v", err)
	}

	// DocAggregate
	_, err = gl.DocAggregate("test", nil)
	if err != ErrNotConnected {
		t.Fatalf("DocAggregate: expected ErrNotConnected, got %v", err)
	}
}

func TestReceiverMethods_Search_ErrNotConnected(t *testing.T) {
	gl := New("postgresql://user:pass@localhost:5432/mydb")

	_, err := gl.Search("test", "col", "query")
	if err != ErrNotConnected {
		t.Fatalf("Search: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.SearchFuzzy("test", "col", "query")
	if err != ErrNotConnected {
		t.Fatalf("SearchFuzzy: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.SearchPhonetic("test", "col", "query")
	if err != ErrNotConnected {
		t.Fatalf("SearchPhonetic: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Similar("test", "col", []float64{0.1, 0.2})
	if err != ErrNotConnected {
		t.Fatalf("Similar: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Suggest("test", "col", "pre")
	if err != ErrNotConnected {
		t.Fatalf("Suggest: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Facets("test", "col")
	if err != ErrNotConnected {
		t.Fatalf("Facets: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Aggregate("test", "col", "count")
	if err != ErrNotConnected {
		t.Fatalf("Aggregate: expected ErrNotConnected, got %v", err)
	}

	err = gl.CreateSearchConfig("myconf", "english")
	if err != ErrNotConnected {
		t.Fatalf("CreateSearchConfig: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Analyze("some text")
	if err != ErrNotConnected {
		t.Fatalf("Analyze: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.ExplainScore("test", "col", "query", "id", 1)
	if err != ErrNotConnected {
		t.Fatalf("ExplainScore: expected ErrNotConnected, got %v", err)
	}
}

func TestReceiverMethods_Utils_ErrNotConnected(t *testing.T) {
	gl := New("postgresql://user:pass@localhost:5432/mydb")

	// Pub/Sub
	err := gl.Publish("ch", "msg")
	if err != ErrNotConnected {
		t.Fatalf("Publish: expected ErrNotConnected, got %v", err)
	}

	err = gl.Subscribe("ch", func(ch, p string) {})
	if err != ErrNotConnected {
		t.Fatalf("Subscribe: expected ErrNotConnected, got %v", err)
	}

	errCh := gl.SubscribeAsync("ch", func(ch, p string) {})
	err = <-errCh
	if err != ErrNotConnected {
		t.Fatalf("SubscribeAsync: expected ErrNotConnected, got %v", err)
	}

	// Queue
	err = gl.Enqueue("q", map[string]interface{}{"x": 1})
	if err != ErrNotConnected {
		t.Fatalf("Enqueue: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Dequeue("q")
	if err != ErrNotConnected {
		t.Fatalf("Dequeue: expected ErrNotConnected, got %v", err)
	}

	// Counter
	_, err = gl.Incr("counters", "k", 1)
	if err != ErrNotConnected {
		t.Fatalf("Incr: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.GetCounter("counters", "k")
	if err != ErrNotConnected {
		t.Fatalf("GetCounter: expected ErrNotConnected, got %v", err)
	}

	// Hash
	err = gl.Hset("h", "k", "f", "v")
	if err != ErrNotConnected {
		t.Fatalf("Hset: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Hget("h", "k", "f")
	if err != ErrNotConnected {
		t.Fatalf("Hget: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Hgetall("h", "k")
	if err != ErrNotConnected {
		t.Fatalf("Hgetall: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Hdel("h", "k", "f")
	if err != ErrNotConnected {
		t.Fatalf("Hdel: expected ErrNotConnected, got %v", err)
	}

	// Sorted set
	err = gl.Zadd("z", "m", 1.0)
	if err != ErrNotConnected {
		t.Fatalf("Zadd: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Zincrby("z", "m", 1.0)
	if err != ErrNotConnected {
		t.Fatalf("Zincrby: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Zrange("z", 0, 10, false)
	if err != ErrNotConnected {
		t.Fatalf("Zrange: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Zrank("z", "m", false)
	if err != ErrNotConnected {
		t.Fatalf("Zrank: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Zscore("z", "m")
	if err != ErrNotConnected {
		t.Fatalf("Zscore: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Zrem("z", "m")
	if err != ErrNotConnected {
		t.Fatalf("Zrem: expected ErrNotConnected, got %v", err)
	}

	// Geo
	_, err = gl.Georadius("g", "geom", 0, 0, 100, 10)
	if err != ErrNotConnected {
		t.Fatalf("Georadius: expected ErrNotConnected, got %v", err)
	}

	err = gl.Geoadd("g", "name", "geom", "place", 0, 0)
	if err != ErrNotConnected {
		t.Fatalf("Geoadd: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Geodist("g", "geom", "name", "a", "b")
	if err != ErrNotConnected {
		t.Fatalf("Geodist: expected ErrNotConnected, got %v", err)
	}

	// Misc
	_, err = gl.CountDistinct("t", "c")
	if err != ErrNotConnected {
		t.Fatalf("CountDistinct: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Script("return 1")
	if err != ErrNotConnected {
		t.Fatalf("Script: expected ErrNotConnected, got %v", err)
	}

	// Stream
	_, err = gl.StreamAdd("s", `{"x":1}`)
	if err != ErrNotConnected {
		t.Fatalf("StreamAdd: expected ErrNotConnected, got %v", err)
	}

	err = gl.StreamCreateGroup("s", "g")
	if err != ErrNotConnected {
		t.Fatalf("StreamCreateGroup: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.StreamRead("s", "g", "c", 10)
	if err != ErrNotConnected {
		t.Fatalf("StreamRead: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.StreamAck("s", "g", 1)
	if err != ErrNotConnected {
		t.Fatalf("StreamAck: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.StreamClaim("s", "g", "c", 1000)
	if err != ErrNotConnected {
		t.Fatalf("StreamClaim: expected ErrNotConnected, got %v", err)
	}

	// Percolate
	err = gl.PercolateAdd("p", "q1", "test query")
	if err != ErrNotConnected {
		t.Fatalf("PercolateAdd: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.Percolate("p", "some text")
	if err != ErrNotConnected {
		t.Fatalf("Percolate: expected ErrNotConnected, got %v", err)
	}

	_, err = gl.PercolateDelete("p", "q1")
	if err != ErrNotConnected {
		t.Fatalf("PercolateDelete: expected ErrNotConnected, got %v", err)
	}
}

func TestErrNotConnected_ErrorMessage(t *testing.T) {
	want := "goldlapel: proxy not started or database connection unavailable"
	if ErrNotConnected.Error() != want {
		t.Fatalf("expected error message %q, got %q", want, ErrNotConnected.Error())
	}
}
