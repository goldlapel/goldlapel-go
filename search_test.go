package goldlapel

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
)

// --- Mock SQL driver that captures queries and params ---

type capturedQuery struct {
	query string
	args  []driver.Value
}

type mockDriver struct {
	mu       sync.Mutex
	captures []capturedQuery
	// columns/rows returned by mock queries
	columns []string
	rows    [][]driver.Value
}

func (d *mockDriver) Open(name string) (driver.Conn, error) {
	return &mockConn{drv: d}, nil
}

func (d *mockDriver) lastCapture() capturedQuery {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.captures) == 0 {
		return capturedQuery{}
	}
	return d.captures[len(d.captures)-1]
}

func (d *mockDriver) allCaptures() []capturedQuery {
	d.mu.Lock()
	defer d.mu.Unlock()
	cp := make([]capturedQuery, len(d.captures))
	copy(cp, d.captures)
	return cp
}

func (d *mockDriver) reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.captures = nil
}

type mockConn struct {
	drv *mockDriver
}

func (c *mockConn) Prepare(query string) (driver.Stmt, error) {
	return &mockStmt{conn: c, query: query}, nil
}
func (c *mockConn) Close() error               { return nil }
func (c *mockConn) Begin() (driver.Tx, error)   { return &mockTx{}, nil }

type mockTx struct{}

func (t *mockTx) Commit() error   { return nil }
func (t *mockTx) Rollback() error { return nil }

type mockStmt struct {
	conn  *mockConn
	query string
}

func (s *mockStmt) Close() error { return nil }
func (s *mockStmt) NumInput() int { return -1 } // accept any number of args

func (s *mockStmt) Exec(args []driver.Value) (driver.Result, error) {
	s.conn.drv.mu.Lock()
	s.conn.drv.captures = append(s.conn.drv.captures, capturedQuery{query: s.query, args: args})
	s.conn.drv.mu.Unlock()
	return mockResult{}, nil
}

func (s *mockStmt) Query(args []driver.Value) (driver.Rows, error) {
	s.conn.drv.mu.Lock()
	s.conn.drv.captures = append(s.conn.drv.captures, capturedQuery{query: s.query, args: args})
	cols := s.conn.drv.columns
	rows := s.conn.drv.rows
	s.conn.drv.mu.Unlock()

	if cols == nil {
		cols = []string{"value"}
	}
	return &mockSQLRows{columns: cols, rows: rows, pos: -1}, nil
}

type mockResult struct{}

func (r mockResult) LastInsertId() (int64, error) { return 0, nil }
func (r mockResult) RowsAffected() (int64, error) { return 1, nil }

type mockSQLRows struct {
	columns []string
	rows    [][]driver.Value
	pos     int
}

func (r *mockSQLRows) Columns() []string { return r.columns }
func (r *mockSQLRows) Close() error      { return nil }
func (r *mockSQLRows) Next(dest []driver.Value) error {
	r.pos++
	if r.pos >= len(r.rows) {
		return io.EOF
	}
	for i, v := range r.rows[r.pos] {
		if i < len(dest) {
			dest[i] = v
		}
	}
	return nil
}

// driverCounter ensures each test gets a unique driver name to avoid conflicts.
var driverCounter int
var driverCounterMu sync.Mutex

func newTestDB(t *testing.T, columns []string, rows [][]driver.Value) (*sql.DB, *mockDriver) {
	t.Helper()
	drv := &mockDriver{columns: columns, rows: rows}

	driverCounterMu.Lock()
	driverCounter++
	name := fmt.Sprintf("mockdrv_%d", driverCounter)
	driverCounterMu.Unlock()

	sql.Register(name, drv)
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, drv
}

// assertContains checks that haystack contains needle (case-insensitive).
func assertContains(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(strings.ToLower(haystack), strings.ToLower(needle)) {
		t.Fatalf("expected SQL to contain %q, got:\n%s", needle, haystack)
	}
}

// assertNotContains checks that haystack does NOT contain needle.
func assertNotContains(t *testing.T, haystack, needle string) {
	t.Helper()
	if strings.Contains(strings.ToLower(haystack), strings.ToLower(needle)) {
		t.Fatalf("expected SQL NOT to contain %q, got:\n%s", needle, haystack)
	}
}

// --- Search ---

func TestSearch_SingleColumn_SQLGeneration(t *testing.T) {
	db, drv := newTestDB(t, []string{"id", "title", "_score"}, nil)

	_, err := Search(db, "articles", "title", "hello world")
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "to_tsvector($1, coalesce(title, ''))")
	assertContains(t, last.query, "plainto_tsquery($2, $3)")
	assertContains(t, last.query, "ts_rank")
	assertContains(t, last.query, "FROM articles")
	assertContains(t, last.query, "ORDER BY _score DESC")
	assertContains(t, last.query, "LIMIT $4")

	// Params: lang, lang, query, limit
	if len(last.args) != 4 {
		t.Fatalf("expected 4 params, got %d: %v", len(last.args), last.args)
	}
	if last.args[0] != "english" || last.args[1] != "english" {
		t.Fatalf("expected lang='english', got %v, %v", last.args[0], last.args[1])
	}
	if last.args[2] != "hello world" {
		t.Fatalf("expected query='hello world', got %v", last.args[2])
	}
	if last.args[3] != int64(50) {
		t.Fatalf("expected limit=50, got %v", last.args[3])
	}
}

func TestSearch_MultiColumn_CoalesceWrapping(t *testing.T) {
	db, drv := newTestDB(t, []string{"id", "_score"}, nil)

	_, err := Search(db, "articles", []string{"title", "body"}, "test")
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "coalesce(title, '')")
	assertContains(t, last.query, "coalesce(body, '')")
	assertContains(t, last.query, "|| ' ' ||")
}

func TestSearch_WithHighlight(t *testing.T) {
	db, drv := newTestDB(t, []string{"id", "_score", "_highlight"}, nil)

	_, err := Search(db, "articles", "title", "hello", WithHighlight(true))
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "ts_headline")
	assertContains(t, last.query, "StartSel=<mark>")
	assertContains(t, last.query, "_highlight")
	// With highlight: lang, lang, query, lang, limit = 5 params
	if len(last.args) != 5 {
		t.Fatalf("expected 5 params with highlight, got %d: %v", len(last.args), last.args)
	}
}

func TestSearch_WithLimit(t *testing.T) {
	db, drv := newTestDB(t, []string{"id", "_score"}, nil)

	_, err := Search(db, "articles", "title", "test", WithLimit(10))
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	// Last param should be limit=10
	if last.args[len(last.args)-1] != int64(10) {
		t.Fatalf("expected limit=10, got %v", last.args[len(last.args)-1])
	}
}

func TestSearch_WithLang(t *testing.T) {
	db, drv := newTestDB(t, []string{"id", "_score"}, nil)

	_, err := Search(db, "articles", "title", "bonjour", WithLang("french"))
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	if last.args[0] != "french" {
		t.Fatalf("expected lang='french', got %v", last.args[0])
	}
}

func TestSearch_InvalidTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := Search(db, "bad table!", "title", "test")
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}
}

func TestSearch_InvalidColumn(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := Search(db, "articles", "bad col!", "test")
	if err == nil {
		t.Fatal("expected error for invalid column name")
	}
}

func TestSearch_InvalidColumnType(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := Search(db, "articles", 42, "test")
	if err == nil {
		t.Fatal("expected error for non-string column type")
	}
}

// --- SearchFuzzy ---

func TestSearchFuzzy_SQLGeneration(t *testing.T) {
	db, drv := newTestDB(t, []string{"id", "name", "_score"}, nil)

	_, err := SearchFuzzy(db, "users", "name", "jonh")
	if err != nil {
		t.Fatal(err)
	}

	caps := drv.allCaptures()

	// The query (no extension creation — proxy handles pg_trgm)
	last := caps[len(caps)-1]
	assertContains(t, last.query, "similarity(name, $1)")
	assertContains(t, last.query, "FROM users")
	assertContains(t, last.query, "similarity(name, $1) > $2")
	assertContains(t, last.query, "ORDER BY _score DESC")
	assertContains(t, last.query, "LIMIT $3")

	if len(last.args) != 3 {
		t.Fatalf("expected 3 params, got %d: %v", len(last.args), last.args)
	}
	if last.args[0] != "jonh" {
		t.Fatalf("expected query='jonh', got %v", last.args[0])
	}
	// threshold default 0.3
	if last.args[1] != 0.3 {
		t.Fatalf("expected threshold=0.3, got %v", last.args[1])
	}
	if last.args[2] != int64(50) {
		t.Fatalf("expected limit=50, got %v", last.args[2])
	}
}

func TestSearchFuzzy_WithThreshold(t *testing.T) {
	db, drv := newTestDB(t, []string{"id", "_score"}, nil)

	_, err := SearchFuzzy(db, "users", "name", "test", WithThreshold(0.5))
	if err != nil {
		t.Fatal(err)
	}

	caps := drv.allCaptures()
	last := caps[len(caps)-1]
	if last.args[1] != 0.5 {
		t.Fatalf("expected threshold=0.5, got %v", last.args[1])
	}
}

func TestSearchFuzzy_InvalidTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := SearchFuzzy(db, "bad table", "name", "test")
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}
}

func TestSearchFuzzy_InvalidColumn(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := SearchFuzzy(db, "users", "bad col", "test")
	if err == nil {
		t.Fatal("expected error for invalid column name")
	}
}

// --- SearchPhonetic ---

func TestSearchPhonetic_SQLGeneration(t *testing.T) {
	db, drv := newTestDB(t, []string{"id", "name", "_score"}, nil)

	_, err := SearchPhonetic(db, "users", "name", "john")
	if err != nil {
		t.Fatal(err)
	}

	caps := drv.allCaptures()

	// The query (no extension creation — proxy handles fuzzystrmatch and pg_trgm)
	last := caps[len(caps)-1]
	assertContains(t, last.query, "soundex(name) = soundex($1)")
	assertContains(t, last.query, "similarity(name, $1)")
	assertContains(t, last.query, "FROM users")
	assertContains(t, last.query, "ORDER BY _score DESC, name")
	assertContains(t, last.query, "LIMIT $2")

	if len(last.args) != 2 {
		t.Fatalf("expected 2 params, got %d: %v", len(last.args), last.args)
	}
	if last.args[0] != "john" {
		t.Fatalf("expected query='john', got %v", last.args[0])
	}
	if last.args[1] != int64(50) {
		t.Fatalf("expected limit=50, got %v", last.args[1])
	}
}

func TestSearchPhonetic_WithLimit(t *testing.T) {
	db, drv := newTestDB(t, []string{"id", "_score"}, nil)

	_, err := SearchPhonetic(db, "users", "name", "jon", WithLimit(5))
	if err != nil {
		t.Fatal(err)
	}

	caps := drv.allCaptures()
	last := caps[len(caps)-1]
	if last.args[1] != int64(5) {
		t.Fatalf("expected limit=5, got %v", last.args[1])
	}
}

func TestSearchPhonetic_InvalidTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := SearchPhonetic(db, "bad!", "name", "test")
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}
}

// --- Similar ---

func TestSimilar_SQLGeneration(t *testing.T) {
	db, drv := newTestDB(t, []string{"id", "embedding", "_score"}, nil)

	_, err := Similar(db, "documents", "embedding", []float64{0.1, 0.2, 0.3})
	if err != nil {
		t.Fatal(err)
	}

	caps := drv.allCaptures()

	// The query (no extension creation — proxy handles vector)
	last := caps[len(caps)-1]
	assertContains(t, last.query, "embedding <=> $1::vector")
	assertContains(t, last.query, "FROM documents")
	assertContains(t, last.query, "ORDER BY _score")
	assertContains(t, last.query, "LIMIT $2")

	if len(last.args) != 2 {
		t.Fatalf("expected 2 params, got %d: %v", len(last.args), last.args)
	}
	// Vector literal
	if last.args[0] != "[0.1,0.2,0.3]" {
		t.Fatalf("expected vector literal '[0.1,0.2,0.3]', got %v", last.args[0])
	}
	if last.args[1] != int64(10) {
		t.Fatalf("expected limit=10 (default), got %v", last.args[1])
	}
}

func TestSimilar_WithLimit(t *testing.T) {
	db, drv := newTestDB(t, []string{"id", "_score"}, nil)

	_, err := Similar(db, "docs", "emb", []float64{1.0}, WithLimit(5))
	if err != nil {
		t.Fatal(err)
	}

	caps := drv.allCaptures()
	last := caps[len(caps)-1]
	if last.args[1] != int64(5) {
		t.Fatalf("expected limit=5, got %v", last.args[1])
	}
}

func TestSimilar_InvalidTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := Similar(db, "bad table", "emb", []float64{1.0})
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}
}

func TestSimilar_InvalidColumn(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := Similar(db, "docs", "bad col", []float64{1.0})
	if err == nil {
		t.Fatal("expected error for invalid column name")
	}
}

// --- Suggest ---

func TestSuggest_SQLGeneration(t *testing.T) {
	db, drv := newTestDB(t, []string{"id", "name", "_score"}, nil)

	_, err := Suggest(db, "products", "name", "lap")
	if err != nil {
		t.Fatal(err)
	}

	caps := drv.allCaptures()

	// The query (no extension creation — proxy handles pg_trgm)
	last := caps[len(caps)-1]
	assertContains(t, last.query, "similarity(name, $1)")
	assertContains(t, last.query, "name ILIKE $2")
	assertContains(t, last.query, "FROM products")
	assertContains(t, last.query, "ORDER BY _score DESC, name")
	assertContains(t, last.query, "LIMIT $3")

	if len(last.args) != 3 {
		t.Fatalf("expected 3 params, got %d: %v", len(last.args), last.args)
	}
	if last.args[0] != "lap" {
		t.Fatalf("expected prefix='lap', got %v", last.args[0])
	}
	if last.args[1] != "lap%" {
		t.Fatalf("expected ILIKE pattern='lap%%', got %v", last.args[1])
	}
	if last.args[2] != int64(10) {
		t.Fatalf("expected limit=10 (default), got %v", last.args[2])
	}
}

func TestSuggest_WithLimit(t *testing.T) {
	db, drv := newTestDB(t, []string{"id", "_score"}, nil)

	_, err := Suggest(db, "products", "name", "la", WithLimit(3))
	if err != nil {
		t.Fatal(err)
	}

	caps := drv.allCaptures()
	last := caps[len(caps)-1]
	if last.args[2] != int64(3) {
		t.Fatalf("expected limit=3, got %v", last.args[2])
	}
}

func TestSuggest_InvalidTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := Suggest(db, "bad!", "name", "test")
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}
}

// --- Facets ---

func TestFacets_BasicSQL(t *testing.T) {
	db, drv := newTestDB(t, []string{"value", "count"}, nil)

	_, err := Facets(db, "products", "category")
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "category AS value")
	assertContains(t, last.query, "COUNT(*) AS count")
	assertContains(t, last.query, "FROM products")
	assertContains(t, last.query, "GROUP BY category")
	assertContains(t, last.query, "ORDER BY count DESC, category")
	assertContains(t, last.query, "LIMIT $1")

	if len(last.args) != 1 {
		t.Fatalf("expected 1 param, got %d: %v", len(last.args), last.args)
	}
	if last.args[0] != int64(50) {
		t.Fatalf("expected limit=50, got %v", last.args[0])
	}
}

func TestFacets_WithQueryFilter(t *testing.T) {
	db, drv := newTestDB(t, []string{"value", "count"}, nil)

	_, err := Facets(db, "products", "category",
		WithQuery("laptop"), WithQueryColumn("title"))
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "to_tsvector($1, coalesce(title, ''))")
	assertContains(t, last.query, "plainto_tsquery($2, $3)")
	assertContains(t, last.query, "category AS value")
	assertContains(t, last.query, "COUNT(*)")
	assertContains(t, last.query, "GROUP BY category")

	if len(last.args) != 4 {
		t.Fatalf("expected 4 params, got %d: %v", len(last.args), last.args)
	}
	if last.args[0] != "english" || last.args[1] != "english" {
		t.Fatalf("expected lang='english', got %v, %v", last.args[0], last.args[1])
	}
	if last.args[2] != "laptop" {
		t.Fatalf("expected query='laptop', got %v", last.args[2])
	}
}

func TestFacets_WithQueryFilterMultiColumn(t *testing.T) {
	db, drv := newTestDB(t, []string{"value", "count"}, nil)

	_, err := Facets(db, "products", "category",
		WithQuery("laptop"), WithQueryColumn([]string{"title", "description"}))
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "coalesce(title, '')")
	assertContains(t, last.query, "coalesce(description, '')")
	assertContains(t, last.query, "|| ' ' ||")
}

func TestFacets_InvalidTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := Facets(db, "bad!", "col")
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}
}

func TestFacets_InvalidColumn(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := Facets(db, "products", "bad col")
	if err == nil {
		t.Fatal("expected error for invalid column name")
	}
}

func TestFacets_InvalidQueryColumnType(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := Facets(db, "products", "category",
		WithQuery("test"), WithQueryColumn(42))
	if err == nil {
		t.Fatal("expected error for invalid queryColumn type")
	}
}

// --- Aggregate ---

func TestAggregate_Count(t *testing.T) {
	db, drv := newTestDB(t, []string{"value"}, nil)

	_, err := Aggregate(db, "orders", "id", "count")
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "COUNT(*) AS value")
	assertContains(t, last.query, "FROM orders")
	// No GROUP BY, no LIMIT params when no groupBy
	if len(last.args) != 0 {
		t.Fatalf("expected 0 params for ungrouped aggregate, got %d: %v", len(last.args), last.args)
	}
}

func TestAggregate_Sum(t *testing.T) {
	db, drv := newTestDB(t, []string{"value"}, nil)

	_, err := Aggregate(db, "orders", "total", "sum")
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "SUM(total) AS value")
	assertContains(t, last.query, "FROM orders")
}

func TestAggregate_Avg(t *testing.T) {
	db, drv := newTestDB(t, []string{"value"}, nil)

	_, err := Aggregate(db, "orders", "amount", "avg")
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "AVG(amount) AS value")
}

func TestAggregate_Min(t *testing.T) {
	db, drv := newTestDB(t, []string{"value"}, nil)

	_, err := Aggregate(db, "orders", "price", "min")
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "MIN(price) AS value")
}

func TestAggregate_Max(t *testing.T) {
	db, drv := newTestDB(t, []string{"value"}, nil)

	_, err := Aggregate(db, "orders", "price", "max")
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "MAX(price) AS value")
}

func TestAggregate_WithGroupBy(t *testing.T) {
	db, drv := newTestDB(t, []string{"category", "value"}, nil)

	_, err := Aggregate(db, "orders", "total", "sum", WithGroupBy("category"))
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "category")
	assertContains(t, last.query, "SUM(total) AS value")
	assertContains(t, last.query, "GROUP BY category")
	assertContains(t, last.query, "ORDER BY value DESC")
	assertContains(t, last.query, "LIMIT $1")

	if len(last.args) != 1 {
		t.Fatalf("expected 1 param, got %d: %v", len(last.args), last.args)
	}
	if last.args[0] != int64(50) {
		t.Fatalf("expected limit=50, got %v", last.args[0])
	}
}

func TestAggregate_InvalidFunc(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := Aggregate(db, "orders", "total", "median")
	if err == nil {
		t.Fatal("expected error for invalid aggregate function")
	}
	assertContains(t, err.Error(), "invalid aggregate function")
}

func TestAggregate_InvalidTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := Aggregate(db, "bad!", "total", "sum")
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}
}

func TestAggregate_InvalidColumn(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := Aggregate(db, "orders", "bad col", "sum")
	if err == nil {
		t.Fatal("expected error for invalid column name")
	}
}

func TestAggregate_InvalidGroupBy(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := Aggregate(db, "orders", "total", "sum", WithGroupBy("bad col"))
	if err == nil {
		t.Fatal("expected error for invalid groupBy column")
	}
}

func TestAggregate_CaseInsensitiveFunc(t *testing.T) {
	db, drv := newTestDB(t, []string{"value"}, nil)

	_, err := Aggregate(db, "orders", "total", "SUM")
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "SUM(total)")
}

// --- CreateSearchConfig ---

func TestCreateSearchConfig_SQLGeneration(t *testing.T) {
	db, drv := newTestDB(t, []string{"?column?"}, nil)

	// The QueryRow for checking existence will return no rows (default mock behavior),
	// triggering the CREATE statement
	err := CreateSearchConfig(db, "my_config", "english")
	if err != nil {
		t.Fatal(err)
	}

	caps := drv.allCaptures()
	// First: SELECT 1 FROM pg_ts_config WHERE cfgname = $1
	assertContains(t, caps[0].query, "pg_ts_config")
	if caps[0].args[0] != "my_config" {
		t.Fatalf("expected cfgname='my_config', got %v", caps[0].args[0])
	}

	// Second: CREATE TEXT SEARCH CONFIGURATION
	if len(caps) >= 2 {
		assertContains(t, caps[1].query, "CREATE TEXT SEARCH CONFIGURATION my_config (COPY = english)")
	}
}

func TestCreateSearchConfig_DefaultCopyFrom(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	err := CreateSearchConfig(db, "custom_cfg", "")
	if err != nil {
		t.Fatal(err)
	}

	caps := drv.allCaptures()
	if len(caps) >= 2 {
		assertContains(t, caps[1].query, "COPY = english")
	}
}

func TestCreateSearchConfig_InvalidName(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	err := CreateSearchConfig(db, "bad name!", "english")
	if err == nil {
		t.Fatal("expected error for invalid config name")
	}
}

func TestCreateSearchConfig_InvalidCopyFrom(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	err := CreateSearchConfig(db, "mycfg", "bad from!")
	if err == nil {
		t.Fatal("expected error for invalid copyFrom name")
	}
}

// --- PercolateAdd ---

func TestPercolateAdd_SQLGeneration(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	err := PercolateAdd(db, "my_percolator", "q1", "breaking news")
	if err != nil {
		t.Fatal(err)
	}

	caps := drv.allCaptures()
	// Should have 3 Exec calls: CREATE TABLE, CREATE INDEX, INSERT/UPSERT
	if len(caps) < 3 {
		t.Fatalf("expected at least 3 captures, got %d", len(caps))
	}

	assertContains(t, caps[0].query, "CREATE TABLE IF NOT EXISTS my_percolator")
	assertContains(t, caps[0].query, "query_id TEXT PRIMARY KEY")
	assertContains(t, caps[0].query, "tsquery TSQUERY")

	assertContains(t, caps[1].query, "CREATE INDEX IF NOT EXISTS my_percolator_tsq_idx")
	assertContains(t, caps[1].query, "USING GIN (tsquery)")

	upsert := caps[2]
	assertContains(t, upsert.query, "INSERT INTO my_percolator")
	assertContains(t, upsert.query, "plainto_tsquery($3, $2)")
	assertContains(t, upsert.query, "ON CONFLICT (query_id) DO UPDATE")

	if upsert.args[0] != "q1" {
		t.Fatalf("expected query_id='q1', got %v", upsert.args[0])
	}
	if upsert.args[1] != "breaking news" {
		t.Fatalf("expected query='breaking news', got %v", upsert.args[1])
	}
	if upsert.args[2] != "english" {
		t.Fatalf("expected lang='english', got %v", upsert.args[2])
	}
}

func TestPercolateAdd_WithMetadata(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	err := PercolateAdd(db, "percolator", "q1", "test",
		WithMetadata(`{"priority":"high"}`))
	if err != nil {
		t.Fatal(err)
	}

	caps := drv.allCaptures()
	upsert := caps[len(caps)-1]
	if upsert.args[3] != `{"priority":"high"}` {
		t.Fatalf("expected metadata JSON, got %v", upsert.args[3])
	}
}

func TestPercolateAdd_WithLang(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	err := PercolateAdd(db, "percolator", "q1", "bonjour", WithLang("french"))
	if err != nil {
		t.Fatal(err)
	}

	caps := drv.allCaptures()
	upsert := caps[len(caps)-1]
	if upsert.args[2] != "french" {
		t.Fatalf("expected lang='french', got %v", upsert.args[2])
	}
}

func TestPercolateAdd_InvalidName(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	err := PercolateAdd(db, "bad name!", "q1", "test")
	if err == nil {
		t.Fatal("expected error for invalid percolator name")
	}
}

// --- Percolate ---

func TestPercolate_SQLGeneration(t *testing.T) {
	db, drv := newTestDB(t, []string{"query_id", "query_text", "metadata", "_score"}, nil)

	_, err := Percolate(db, "my_percolator", "A breaking news story about technology")
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "to_tsvector($1, $2)")
	assertContains(t, last.query, "ts_rank(to_tsvector($1, $2), tsquery)")
	assertContains(t, last.query, "FROM my_percolator")
	assertContains(t, last.query, "to_tsvector($1, $2) @@ tsquery")
	assertContains(t, last.query, "ORDER BY _score DESC")
	assertContains(t, last.query, "LIMIT $3")

	if len(last.args) != 3 {
		t.Fatalf("expected 3 params, got %d: %v", len(last.args), last.args)
	}
	if last.args[0] != "english" {
		t.Fatalf("expected lang='english', got %v", last.args[0])
	}
	if last.args[1] != "A breaking news story about technology" {
		t.Fatalf("expected text, got %v", last.args[1])
	}
	if last.args[2] != int64(50) {
		t.Fatalf("expected limit=50, got %v", last.args[2])
	}
}

func TestPercolate_WithLang(t *testing.T) {
	db, drv := newTestDB(t, []string{"query_id", "query_text", "metadata", "_score"}, nil)

	_, err := Percolate(db, "percolator", "bonjour le monde", WithLang("french"))
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	if last.args[0] != "french" {
		t.Fatalf("expected lang='french', got %v", last.args[0])
	}
}

func TestPercolate_WithLimit(t *testing.T) {
	db, drv := newTestDB(t, []string{"query_id", "query_text", "metadata", "_score"}, nil)

	_, err := Percolate(db, "percolator", "test", WithLimit(5))
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	if last.args[2] != int64(5) {
		t.Fatalf("expected limit=5, got %v", last.args[2])
	}
}

func TestPercolate_InvalidName(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := Percolate(db, "bad name!", "test")
	if err == nil {
		t.Fatal("expected error for invalid percolator name")
	}
}

// --- PercolateDelete ---

func TestPercolateDelete_SQLGeneration(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	_, err := PercolateDelete(db, "my_percolator", "q1")
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "DELETE FROM my_percolator WHERE query_id = $1")

	if len(last.args) != 1 {
		t.Fatalf("expected 1 param, got %d: %v", len(last.args), last.args)
	}
	if last.args[0] != "q1" {
		t.Fatalf("expected query_id='q1', got %v", last.args[0])
	}
}

func TestPercolateDelete_InvalidName(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := PercolateDelete(db, "bad name!", "q1")
	if err == nil {
		t.Fatal("expected error for invalid percolator name")
	}
}

// --- Analyze ---

func TestAnalyze_SQLGeneration(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"alias", "description", "token", "dictionaries", "dictionary", "lexemes"},
		nil)

	_, err := Analyze(db, "The quick brown fox")
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "ts_debug($1, $2)")
	assertContains(t, last.query, "alias")
	assertContains(t, last.query, "description")
	assertContains(t, last.query, "token")
	assertContains(t, last.query, "lexemes")

	if len(last.args) != 2 {
		t.Fatalf("expected 2 params, got %d: %v", len(last.args), last.args)
	}
	if last.args[0] != "english" {
		t.Fatalf("expected lang='english', got %v", last.args[0])
	}
	if last.args[1] != "The quick brown fox" {
		t.Fatalf("expected text='The quick brown fox', got %v", last.args[1])
	}
}

func TestAnalyze_WithLang(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"alias", "description", "token", "dictionaries", "dictionary", "lexemes"},
		nil)

	_, err := Analyze(db, "Le renard brun rapide", WithLang("french"))
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	if last.args[0] != "french" {
		t.Fatalf("expected lang='french', got %v", last.args[0])
	}
}

// --- ExplainScore ---

func TestExplainScore_SQLGeneration(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"document_text", "document_tokens", "query_tokens", "matches", "score", "headline"},
		[][]driver.Value{{"hello world", "'hello':1 'world':2", "'hello'", true, 0.5, "**hello** world"}})

	result, err := ExplainScore(db, "articles", "title", "hello", "id", 42)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "title AS document_text")
	assertContains(t, last.query, "to_tsvector($1, title)")
	assertContains(t, last.query, "plainto_tsquery($1, $2)")
	assertContains(t, last.query, "ts_rank")
	assertContains(t, last.query, "ts_headline")
	assertContains(t, last.query, "FROM articles")
	assertContains(t, last.query, "WHERE id = $3")

	if len(last.args) != 3 {
		t.Fatalf("expected 3 params, got %d: %v", len(last.args), last.args)
	}
	if last.args[0] != "english" {
		t.Fatalf("expected lang='english', got %v", last.args[0])
	}
	if last.args[1] != "hello" {
		t.Fatalf("expected query='hello', got %v", last.args[1])
	}
	if last.args[2] != int64(42) {
		t.Fatalf("expected id=42, got %v", last.args[2])
	}

	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestExplainScore_NotFound(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"document_text", "document_tokens", "query_tokens", "matches", "score", "headline"},
		nil) // no rows

	result, err := ExplainScore(db, "articles", "title", "hello", "id", 999)
	if err != nil {
		t.Fatal(err)
	}

	if result != nil {
		t.Fatalf("expected nil result for not found, got %v", result)
	}
}

func TestExplainScore_WithLang(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"document_text", "document_tokens", "query_tokens", "matches", "score", "headline"},
		[][]driver.Value{{"bonjour", "'bonjour':1", "'bonjour'", true, 0.5, "**bonjour**"}})

	_, err := ExplainScore(db, "articles", "title", "bonjour", "id", 1, WithLang("french"))
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	if last.args[0] != "french" {
		t.Fatalf("expected lang='french', got %v", last.args[0])
	}
}

func TestExplainScore_InvalidTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := ExplainScore(db, "bad!", "title", "test", "id", 1)
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}
}

func TestExplainScore_InvalidColumn(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := ExplainScore(db, "articles", "bad col", "test", "id", 1)
	if err == nil {
		t.Fatal("expected error for invalid column name")
	}
}

func TestExplainScore_InvalidIdColumn(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := ExplainScore(db, "articles", "title", "test", "bad!", 1)
	if err == nil {
		t.Fatal("expected error for invalid id column name")
	}
}

// --- Validate identifier ---

func TestValidateIdentifier_Valid(t *testing.T) {
	for _, name := range []string{"users", "my_table", "_private", "Column1", "a"} {
		if err := validateIdentifier(name); err != nil {
			t.Fatalf("expected %q to be valid, got error: %v", name, err)
		}
	}
}

func TestValidateIdentifier_Invalid(t *testing.T) {
	for _, name := range []string{"", "has space", "has-dash", "1starts_num", "semi;colon", "tab\tnewline"} {
		if err := validateIdentifier(name); err == nil {
			t.Fatalf("expected %q to be invalid", name)
		}
	}
}
