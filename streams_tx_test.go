package goldlapel

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
)

// --- per-query-aware mock driver dedicated to stream tests ---

type streamCapture struct {
	query string
	args  []driver.Value
	inTx  bool
}

type streamMock struct {
	mu sync.Mutex
	// Query-routing — pick columns/rows based on a substring of the query.
	// First matching rule wins.
	rules []streamRule
	// Error to inject on the Nth Exec/Query (1-indexed). 0 means "no error".
	errAt int
	nExec int

	captures  []streamCapture
	commits   int
	rollbacks int
}

type streamRule struct {
	match   string
	columns []string
	rows    [][]driver.Value
}

func (d *streamMock) rowsFor(q string) (cols []string, rows [][]driver.Value) {
	for _, r := range d.rules {
		if strings.Contains(q, r.match) {
			return r.columns, r.rows
		}
	}
	return []string{"unused"}, nil
}

func (d *streamMock) Open(name string) (driver.Conn, error) {
	return &streamMockConn{drv: d}, nil
}

type streamMockConn struct {
	drv  *streamMock
	inTx bool
}

func (c *streamMockConn) Prepare(q string) (driver.Stmt, error) {
	return &streamMockStmt{conn: c, query: q}, nil
}
func (c *streamMockConn) Close() error { return nil }
func (c *streamMockConn) Begin() (driver.Tx, error) {
	c.drv.mu.Lock()
	c.inTx = true
	c.drv.mu.Unlock()
	return &streamMockTx{conn: c}, nil
}

type streamMockTx struct{ conn *streamMockConn }

func (t *streamMockTx) Commit() error {
	t.conn.drv.mu.Lock()
	t.conn.drv.commits++
	t.conn.inTx = false
	t.conn.drv.mu.Unlock()
	return nil
}
func (t *streamMockTx) Rollback() error {
	t.conn.drv.mu.Lock()
	t.conn.drv.rollbacks++
	t.conn.inTx = false
	t.conn.drv.mu.Unlock()
	return nil
}

type streamMockStmt struct {
	conn  *streamMockConn
	query string
}

func (s *streamMockStmt) Close() error  { return nil }
func (s *streamMockStmt) NumInput() int { return -1 }

func (s *streamMockStmt) shouldErr() error {
	s.conn.drv.mu.Lock()
	s.conn.drv.nExec++
	trigger := s.conn.drv.errAt
	n := s.conn.drv.nExec
	s.conn.drv.mu.Unlock()
	if trigger != 0 && n == trigger {
		return errors.New("injected query error")
	}
	return nil
}

func (s *streamMockStmt) Exec(args []driver.Value) (driver.Result, error) {
	s.conn.drv.mu.Lock()
	s.conn.drv.captures = append(s.conn.drv.captures, streamCapture{
		query: s.query, args: args, inTx: s.conn.inTx,
	})
	s.conn.drv.mu.Unlock()
	if err := s.shouldErr(); err != nil {
		return nil, err
	}
	return streamMockResult{}, nil
}

func (s *streamMockStmt) Query(args []driver.Value) (driver.Rows, error) {
	s.conn.drv.mu.Lock()
	s.conn.drv.captures = append(s.conn.drv.captures, streamCapture{
		query: s.query, args: args, inTx: s.conn.inTx,
	})
	cols, rows := s.conn.drv.rowsFor(s.query)
	s.conn.drv.mu.Unlock()
	if err := s.shouldErr(); err != nil {
		return nil, err
	}
	return &streamMockRows{columns: cols, rows: rows, pos: -1}, nil
}

type streamMockResult struct{}

func (r streamMockResult) LastInsertId() (int64, error) { return 0, nil }
func (r streamMockResult) RowsAffected() (int64, error) { return 1, nil }

type streamMockRows struct {
	columns []string
	rows    [][]driver.Value
	pos     int
}

func (r *streamMockRows) Columns() []string { return r.columns }
func (r *streamMockRows) Close() error      { return nil }
func (r *streamMockRows) Next(dest []driver.Value) error {
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

var streamDriverCounter int
var streamDriverMu sync.Mutex

func newStreamMockDB(t *testing.T, drv *streamMock) *sql.DB {
	t.Helper()
	streamDriverMu.Lock()
	streamDriverCounter++
	name := fmt.Sprintf("streammock_%d", streamDriverCounter)
	streamDriverMu.Unlock()
	sql.Register(name, drv)
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// streamPatternsFor returns a canonical pattern set so tests don't depend on
// the live DDL API.
func streamPatternsFor(stream string) *DdlEntry {
	return &DdlEntry{
		Tables: map[string]string{
			"stream":  "_goldlapel.stream_" + stream,
			"groups":  "_goldlapel.stream_" + stream + "_groups",
			"pending": "_goldlapel.stream_" + stream + "_pending",
		},
		QueryPatterns: map[string]string{
			"group_get_cursor":     "SELECT last_delivered_id FROM g WHERE group_name = $1 FOR UPDATE",
			"read_since":           "SELECT id, payload, created_at FROM m WHERE id > $1 ORDER BY id LIMIT $2",
			"group_advance_cursor": "UPDATE g SET last_delivered_id = $1 WHERE group_name = $2",
			"pending_insert":       "INSERT INTO p (message_id, group_name, consumer) VALUES ($1, $2, $3)",
		},
	}
}

// --- tests ---

// TestStreamRead_WrapsInTransaction proves the cursor SELECT, read_since,
// advance, and pending inserts all fire inside an open tx. Without the fix
// these statements run against the pool under autocommit and lose FOR UPDATE
// correctness — concurrent consumers race and double-claim messages.
func TestStreamRead_WrapsInTransaction(t *testing.T) {
	drv := &streamMock{
		rules: []streamRule{
			{
				match:   "FOR UPDATE",
				columns: []string{"last_delivered_id"},
				rows:    [][]driver.Value{{int64(0)}},
			},
			{
				match:   "ORDER BY id",
				columns: []string{"id", "payload", "created_at"},
				rows: [][]driver.Value{
					{int64(1), []byte(`{"i":1}`), "2026-01-01T00:00:00Z"},
					{int64(2), []byte(`{"i":2}`), "2026-01-01T00:00:01Z"},
				},
			},
		},
	}
	db := newStreamMockDB(t, drv)
	gl := &GoldLapel{db: db, ddlCache: &sync.Map{}}
	gl.ddlCache.Store("stream:orders", streamPatternsFor("orders"))

	msgs, err := StreamRead(context.Background(), gl, "orders", "workers", "c", 10)
	if err != nil {
		t.Fatalf("StreamRead: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("want 2 messages, got %d", len(msgs))
	}

	drv.mu.Lock()
	defer drv.mu.Unlock()
	if drv.commits != 1 {
		t.Fatalf("want 1 commit, got %d (rollbacks=%d)", drv.commits, drv.rollbacks)
	}
	if drv.rollbacks != 0 {
		t.Fatalf("happy path should not rollback; got %d", drv.rollbacks)
	}
	if len(drv.captures) == 0 {
		t.Fatal("no queries captured")
	}
	// Every non-tx-boundary statement must execute inside the tx.
	sawCursor, sawAdvance, sawPending := false, false, false
	for _, c := range drv.captures {
		if !c.inTx {
			t.Errorf("statement ran outside tx: %q", c.query)
		}
		if strings.Contains(c.query, "FOR UPDATE") {
			sawCursor = true
		}
		if strings.HasPrefix(c.query, "UPDATE g") {
			sawAdvance = true
		}
		if strings.HasPrefix(c.query, "INSERT INTO p") {
			sawPending = true
		}
	}
	if !sawCursor || !sawAdvance || !sawPending {
		t.Errorf("missing expected queries: cursor=%v advance=%v pending=%v",
			sawCursor, sawAdvance, sawPending)
	}
}

// TestStreamRead_CommitsWhenGroupMissing covers the early-return path. The
// tx must commit cleanly rather than leaking an open transaction.
func TestStreamRead_CommitsWhenGroupMissing(t *testing.T) {
	drv := &streamMock{
		rules: []streamRule{
			{match: "FOR UPDATE", columns: []string{"last_delivered_id"}, rows: nil},
		},
	}
	db := newStreamMockDB(t, drv)
	gl := &GoldLapel{db: db, ddlCache: &sync.Map{}}
	gl.ddlCache.Store("stream:orders", streamPatternsFor("orders"))

	msgs, err := StreamRead(context.Background(), gl, "orders", "workers", "c", 10)
	if err != nil {
		t.Fatalf("StreamRead: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("want 0 messages, got %d", len(msgs))
	}
	drv.mu.Lock()
	defer drv.mu.Unlock()
	if drv.commits != 1 {
		t.Fatalf("want 1 commit on empty-cursor path, got %d", drv.commits)
	}
	if drv.rollbacks != 0 {
		t.Fatalf("want 0 rollbacks, got %d", drv.rollbacks)
	}
}

// TestStreamRead_RollsBackOnError forces a query failure mid-flight and
// verifies we rollback the tx instead of leaking or committing partial work.
func TestStreamRead_RollsBackOnError(t *testing.T) {
	drv := &streamMock{
		rules: []streamRule{
			{match: "FOR UPDATE", columns: []string{"last_delivered_id"}, rows: [][]driver.Value{{int64(0)}}},
			{match: "ORDER BY id", columns: []string{"id", "payload", "created_at"}, rows: [][]driver.Value{
				{int64(1), []byte(`{"i":1}`), "2026-01-01T00:00:00Z"},
			}},
		},
		errAt: 3, // fail on the 3rd Exec/Query (=> during advance cursor UPDATE)
	}
	db := newStreamMockDB(t, drv)
	gl := &GoldLapel{db: db, ddlCache: &sync.Map{}}
	gl.ddlCache.Store("stream:orders", streamPatternsFor("orders"))

	_, err := StreamRead(context.Background(), gl, "orders", "workers", "c", 10)
	if err == nil {
		t.Fatal("want error from injected query failure, got nil")
	}
	drv.mu.Lock()
	defer drv.mu.Unlock()
	if drv.rollbacks != 1 {
		t.Fatalf("want 1 rollback on error path, got %d", drv.rollbacks)
	}
	if drv.commits != 0 {
		t.Fatalf("want 0 commits on error path, got %d", drv.commits)
	}
}

// TestStreamRead_HonoursScopedTx verifies that when the GoldLapel instance is
// already bound to a transaction (via InTx / WithTx), StreamRead uses that tx
// rather than starting a new one. This preserves the caller's atomicity
// boundary.
func TestStreamRead_HonoursScopedTx(t *testing.T) {
	drv := &streamMock{
		rules: []streamRule{
			{match: "FOR UPDATE", columns: []string{"last_delivered_id"}, rows: nil},
		},
	}
	db := newStreamMockDB(t, drv)
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}

	drv.mu.Lock()
	commitsBefore := drv.commits
	drv.mu.Unlock()

	gl := &GoldLapel{db: db, tx: tx, ddlCache: &sync.Map{}}
	gl.ddlCache.Store("stream:orders", streamPatternsFor("orders"))

	if _, err := StreamRead(ctx, gl, "orders", "workers", "c", 10); err != nil {
		t.Fatalf("StreamRead: %v", err)
	}

	drv.mu.Lock()
	commitsAfter := drv.commits
	rollbacksAfter := drv.rollbacks
	drv.mu.Unlock()

	if commitsAfter != commitsBefore {
		t.Errorf("caller-owned tx: StreamRead must not commit (before=%d after=%d)",
			commitsBefore, commitsAfter)
	}
	if rollbacksAfter != 0 {
		t.Errorf("caller-owned tx: StreamRead must not rollback; got %d", rollbacksAfter)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("caller Commit: %v", err)
	}
}

// TestStreamRead_NoDBReturnsError guards the nil-db path.
func TestStreamRead_NoDBReturnsError(t *testing.T) {
	gl := &GoldLapel{ddlCache: &sync.Map{}}
	gl.ddlCache.Store("stream:orders", streamPatternsFor("orders"))
	_, err := StreamRead(context.Background(), gl, "orders", "workers", "c", 10)
	if err != ErrNotConnected {
		t.Fatalf("want ErrNotConnected, got %v", err)
	}
}
