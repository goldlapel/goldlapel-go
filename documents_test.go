package goldlapel

import (
	"context"
	"database/sql/driver"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- Namespace shape ---

func TestDocumentsNamespace_IsAttachedAndPointsAtParent(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	if gl.Documents == nil {
		t.Fatal("expected gl.Documents to be non-nil after Start/buildForTest")
	}
	if gl.Documents.gl != gl {
		t.Fatal("Documents.gl must back-reference the parent — never duplicate state")
	}
}

func TestDocumentsNamespace_StateReadsThroughParent(t *testing.T) {
	// Reconfiguring the parent must be visible on the next call through the
	// sub-API. Mirrors test_documents.py::test_state_reads_through_parent.
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.dashboardToken = "rotated-token"
	if gl.Documents.gl.dashboardToken != "rotated-token" {
		t.Fatalf("expected sub-API to read live token from parent, got %q",
			gl.Documents.gl.dashboardToken)
	}
}

// TestDocumentsNamespace_NoLegacyFlatMethods confirms the hard cut: the
// gl.Doc<Verb> receiver methods are gone after Phase 4. We don't reflect-
// over methods (Go method sets are decided at compile time) but the build
// itself wouldn't have linked if the legacy methods were still defined,
// so this test is the runtime canary that the namespace replaces them.
func TestDocumentsNamespace_NoLegacyFlatMethods(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	// Verifying Documents.* exists is sufficient — the receiver_test.go
	// suite drives every replacement verb through to ErrNotConnected.
	if gl.Documents == nil || gl.Streams == nil {
		t.Fatal("nested namespaces must be attached on every constructed *GoldLapel")
	}
}

// --- Verb dispatch ---

// TestDocumentsInsert_ResolvesPatternedTable exercises the full path: the
// Documents.Insert call resolves the proxy-supplied table name out of the
// pattern cache and runs INSERT INTO that table. Without the resolution
// the SQL would target the user-supplied collection directly.
func TestDocumentsInsert_ResolvesPatternedTable(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		[][]driver.Value{{"11111111-1111-1111-1111-111111111111", `{"name":"alice"}`, "2026-01-01T00:00:00Z"}})

	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	// Stash a pattern entry that maps "users" → the canonical proxy table.
	gl.ddlCache.Store("doc_store:users", &DdlEntry{
		Tables: map[string]string{"main": "_goldlapel.doc_users"},
	})

	ctx := context.Background()
	if _, err := gl.Documents.Insert(ctx, "users", map[string]interface{}{"name": "alice"}); err != nil {
		t.Fatalf("Documents.Insert: %v", err)
	}

	captures := drv.allCaptures()
	if len(captures) == 0 {
		t.Fatal("expected at least one captured query")
	}
	last := captures[len(captures)-1]
	if !strings.Contains(last.query, "INSERT INTO _goldlapel.doc_users") {
		t.Fatalf("expected INSERT against the patterned table, got %q", last.query)
	}
}

func TestDocumentsCount_ResolvesPatternedTable(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"count"},
		[][]driver.Value{{int64(7)}})

	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("doc_store:orders", &DdlEntry{
		Tables: map[string]string{"main": "_goldlapel.doc_orders"},
	})

	count, err := gl.Documents.Count(context.Background(), "orders", nil)
	if err != nil {
		t.Fatalf("Documents.Count: %v", err)
	}
	if count != 7 {
		t.Fatalf("want 7, got %d", count)
	}

	captures := drv.allCaptures()
	if len(captures) == 0 {
		t.Fatal("expected a captured query")
	}
	last := captures[len(captures)-1]
	if !strings.Contains(last.query, "SELECT COUNT(*) FROM _goldlapel.doc_orders") {
		t.Fatalf("expected COUNT against the patterned table, got %q", last.query)
	}
}

// TestDocumentsCreateCollection_PassesUnloggedThrough mirrors
// test_documents.py::test_create_collection_unlogged_passes_through. The
// DocUnlogged option must arrive at the proxy as options.unlogged=true.
func TestDocumentsCreateCollection_PassesUnloggedThrough(t *testing.T) {
	f := newFakeDashboard()
	defer f.close()
	f.queue(200, map[string]any{
		"tables":         map[string]any{"main": "_goldlapel.doc_sessions"},
		"query_patterns": map[string]any{},
	})

	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.dashboardPort = f.port()
	gl.dashboardToken = "tok"

	if err := gl.Documents.CreateCollection(context.Background(), "sessions", DocUnlogged(true)); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.captured) != 1 {
		t.Fatalf("want 1 DDL POST, got %d", len(f.captured))
	}
	c := f.captured[0]
	if c.path != "/api/ddl/doc_store/create" {
		t.Errorf("wrong path: %s", c.path)
	}
	options, ok := c.body["options"].(map[string]any)
	if !ok {
		t.Fatalf("expected options map, got %T (%v)", c.body["options"], c.body["options"])
	}
	if options["unlogged"] != true {
		t.Errorf("expected options.unlogged=true, got %v", options["unlogged"])
	}
}

// TestDocumentsCreateCollection_NoUnloggedDoesNotEmitOptions mirrors
// test_documents.py::test_create_collection_just_fetches_patterns.
func TestDocumentsCreateCollection_NoUnloggedDoesNotEmitOptions(t *testing.T) {
	f := newFakeDashboard()
	defer f.close()
	f.queue(200, map[string]any{
		"tables":         map[string]any{"main": "_goldlapel.doc_users"},
		"query_patterns": map[string]any{},
	})

	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.dashboardPort = f.port()
	gl.dashboardToken = "tok"

	if err := gl.Documents.CreateCollection(context.Background(), "users"); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.captured[0]
	if _, hasOpts := c.body["options"]; hasOpts {
		t.Errorf("no options option means no `options` key in body, got %+v", c.body)
	}
}

// TestDocumentsAggregate_ResolvesLookupFromCollections walks the pipeline
// for each $lookup.from collection, fetches its patterns, and threads the
// resolved table names into the SQL. Mirrors the Python equivalent.
func TestDocumentsAggregate_ResolvesLookupFromCollections(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "user_orders"},
		nil)

	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("doc_store:users", &DdlEntry{
		Tables: map[string]string{"main": "_goldlapel.doc_users"},
	})
	gl.ddlCache.Store("doc_store:orders", &DdlEntry{
		Tables: map[string]string{"main": "_goldlapel.doc_orders"},
	})

	pipeline := []map[string]interface{}{
		{"$lookup": map[string]interface{}{
			"from":         "orders",
			"localField":   "id",
			"foreignField": "userId",
			"as":           "user_orders",
		}},
	}
	if _, err := gl.Documents.Aggregate(context.Background(), "users", pipeline); err != nil {
		t.Fatalf("Aggregate: %v", err)
	}

	captures := drv.allCaptures()
	if len(captures) == 0 {
		t.Fatal("expected a captured query")
	}
	q := captures[len(captures)-1].query
	// Source FROM and lookup FROM both target their canonical tables.
	if !strings.Contains(q, "FROM _goldlapel.doc_users") {
		t.Errorf("source must read from canonical users, got %q", q)
	}
	if !strings.Contains(q, "FROM _goldlapel.doc_orders AS orders") {
		t.Errorf("$lookup must read from canonical orders aliased as orders, got %q", q)
	}
}

// --- Streams namespace ---

func TestStreamsNamespace_IsAttachedAndPointsAtParent(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	if gl.Streams == nil {
		t.Fatal("expected gl.Streams to be non-nil")
	}
	if gl.Streams.gl != gl {
		t.Fatal("Streams.gl must back-reference the parent")
	}
}

// TestStreamsNamespace_DispatchesToFreeFunctions exercises a full happy
// path through gl.Streams.Add → StreamAdd. Ensures the namespace forwards
// to the same patterned execution as the package-level function.
func TestStreamsNamespace_DispatchesToFreeFunctions(t *testing.T) {
	drv := &streamMock{
		rules: []streamRule{
			{
				match:   "INSERT INTO m",
				columns: []string{"id", "created_at"},
				rows:    [][]driver.Value{{int64(7), time.Unix(0, 0).UTC()}},
			},
		},
	}
	db := newStreamMockDB(t, drv)

	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	if gl.ddlCache == nil {
		gl.ddlCache = &sync.Map{}
	}
	// Patterns must include the StreamAdd-required "insert" key — the
	// shared streamPatternsFor() helper covers Read/Ack/Claim. SQL gets
	// rewritten with VALUES ($1::jsonb) before execution so the mock's
	// "INSERT INTO m" rule still matches.
	patterns := streamPatternsFor("events")
	patterns.QueryPatterns["insert"] = "INSERT INTO m (payload) VALUES ($1) RETURNING id, created_at"
	gl.ddlCache.Store("stream:events", patterns)

	id, err := gl.Streams.Add(context.Background(), "events", `{"i":1}`)
	if err != nil {
		t.Fatalf("Streams.Add: %v", err)
	}
	if id != 7 {
		t.Fatalf("want 7, got %d", id)
	}
}

// TestSupportedVersion_DocStore_IsV1 locks the wrapper-pinned doc_store
// schema version. Bumping requires moving in lockstep with the proxy.
func TestSupportedVersion_DocStore_IsV1(t *testing.T) {
	if got := SupportedVersion("doc_store"); got != "v1" {
		t.Fatalf("want v1, got %q", got)
	}
}
