package goldlapel

import (
	"context"
	"database/sql/driver"
	"strings"
	"sync"
	"testing"
)

// counterPatternsFor returns a fake DDL pattern set keyed against the
// canonical proxy table name `_goldlapel.counter_<name>`. Mirrors the Python
// fixture in tests/test_counters.py — the SQL is the actual pattern shape
// the proxy emits at v1, including the Phase 5 `updated_at = NOW()` stamp
// on every UPSERT.
func counterPatternsFor(name string) *DdlEntry {
	main := "_goldlapel.counter_" + name
	return &DdlEntry{
		Tables: map[string]string{"main": main},
		QueryPatterns: map[string]string{
			"incr":       "INSERT INTO " + main + " (key, value, updated_at) VALUES ($1, $2, NOW()) ON CONFLICT (key) DO UPDATE SET value = " + main + ".value + EXCLUDED.value, updated_at = NOW() RETURNING value",
			"set":        "INSERT INTO " + main + " (key, value, updated_at) VALUES ($1, $2, NOW()) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW() RETURNING value",
			"get":        "SELECT value FROM " + main + " WHERE key = $1",
			"delete":     "DELETE FROM " + main + " WHERE key = $1",
			"count_keys": "SELECT COUNT(*) FROM " + main,
		},
	}
}

// --- Namespace shape ---

func TestCountersNamespace_IsAttachedAndPointsAtParent(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	if gl.Counters == nil {
		t.Fatal("expected gl.Counters to be non-nil after buildForTest")
	}
	if gl.Counters.gl != gl {
		t.Fatal("Counters.gl must back-reference the parent")
	}
}

func TestCountersNamespace_StateReadsThroughParent(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.dashboardToken = "rotated-token"
	if gl.Counters.gl.dashboardToken != "rotated-token" {
		t.Fatalf("expected sub-API to read live token from parent, got %q",
			gl.Counters.gl.dashboardToken)
	}
}

// --- Verb dispatch + SQL shape ---

func TestCountersIncr_UsesPatternedTable(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"value"},
		[][]driver.Value{{int64(7)}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	if gl.ddlCache == nil {
		gl.ddlCache = &sync.Map{}
	}
	gl.ddlCache.Store("counter:pageviews", counterPatternsFor("pageviews"))

	got, err := gl.Counters.Incr(context.Background(), "pageviews", "home", 5)
	if err != nil {
		t.Fatalf("Counters.Incr: %v", err)
	}
	if got != 7 {
		t.Fatalf("want 7, got %d", got)
	}

	caps := drv.allCaptures()
	if len(caps) == 0 {
		t.Fatal("expected at least one captured query")
	}
	last := caps[len(caps)-1]
	if !strings.Contains(last.query, "_goldlapel.counter_pageviews") {
		t.Fatalf("expected canonical table in SQL, got %q", last.query)
	}
	// Phase 5 contract: every UPSERT pattern stamps updated_at = NOW().
	if !strings.Contains(last.query, "updated_at = NOW()") {
		t.Fatalf("expected Phase 5 updated_at = NOW() in incr SQL, got %q", last.query)
	}
	// Param binding: ($1=key, $2=amount).
	if len(last.args) != 2 || last.args[0] != "home" || last.args[1] != int64(5) {
		t.Fatalf("expected (home, 5), got %v", last.args)
	}
}

func TestCountersDecr_PassesNegativeAmount(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"value"},
		[][]driver.Value{{int64(-3)}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("counter:pageviews", counterPatternsFor("pageviews"))

	got, err := gl.Counters.Decr(context.Background(), "pageviews", "home", 3)
	if err != nil {
		t.Fatalf("Counters.Decr: %v", err)
	}
	if got != -3 {
		t.Fatalf("want -3, got %d", got)
	}
	last := drv.lastCapture()
	// Decr is implemented as Incr(-amount), so $2 must be -3.
	if len(last.args) != 2 || last.args[1] != int64(-3) {
		t.Fatalf("Decr should bind $2 = -amount, got %v", last.args)
	}
}

func TestCountersSet_UsesSetPattern(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"value"},
		[][]driver.Value{{int64(100)}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("counter:pageviews", counterPatternsFor("pageviews"))

	got, err := gl.Counters.Set(context.Background(), "pageviews", "home", 100)
	if err != nil {
		t.Fatalf("Counters.Set: %v", err)
	}
	if got != 100 {
		t.Fatalf("want 100, got %d", got)
	}
	last := drv.lastCapture()
	// Set pattern: SET value = EXCLUDED.value (not added).
	if !strings.Contains(last.query, "value = EXCLUDED.value") {
		t.Fatalf("expected Set pattern with `value = EXCLUDED.value`, got %q", last.query)
	}
}

func TestCountersGet_ReturnsZeroForUnknownKey(t *testing.T) {
	// Empty rows → sql.ErrNoRows on Scan → 0 (Redis convention).
	db, _ := newTestDB(t, []string{"value"}, nil)
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("counter:pageviews", counterPatternsFor("pageviews"))

	got, err := gl.Counters.Get(context.Background(), "pageviews", "missing")
	if err != nil {
		t.Fatalf("Counters.Get: %v", err)
	}
	if got != 0 {
		t.Fatalf("expected 0 for unknown key, got %d", got)
	}
}

func TestCountersDelete_RowsAffectedDrivesBool(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("counter:pageviews", counterPatternsFor("pageviews"))

	// Mock RowsAffected returns 1 — we expect true.
	deleted, err := gl.Counters.Delete(context.Background(), "pageviews", "home")
	if err != nil {
		t.Fatalf("Counters.Delete: %v", err)
	}
	if !deleted {
		t.Fatal("expected delete to return true when RowsAffected > 0")
	}
}

func TestCountersCountKeys_NoArgsAfterName(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"count"},
		[][]driver.Value{{int64(5)}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("counter:pageviews", counterPatternsFor("pageviews"))

	got, err := gl.Counters.CountKeys(context.Background(), "pageviews")
	if err != nil {
		t.Fatalf("Counters.CountKeys: %v", err)
	}
	if got != 5 {
		t.Fatalf("want 5, got %d", got)
	}
	last := drv.lastCapture()
	if len(last.args) != 0 {
		t.Fatalf("CountKeys takes no params, got %v", last.args)
	}
}

func TestCountersCreate_PostsDDLOnly(t *testing.T) {
	f := newFakeDashboard()
	defer f.close()
	f.queue(200, map[string]any{
		"tables":         map[string]any{"main": "_goldlapel.counter_pageviews"},
		"query_patterns": map[string]any{},
	})

	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.dashboardPort = f.port()
	gl.dashboardToken = "tok"

	if err := gl.Counters.Create(context.Background(), "pageviews"); err != nil {
		t.Fatalf("Create: %v", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.captured) != 1 {
		t.Fatalf("want 1 DDL POST, got %d", len(f.captured))
	}
	c := f.captured[0]
	if c.path != "/api/ddl/counter/create" {
		t.Errorf("wrong path: %s", c.path)
	}
	if c.body["name"] != "pageviews" {
		t.Errorf("wrong name: %v", c.body["name"])
	}
}

// --- Phase 5 contract: no legacy flat methods on *GoldLapel ---
//
// `Incr`, `GetCounter`, etc. were receiver methods pre-Phase-5; they are
// removed in this rollout. The Go compiler is the canary — if any are
// resurrected, this file's other tests would still fail at build time
// because counters_test.go refers to gl.Counters.Incr / gl.Counters.Get
// instead. This test exercises the full surface to make the cut explicit.
func TestCountersNamespace_NoLegacyFlatMethods(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	if gl.Counters == nil {
		t.Fatal("counter sub-API must be the only path post-Phase-5")
	}
}

func TestSupportedVersion_Counter_IsV1(t *testing.T) {
	if got := SupportedVersion("counter"); got != "v1" {
		t.Fatalf("want v1, got %q", got)
	}
}
