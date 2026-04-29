package goldlapel

import (
	"context"
	"database/sql/driver"
	"strings"
	"sync"
	"testing"
)

// hashPatternsFor returns a fake DDL pattern set keyed against the canonical
// proxy table `_goldlapel.hash_<name>`. Phase 5 contract: row-per-field
// (`hash_key`, `field`, `value`) — NOT a JSONB-blob-per-key. Mirrors the
// Python fixture in tests/test_hashes.py.
func hashPatternsFor(name string) *DdlEntry {
	main := "_goldlapel.hash_" + name
	return &DdlEntry{
		Tables: map[string]string{"main": main},
		QueryPatterns: map[string]string{
			"hset":    "INSERT INTO " + main + " (hash_key, field, value, updated_at) VALUES ($1, $2, $3::jsonb, NOW()) ON CONFLICT (hash_key, field) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW() RETURNING value",
			"hget":    "SELECT value FROM " + main + " WHERE hash_key = $1 AND field = $2",
			"hgetall": "SELECT field, value FROM " + main + " WHERE hash_key = $1",
			"hkeys":   "SELECT field FROM " + main + " WHERE hash_key = $1 ORDER BY field",
			"hvals":   "SELECT value FROM " + main + " WHERE hash_key = $1 ORDER BY field",
			"hexists": "SELECT EXISTS (SELECT 1 FROM " + main + " WHERE hash_key = $1 AND field = $2)",
			"hdel":    "DELETE FROM " + main + " WHERE hash_key = $1 AND field = $2",
			"hlen":    "SELECT COUNT(*) FROM " + main + " WHERE hash_key = $1",
		},
	}
}

func TestHashesNamespace_IsAttached(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	if gl.Hashes == nil {
		t.Fatal("expected gl.Hashes to be non-nil")
	}
	if gl.Hashes.gl != gl {
		t.Fatal("Hashes.gl must back-reference the parent")
	}
}

// TestHashesSet_RowPerFieldStorage locks the Phase 5 contract: writes are
// row-per-field, NOT a single JSONB blob per hash_key.
func TestHashesSet_RowPerFieldStorage(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"value"},
		[][]driver.Value{{[]byte(`{"name":"alice"}`)}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	if gl.ddlCache == nil {
		gl.ddlCache = &sync.Map{}
	}
	gl.ddlCache.Store("hash:profiles", hashPatternsFor("profiles"))

	got, err := gl.Hashes.Set(context.Background(), "profiles", "user:1", "name",
		map[string]interface{}{"name": "alice"})
	if err != nil {
		t.Fatalf("Hashes.Set: %v", err)
	}
	if !strings.Contains(string(got), "alice") {
		t.Fatalf("expected returned JSONB to contain alice, got %s", got)
	}
	last := drv.lastCapture()
	// Three params: hash_key, field, encoded value.
	if len(last.args) != 3 {
		t.Fatalf("expected 3 params (hash_key, field, value), got %v", last.args)
	}
	if last.args[0] != "user:1" || last.args[1] != "name" {
		t.Fatalf("expected (user:1, name, ...), got %v", last.args)
	}
	// Phase 5: the SQL must NOT use jsonb_build_object on a single `data` column.
	if strings.Contains(last.query, "jsonb_build_object") {
		t.Fatalf("Phase 5 contract violation: hash storage must be row-per-field, "+
			"not JSONB-blob-per-key. SQL was: %q", last.query)
	}
}

func TestHashesGet_ReturnsNilForAbsent(t *testing.T) {
	db, _ := newTestDB(t, []string{"value"}, nil)
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("hash:profiles", hashPatternsFor("profiles"))

	got, err := gl.Hashes.Get(context.Background(), "profiles", "user:1", "ghost")
	if err != nil {
		t.Fatalf("Hashes.Get: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for absent (key, field), got %s", got)
	}
}

// TestHashesGetAll_RebuildsMapClientSide locks the Phase 5 storage flip:
// GetAll streams (field, value) rows back and reassembles them into a
// map[string]json.RawMessage on the client side, instead of reading a
// single JSONB blob.
func TestHashesGetAll_RebuildsMapClientSide(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"field", "value"},
		[][]driver.Value{
			{"name", []byte(`"alice"`)},
			{"age", []byte(`30`)},
		})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("hash:profiles", hashPatternsFor("profiles"))

	out, err := gl.Hashes.GetAll(context.Background(), "profiles", "user:1")
	if err != nil {
		t.Fatalf("Hashes.GetAll: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 fields, got %d (%v)", len(out), out)
	}
	if string(out["name"]) != `"alice"` {
		t.Errorf("expected name='\"alice\"', got %s", out["name"])
	}
	if string(out["age"]) != `30` {
		t.Errorf("expected age=30, got %s", out["age"])
	}
}

func TestHashesGetAll_EmptyMapForUnknownKey(t *testing.T) {
	// No rows → empty (non-nil) map (matches Python's empty-dict shape).
	db, _ := newTestDB(t, []string{"field", "value"}, nil)
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("hash:profiles", hashPatternsFor("profiles"))

	out, err := gl.Hashes.GetAll(context.Background(), "profiles", "ghost")
	if err != nil {
		t.Fatalf("Hashes.GetAll: %v", err)
	}
	if out == nil {
		t.Fatal("GetAll on absent key should return empty map, not nil")
	}
	if len(out) != 0 {
		t.Fatalf("expected empty map, got %v", out)
	}
}

func TestHashesKeys(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"field"},
		[][]driver.Value{{"a"}, {"b"}, {"c"}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("hash:profiles", hashPatternsFor("profiles"))

	out, err := gl.Hashes.Keys(context.Background(), "profiles", "user:1")
	if err != nil {
		t.Fatalf("Hashes.Keys: %v", err)
	}
	if len(out) != 3 || out[0] != "a" || out[2] != "c" {
		t.Fatalf("expected [a b c], got %v", out)
	}
}

func TestHashesValues(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"value"},
		[][]driver.Value{
			{[]byte(`"alice"`)},
			{[]byte(`30`)},
		})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("hash:profiles", hashPatternsFor("profiles"))

	out, err := gl.Hashes.Values(context.Background(), "profiles", "user:1")
	if err != nil {
		t.Fatalf("Hashes.Values: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 values, got %d", len(out))
	}
}

func TestHashesExists_True(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"exists"},
		[][]driver.Value{{true}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("hash:profiles", hashPatternsFor("profiles"))

	got, err := gl.Hashes.Exists(context.Background(), "profiles", "user:1", "name")
	if err != nil {
		t.Fatalf("Hashes.Exists: %v", err)
	}
	if !got {
		t.Fatal("expected exists=true")
	}
}

func TestHashesDelete_RowsAffected(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("hash:profiles", hashPatternsFor("profiles"))

	removed, err := gl.Hashes.Delete(context.Background(), "profiles", "user:1", "name")
	if err != nil {
		t.Fatalf("Hashes.Delete: %v", err)
	}
	if !removed {
		t.Fatal("expected removed=true (mock RowsAffected returns 1)")
	}
}

func TestHashesLen(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"count"},
		[][]driver.Value{{int64(7)}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("hash:profiles", hashPatternsFor("profiles"))

	got, err := gl.Hashes.Len(context.Background(), "profiles", "user:1")
	if err != nil {
		t.Fatalf("Hashes.Len: %v", err)
	}
	if got != 7 {
		t.Fatalf("want 7, got %d", got)
	}
}

func TestSupportedVersion_Hash_IsV1(t *testing.T) {
	if got := SupportedVersion("hash"); got != "v1" {
		t.Fatalf("want v1, got %q", got)
	}
}
