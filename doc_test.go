package goldlapel

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// ctx is the default context used by all package-level helper tests below.
var ctx = context.Background()

// --- DocInsert ---

func TestDocInsert_SQLGeneration(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		[][]driver.Value{{"11111111-1111-1111-1111-111111111111", `{"name":"alice"}`, "2026-01-01T00:00:00Z"}})

	result, err := DocInsert(ctx, db, "users", map[string]interface{}{"name": "alice"})
	if err != nil {
		t.Fatal(err)
	}

	captures := drv.allCaptures()
	// First capture: CREATE TABLE IF NOT EXISTS
	assertContains(t, captures[0].query, "CREATE TABLE IF NOT EXISTS users")
	assertContains(t, captures[0].query, "data JSONB NOT NULL")
	assertContains(t, captures[0].query, "_id UUID PRIMARY KEY DEFAULT gen_random_uuid()")

	// Second capture: INSERT RETURNING
	assertContains(t, captures[1].query, "INSERT INTO users")
	assertContains(t, captures[1].query, "$1::jsonb")
	assertContains(t, captures[1].query, "RETURNING _id, data, created_at")

	data, ok := result["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected data to be a map, got %T: %v", result["data"], result["data"])
	}
	if data["name"] != "alice" {
		t.Fatalf("expected data.name=alice, got %v", data["name"])
	}
	if result["_id"] != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("expected _id=UUID string, got %v", result["_id"])
	}
	if result["created_at"] != "2026-01-01T00:00:00Z" {
		t.Fatalf("expected created_at, got %v", result["created_at"])
	}
}

func TestDocInsert_InvalidCollection(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	_, err := DocInsert(ctx, db, "drop table;--", map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for invalid collection name")
	}
	assertContains(t, err.Error(), "invalid identifier")
}

// --- DocInsertMany ---

func TestDocInsertMany_SQLGeneration(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		[][]driver.Value{
			{"11111111-1111-1111-1111-111111111111", `{"name":"alice"}`, "2026-01-01T00:00:00Z"},
			{"22222222-2222-2222-2222-222222222222", `{"name":"bob"}`, "2026-01-01T00:00:00Z"},
		})

	docs := []interface{}{
		map[string]interface{}{"name": "alice"},
		map[string]interface{}{"name": "bob"},
	}
	results, err := DocInsertMany(ctx, db, "users", docs)
	if err != nil {
		t.Fatal(err)
	}

	captures := drv.allCaptures()
	// Second capture is the INSERT (first is CREATE TABLE)
	insertQ := captures[1].query
	assertContains(t, insertQ, "INSERT INTO users (data) VALUES")
	assertContains(t, insertQ, "$1::jsonb")
	assertContains(t, insertQ, "$2::jsonb")
	assertContains(t, insertQ, "RETURNING _id, data, created_at")

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0]["data"].(map[string]interface{})["name"] != "alice" {
		t.Fatalf("expected data.name=alice, got %v", results[0]["data"])
	}
	if results[1]["data"].(map[string]interface{})["name"] != "bob" {
		t.Fatalf("expected data.name=bob, got %v", results[1]["data"])
	}
}

func TestDocInsertMany_Empty(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	results, err := DocInsertMany(ctx, db, "users", nil)
	if err != nil {
		t.Fatal(err)
	}
	if results != nil {
		t.Fatalf("expected nil for empty insert, got %v", results)
	}
}

// --- DocFind ---

func TestDocFind_NoFilter(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		[][]driver.Value{
			{"11111111-1111-1111-1111-111111111111", `{"name":"alice"}`, "2026-01-01T00:00:00Z"},
		})

	results, err := DocFind(ctx, db, "users", nil)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "SELECT _id, data, created_at FROM users")
	assertNotContains(t, last.query, "WHERE")
	// No default ORDER BY — matches the 6-wrapper consensus (Python, JS, Ruby,
	// Java, PHP, .NET). UUIDs don't sort by insert order, so we don't fake one.
	assertNotContains(t, last.query, "ORDER BY")
	assertContains(t, last.query, "LIMIT $1")

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func TestDocFind_WithFilter(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		[][]driver.Value{
			{"11111111-1111-1111-1111-111111111111", `{"role":"admin"}`, "2026-01-01T00:00:00Z"},
		})

	_, err := DocFind(ctx, db, "users", map[string]string{"role": "admin"})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "WHERE data @> $1::jsonb")
	assertContains(t, last.query, "LIMIT $2")
}

func TestDocFind_EmptyFilter(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	_, err := DocFind(ctx, db, "users", map[string]string{})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	// Empty map {} should not produce a WHERE clause
	assertNotContains(t, last.query, "WHERE")
}

func TestDocFind_WithSort(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	_, err := DocFind(ctx, db, "users", nil, DocSort(map[string]int{"name": 1, "age": -1}))
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	// Keys sorted alphabetically: age DESC, name ASC
	assertContains(t, last.query, "ORDER BY data->>'age' DESC, data->>'name' ASC")
}

func TestDocFind_WithLimitAndSkip(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	_, err := DocFind(ctx, db, "users", nil, DocLimit(10), DocSkip(20))
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "LIMIT $1")
	assertContains(t, last.query, "OFFSET $2")

	if last.args[0] != int64(10) {
		t.Fatalf("expected limit=10, got %v", last.args[0])
	}
	if last.args[1] != int64(20) {
		t.Fatalf("expected skip=20, got %v", last.args[1])
	}
}

func TestDocFind_InvalidCollection(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	_, err := DocFind(ctx, db, "123bad", nil)
	if err == nil {
		t.Fatal("expected error for invalid collection name")
	}
}

// --- DocFindOne ---

func TestDocFindOne_WithFilter(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		[][]driver.Value{
			{"11111111-1111-1111-1111-111111111111", `{"name":"alice"}`, "2026-01-01T00:00:00Z"},
		})

	result, err := DocFindOne(ctx, db, "users", map[string]string{"name": "alice"})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "WHERE data @> $1::jsonb")
	assertContains(t, last.query, "LIMIT 1")
	assertNotContains(t, last.query, "ORDER BY")

	if result["data"].(map[string]interface{})["name"] != "alice" {
		t.Fatalf("expected data.name=alice, got %v", result["data"])
	}
}

func TestDocFindOne_NotFound(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	result, err := DocFindOne(ctx, db, "users", map[string]string{"name": "nobody"})
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Fatalf("expected nil for not found, got %v", result)
	}
}

// --- DocUpdate ---

func TestDocUpdate_SQLGeneration(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	_, err := DocUpdate(ctx, db, "users",
		map[string]string{"role": "user"},
		map[string]string{"role": "admin"})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "UPDATE users SET data = data || $1::jsonb WHERE data @> $2::jsonb")
}

func TestDocUpdate_InvalidCollection(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	_, err := DocUpdate(ctx, db, "bad name!", nil, nil)
	if err == nil {
		t.Fatal("expected error for invalid collection name")
	}
}

// --- DocUpdateOne ---

func TestDocUpdateOne_CTEWithLimit1(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	_, err := DocUpdateOne(ctx, db, "users",
		map[string]string{"role": "user"},
		map[string]string{"role": "admin"})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "WITH target AS")
	assertContains(t, last.query, "LIMIT 1")
	assertContains(t, last.query, "data || $2::jsonb")
	assertContains(t, last.query, "data @> $1::jsonb")
}

// --- DocDelete ---

func TestDocDelete_SQLGeneration(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	_, err := DocDelete(ctx, db, "users", map[string]string{"role": "banned"})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "DELETE FROM users WHERE data @> $1::jsonb")
}

func TestDocDelete_InvalidCollection(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	_, err := DocDelete(ctx, db, "1invalid", nil)
	if err == nil {
		t.Fatal("expected error for invalid collection name")
	}
}

// --- DocDeleteOne ---

func TestDocDeleteOne_CTEWithLimit1(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	_, err := DocDeleteOne(ctx, db, "users", map[string]string{"name": "alice"})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "WITH target AS")
	assertContains(t, last.query, "LIMIT 1")
	assertContains(t, last.query, "DELETE FROM users USING target")
}

// --- DocCount ---

func TestDocCount_NoFilter(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"count"},
		[][]driver.Value{{int64(42)}})

	count, err := DocCount(ctx, db, "users", nil)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "SELECT COUNT(*) FROM users")
	assertNotContains(t, last.query, "WHERE")

	if count != 42 {
		t.Fatalf("expected count=42, got %d", count)
	}
}

func TestDocCount_WithFilter(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"count"},
		[][]driver.Value{{int64(5)}})

	count, err := DocCount(ctx, db, "users", map[string]string{"role": "admin"})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "WHERE data @> $1::jsonb")

	if count != 5 {
		t.Fatalf("expected count=5, got %d", count)
	}
}

func TestDocCount_EmptyFilter(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"count"},
		[][]driver.Value{{int64(10)}})

	_, err := DocCount(ctx, db, "users", map[string]string{})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertNotContains(t, last.query, "WHERE")
}

func TestDocCount_InvalidCollection(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	_, err := DocCount(ctx, db, "bad;name", nil)
	if err == nil {
		t.Fatal("expected error for invalid collection name")
	}
}

// --- DocCreateIndex ---

func TestDocCreateIndex_FullIndex(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	err := DocCreateIndex(ctx, db, "users", nil)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "CREATE INDEX IF NOT EXISTS users_data_gin")
	assertContains(t, last.query, "USING GIN")
	assertContains(t, last.query, "jsonb_path_ops")
}

func TestDocCreateIndex_SingleKey(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	err := DocCreateIndex(ctx, db, "users", []string{"email"})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "CREATE INDEX IF NOT EXISTS users_email_idx")
	assertContains(t, last.query, "(data->'email')")
}

func TestDocCreateIndex_MultipleKeys(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	err := DocCreateIndex(ctx, db, "users", []string{"first_name", "last_name"})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "users_first_name_last_name_idx")
	assertContains(t, last.query, "(data->'first_name')")
	assertContains(t, last.query, "(data->'last_name')")
}

func TestDocCreateIndex_InvalidCollection(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	err := DocCreateIndex(ctx, db, "bad;name", nil)
	if err == nil {
		t.Fatal("expected error for invalid collection name")
	}
}

func TestDocCreateIndex_InvalidKey(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	err := DocCreateIndex(ctx, db, "users", []string{"bad;key"})
	if err == nil {
		t.Fatal("expected error for invalid key name")
	}
}

// --- Identifier validation across all functions ---

func TestDoc_AllFunctions_RejectSQLInjection(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	badName := "users; DROP TABLE users;--"

	tests := []struct {
		name string
		fn   func() error
	}{
		{"DocInsert", func() error { _, err := DocInsert(ctx, db, badName, map[string]string{}); return err }},
		{"DocInsertMany", func() error { _, err := DocInsertMany(ctx, db, badName, []interface{}{map[string]string{"a": "b"}}); return err }},
		{"DocFind", func() error { _, err := DocFind(ctx, db, badName, nil); return err }},
		{"DocFindOne", func() error { _, err := DocFindOne(ctx, db, badName, nil); return err }},
		{"DocUpdate", func() error { _, err := DocUpdate(ctx, db, badName, nil, nil); return err }},
		{"DocUpdateOne", func() error { _, err := DocUpdateOne(ctx, db, badName, nil, nil); return err }},
		{"DocDelete", func() error { _, err := DocDelete(ctx, db, badName, nil); return err }},
		{"DocDeleteOne", func() error { _, err := DocDeleteOne(ctx, db, badName, nil); return err }},
		{"DocCount", func() error { _, err := DocCount(ctx, db, badName, nil); return err }},
		{"DocCreateIndex", func() error { return DocCreateIndex(ctx, db, badName, nil) }},
		{"DocAggregate", func() error {
			_, err := DocAggregate(ctx, db, badName, []map[string]interface{}{{"$match": nil}})
			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn()
			if err == nil {
				t.Fatalf("%s: expected error for SQL injection attempt", tt.name)
			}
			if !strings.Contains(err.Error(), "invalid identifier") {
				t.Fatalf("%s: expected 'invalid identifier' error, got: %v", tt.name, err)
			}
		})
	}
}

// --- buildSortClause ---

func TestBuildSortClause_Deterministic(t *testing.T) {
	s := map[string]int{"name": 1, "age": -1, "email": 1}
	parts, err := buildSortClause(s)
	if err != nil {
		t.Fatal(err)
	}

	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}
	// Alphabetical: age, email, name
	if parts[0] != "data->>'age' DESC" {
		t.Fatalf("expected age DESC, got %s", parts[0])
	}
	if parts[1] != "data->>'email' ASC" {
		t.Fatalf("expected email ASC, got %s", parts[1])
	}
	if parts[2] != "data->>'name' ASC" {
		t.Fatalf("expected name ASC, got %s", parts[2])
	}
}

func TestBuildSortClause_InvalidKey(t *testing.T) {
	s := map[string]int{"valid": 1, "bad;key": -1}
	_, err := buildSortClause(s)
	if err == nil {
		t.Fatal("expected error for invalid sort key")
	}
	assertContains(t, err.Error(), "invalid sort key")
}

func TestDocFind_InvalidSortKey(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	_, err := DocFind(ctx, db, "users", nil, DocSort(map[string]int{"bad;key": 1}))
	if err == nil {
		t.Fatal("expected error for invalid sort key")
	}
	assertContains(t, err.Error(), "invalid sort key")
}

// --- DocAggregate ---

func TestDocAggregate_FullPipeline(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "total", "count"},
		[][]driver.Value{
			{"electronics", int64(500), int64(3)},
		})

	pipeline := []map[string]interface{}{
		{"$match": map[string]interface{}{"status": "active"}},
		{"$group": map[string]interface{}{
			"_id":   "$category",
			"total": map[string]interface{}{"$sum": "$price"},
			"count": map[string]interface{}{"$sum": float64(1)},
		}},
		{"$sort": map[string]interface{}{"total": float64(-1)}},
		{"$limit": float64(10)},
		{"$skip": float64(5)},
	}

	results, err := DocAggregate(ctx, db, "orders", pipeline)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "SELECT data->>'category' AS _id")
	assertContains(t, last.query, "COUNT(*) AS count")
	assertContains(t, last.query, "SUM((data->>'price')::numeric) AS total")
	assertContains(t, last.query, "FROM orders")
	assertContains(t, last.query, "WHERE data @> $1::jsonb")
	assertContains(t, last.query, "GROUP BY data->>'category'")
	assertContains(t, last.query, "ORDER BY total DESC")
	assertContains(t, last.query, "LIMIT $2")
	assertContains(t, last.query, "OFFSET $3")

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0]["_id"] != "electronics" {
		t.Fatalf("expected _id=electronics, got %v", results[0]["_id"])
	}
}

func TestDocAggregate_Accumulators(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "avg_price", "max_price", "min_price", "num"},
		[][]driver.Value{
			{"books", int64(25), int64(50), int64(10), int64(5)},
		})

	pipeline := []map[string]interface{}{
		{"$group": map[string]interface{}{
			"_id":       "$category",
			"avg_price": map[string]interface{}{"$avg": "$price"},
			"max_price": map[string]interface{}{"$max": "$price"},
			"min_price": map[string]interface{}{"$min": "$price"},
			"num":       map[string]interface{}{"$count": true},
		}},
	}

	_, err := DocAggregate(ctx, db, "products", pipeline)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "AVG((data->>'price')::numeric) AS avg_price")
	assertContains(t, last.query, "MAX((data->>'price')::numeric) AS max_price")
	assertContains(t, last.query, "MIN((data->>'price')::numeric) AS min_price")
	assertContains(t, last.query, "COUNT(*) AS num")
	assertContains(t, last.query, "GROUP BY data->>'category'")
}

func TestDocAggregate_NullID(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "total"},
		[][]driver.Value{
			{nil, int64(1000)},
		})

	pipeline := []map[string]interface{}{
		{"$group": map[string]interface{}{
			"_id":   nil,
			"total": map[string]interface{}{"$sum": "$amount"},
		}},
	}

	results, err := DocAggregate(ctx, db, "orders", pipeline)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "NULL AS _id")
	assertContains(t, last.query, "SUM((data->>'amount')::numeric) AS total")
	assertNotContains(t, last.query, "GROUP BY")

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func TestDocAggregate_MatchOnly(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		[][]driver.Value{
			{"11111111-1111-1111-1111-111111111111", `{"status":"active","name":"alice"}`, "2026-01-01T00:00:00Z"},
		})

	pipeline := []map[string]interface{}{
		{"$match": map[string]interface{}{"status": "active"}},
	}

	results, err := DocAggregate(ctx, db, "users", pipeline)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "SELECT _id, data, created_at FROM users")
	assertContains(t, last.query, "WHERE data @> $1::jsonb")
	assertNotContains(t, last.query, "GROUP BY")

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	data, ok := results[0]["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected data to be a map, got %T: %v", results[0]["data"], results[0]["data"])
	}
	if data["name"] != "alice" {
		t.Fatalf("expected data.name=alice, got %v", data["name"])
	}
}

func TestDocAggregate_SortWithoutGroup(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	pipeline := []map[string]interface{}{
		{"$sort": map[string]interface{}{"name": float64(1), "age": float64(-1)}},
	}

	_, err := DocAggregate(ctx, db, "users", pipeline)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	// Without $group, sort keys use data->>'key' expressions
	assertContains(t, last.query, "data->>'age' DESC")
	assertContains(t, last.query, "data->>'name' ASC")
}

func TestDocAggregate_SortWithGroup(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "total"},
		nil)

	pipeline := []map[string]interface{}{
		{"$group": map[string]interface{}{
			"_id":   "$category",
			"total": map[string]interface{}{"$sum": "$price"},
		}},
		{"$sort": map[string]interface{}{"total": float64(-1)}},
	}

	_, err := DocAggregate(ctx, db, "products", pipeline)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	// With $group, sort keys are aliases
	assertContains(t, last.query, "ORDER BY total DESC")
	assertNotContains(t, last.query, "data->>'total'")
}

func TestDocAggregate_UnsupportedStage(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	pipeline := []map[string]interface{}{
		{"$bucket": map[string]interface{}{"groupBy": "$price"}},
	}

	_, err := DocAggregate(ctx, db, "users", pipeline)
	if err == nil {
		t.Fatal("expected error for unsupported pipeline stage")
	}
	assertContains(t, err.Error(), "unsupported pipeline stage")
}

func TestDocAggregate_EmptyPipeline(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	results, err := DocAggregate(ctx, db, "users", nil)
	if err != nil {
		t.Fatal(err)
	}
	if results != nil {
		t.Fatalf("expected nil for empty pipeline, got %v", results)
	}
}

func TestDocAggregate_InvalidCollection(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	_, err := DocAggregate(ctx, db, "bad;name", []map[string]interface{}{
		{"$match": map[string]interface{}{"a": "b"}},
	})
	if err == nil {
		t.Fatal("expected error for invalid collection name")
	}
	assertContains(t, err.Error(), "invalid identifier")
}

func TestDocAggregate_MultipleKeysInStage(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	pipeline := []map[string]interface{}{
		{"$match": nil, "$sort": map[string]interface{}{"x": float64(1)}},
	}

	_, err := DocAggregate(ctx, db, "users", pipeline)
	if err == nil {
		t.Fatal("expected error for stage with multiple keys")
	}
	assertContains(t, err.Error(), "exactly one key")
}

// --- Comparison Operators ---

func TestBuildFilter_GtNumeric(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	_, err := DocFind(ctx, db, "users", map[string]interface{}{
		"age": map[string]interface{}{"$gt": float64(21)},
	})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "(data->>'age')::numeric > $1")
	assertNotContains(t, last.query, "@>")
}

func TestBuildFilter_GteLteRange(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	_, err := DocFind(ctx, db, "users", map[string]interface{}{
		"score": map[string]interface{}{
			"$gte": float64(50),
			"$lte": float64(100),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "(data->>'score')::numeric >= $1")
	assertContains(t, last.query, "(data->>'score')::numeric <= $2")
}

func TestBuildFilter_LtString(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	_, err := DocFind(ctx, db, "users", map[string]interface{}{
		"name": map[string]interface{}{"$lt": "M"},
	})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "data->>'name' < $1")
	assertNotContains(t, last.query, "numeric")
}

func TestBuildFilter_EqAndNe(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	_, err := DocFind(ctx, db, "users", map[string]interface{}{
		"role":   map[string]interface{}{"$eq": "admin"},
		"status": map[string]interface{}{"$ne": "banned"},
	})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "data->>'role' = $1")
	assertContains(t, last.query, "data->>'status' != $2")
}

func TestBuildFilter_In(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	_, err := DocFind(ctx, db, "users", map[string]interface{}{
		"role": map[string]interface{}{
			"$in": []interface{}{"admin", "editor"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "data->>'role' IN ($1, $2)")
}

func TestBuildFilter_Nin(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	_, err := DocFind(ctx, db, "users", map[string]interface{}{
		"status": map[string]interface{}{
			"$nin": []interface{}{"banned", "deleted"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "data->>'status' NOT IN ($1, $2)")
}

func TestBuildFilter_Exists(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	_, err := DocFind(ctx, db, "users", map[string]interface{}{
		"email": map[string]interface{}{"$exists": true},
	})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "data ? $1")
}

func TestBuildFilter_NotExists(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	_, err := DocFind(ctx, db, "users", map[string]interface{}{
		"phone": map[string]interface{}{"$exists": false},
	})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "NOT (data ? $1)")
}

func TestBuildFilter_Regex(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	_, err := DocFind(ctx, db, "users", map[string]interface{}{
		"email": map[string]interface{}{"$regex": ".*@example\\.com$"},
	})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "data->>'email' ~ $1")
}

func TestBuildFilter_DotNotation(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	_, err := DocFind(ctx, db, "users", map[string]interface{}{
		"address.city": map[string]interface{}{"$eq": "Portland"},
	})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "data->'address'->>'city' = $1")
}

func TestBuildFilter_MixedContainmentAndOperators(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	_, err := DocFind(ctx, db, "users", map[string]interface{}{
		"role": "admin",
		"age":  map[string]interface{}{"$gte": float64(18)},
	})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	// Containment clause first ($1), operator clause second ($2)
	assertContains(t, last.query, "data @> $1::jsonb")
	assertContains(t, last.query, "(data->>'age')::numeric >= $2")
}

func TestBuildFilter_InvalidDotKey(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	_, err := DocFind(ctx, db, "users", map[string]interface{}{
		"bad;key.field": map[string]interface{}{"$gt": float64(1)},
	})
	if err == nil {
		t.Fatal("expected error for invalid dot-notation key")
	}
	assertContains(t, err.Error(), "invalid filter key")
}

// --- expandDotKeys ---

func TestExpandDotKeys_NoDots(t *testing.T) {
	input := map[string]interface{}{"name": "alice", "age": float64(30)}
	got := expandDotKeys(input)
	if !reflect.DeepEqual(got, input) {
		t.Fatalf("expected %v, got %v", input, got)
	}
}

func TestExpandDotKeys_SingleDot(t *testing.T) {
	input := map[string]interface{}{"addr.city": "NY"}
	want := map[string]interface{}{"addr": map[string]interface{}{"city": "NY"}}
	got := expandDotKeys(input)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestExpandDotKeys_MultipleDots(t *testing.T) {
	input := map[string]interface{}{"a.b.c": "deep"}
	want := map[string]interface{}{"a": map[string]interface{}{"b": map[string]interface{}{"c": "deep"}}}
	got := expandDotKeys(input)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestExpandDotKeys_SharedPrefix(t *testing.T) {
	input := map[string]interface{}{"a.b": float64(1), "a.c": float64(2)}
	got := expandDotKeys(input)
	aMap, ok := got["a"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected nested map at 'a', got %T", got["a"])
	}
	if aMap["b"] != float64(1) || aMap["c"] != float64(2) {
		t.Fatalf("expected {b:1, c:2}, got %v", aMap)
	}
}

func TestExpandDotKeys_MixedPlainAndDotted(t *testing.T) {
	input := map[string]interface{}{"role": "admin", "addr.city": "NY"}
	got := expandDotKeys(input)
	if got["role"] != "admin" {
		t.Fatalf("expected role=admin, got %v", got["role"])
	}
	addrMap, ok := got["addr"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected nested map at 'addr', got %T", got["addr"])
	}
	if addrMap["city"] != "NY" {
		t.Fatalf("expected city=NY, got %v", addrMap["city"])
	}
}

func TestExpandDotKeys_EmptyMap(t *testing.T) {
	input := map[string]interface{}{}
	got := expandDotKeys(input)
	if len(got) != 0 {
		t.Fatalf("expected empty map, got %v", got)
	}
}

func TestBuildFilter_DotNotationContainment(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	_, err := DocFind(ctx, db, "users", map[string]interface{}{
		"addr.city": "NY",
	})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "data @> $1::jsonb")
	// The param should be the expanded nested JSON
	paramStr, ok := last.args[0].(string)
	if !ok {
		t.Fatalf("expected string param, got %T", last.args[0])
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(paramStr), &parsed); err != nil {
		t.Fatal(err)
	}
	addrMap, ok := parsed["addr"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected nested addr map, got %T", parsed["addr"])
	}
	if addrMap["city"] != "NY" {
		t.Fatalf("expected city=NY, got %v", addrMap["city"])
	}
}

func TestBuildFilter_DotNotationContainmentDeep(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	_, err := DocFind(ctx, db, "users", map[string]interface{}{
		"addr.geo.lat": float64(40.7),
	})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "data @> $1::jsonb")
	paramStr, ok := last.args[0].(string)
	if !ok {
		t.Fatalf("expected string param, got %T", last.args[0])
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(paramStr), &parsed); err != nil {
		t.Fatal(err)
	}
	addrMap, ok := parsed["addr"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected nested addr map, got %T", parsed["addr"])
	}
	geoMap, ok := addrMap["geo"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected nested geo map, got %T", addrMap["geo"])
	}
	if geoMap["lat"] != float64(40.7) {
		t.Fatalf("expected lat=40.7, got %v", geoMap["lat"])
	}
}

func TestBuildFilter_UnsupportedOperator(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	_, err := DocFind(ctx, db, "users", map[string]interface{}{
		"age": map[string]interface{}{"$bogus": float64(1)},
	})
	if err == nil {
		t.Fatal("expected error for unsupported operator")
	}
	assertContains(t, err.Error(), "unsupported filter operator")
}

func TestBuildFilter_OperatorsInDocDelete(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	_, err := DocDelete(ctx, db, "users", map[string]interface{}{
		"age": map[string]interface{}{"$lt": float64(18)},
	})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "DELETE FROM users WHERE")
	assertContains(t, last.query, "(data->>'age')::numeric < $1")
}

func TestBuildFilter_OperatorsInDocCount(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"count"},
		[][]driver.Value{{int64(3)}})

	_, err := DocCount(ctx, db, "users", map[string]interface{}{
		"score": map[string]interface{}{"$gte": float64(90)},
	})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "SELECT COUNT(*) FROM users WHERE")
	assertContains(t, last.query, "(data->>'score')::numeric >= $1")
}

// --- Composite $group _id ---

func TestDocAggregate_CompositeID(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "total"},
		[][]driver.Value{
			{`{"dept":"eng","year":"2026"}`, int64(500)},
		})

	pipeline := []map[string]interface{}{
		{"$group": map[string]interface{}{
			"_id":   map[string]interface{}{"year": "$year", "dept": "$dept"},
			"total": map[string]interface{}{"$sum": "$revenue"},
		}},
	}

	results, err := DocAggregate(ctx, db, "sales", pipeline)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	// json_build_object with keys in alphabetical order
	assertContains(t, last.query, "json_build_object('dept', data->>'dept', 'year', data->>'year') AS _id")
	assertContains(t, last.query, "SUM((data->>'revenue')::numeric) AS total")
	// Multi-expression GROUP BY
	assertContains(t, last.query, "GROUP BY data->>'dept', data->>'year'")

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	// _id should be parsed back into a map
	idMap, ok := results[0]["_id"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected _id to be a map, got %T: %v", results[0]["_id"], results[0]["_id"])
	}
	if idMap["dept"] != "eng" {
		t.Fatalf("expected dept=eng, got %v", idMap["dept"])
	}
	if idMap["year"] != "2026" {
		t.Fatalf("expected year=2026, got %v", idMap["year"])
	}
}

func TestDocAggregate_CompositeID_WithMatch(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "count"},
		[][]driver.Value{
			{`{"region":"us","status":"active"}`, int64(42)},
		})

	pipeline := []map[string]interface{}{
		{"$match": map[string]interface{}{"org": "acme"}},
		{"$group": map[string]interface{}{
			"_id":   map[string]interface{}{"region": "$region", "status": "$status"},
			"count": map[string]interface{}{"$sum": float64(1)},
		}},
		{"$sort": map[string]interface{}{"count": float64(-1)}},
	}

	_, err := DocAggregate(ctx, db, "users", pipeline)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "json_build_object('region', data->>'region', 'status', data->>'status') AS _id")
	assertContains(t, last.query, "WHERE data @> $1::jsonb")
	assertContains(t, last.query, "GROUP BY data->>'region', data->>'status'")
	assertContains(t, last.query, "ORDER BY count DESC")
}

func TestDocAggregate_CompositeID_EmptyMap(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	pipeline := []map[string]interface{}{
		{"$group": map[string]interface{}{
			"_id": map[string]interface{}{},
		}},
	}

	_, err := DocAggregate(ctx, db, "orders", pipeline)
	if err == nil {
		t.Fatal("expected error for empty composite _id map")
	}
	assertContains(t, err.Error(), "must not be empty")
}

func TestDocAggregate_CompositeID_InvalidField(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	pipeline := []map[string]interface{}{
		{"$group": map[string]interface{}{
			"_id": map[string]interface{}{"ok": "$bad;field"},
		}},
	}

	_, err := DocAggregate(ctx, db, "orders", pipeline)
	if err == nil {
		t.Fatal("expected error for invalid field in composite _id")
	}
	assertContains(t, err.Error(), "invalid identifier")
}

// --- $push and $addToSet accumulators ---

func TestDocAggregate_Push(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "names"},
		[][]driver.Value{
			{"eng", "{alice,bob,alice}"},
		})

	pipeline := []map[string]interface{}{
		{"$group": map[string]interface{}{
			"_id":   "$dept",
			"names": map[string]interface{}{"$push": "$name"},
		}},
	}

	results, err := DocAggregate(ctx, db, "employees", pipeline)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "array_agg(data->>'name') AS names")
	assertContains(t, last.query, "GROUP BY data->>'dept'")

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	names, ok := results[0]["names"].([]string)
	if !ok {
		t.Fatalf("expected names to be []string, got %T: %v", results[0]["names"], results[0]["names"])
	}
	if len(names) != 3 || names[0] != "alice" || names[1] != "bob" || names[2] != "alice" {
		t.Fatalf("expected [alice bob alice], got %v", names)
	}
}

func TestDocAggregate_AddToSet(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "cities"},
		[][]driver.Value{
			{"us", "{portland,seattle}"},
		})

	pipeline := []map[string]interface{}{
		{"$group": map[string]interface{}{
			"_id":    "$country",
			"cities": map[string]interface{}{"$addToSet": "$city"},
		}},
	}

	results, err := DocAggregate(ctx, db, "offices", pipeline)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "array_agg(DISTINCT data->>'city') AS cities")
	assertContains(t, last.query, "GROUP BY data->>'country'")

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	cities, ok := results[0]["cities"].([]string)
	if !ok {
		t.Fatalf("expected cities to be []string, got %T: %v", results[0]["cities"], results[0]["cities"])
	}
	if len(cities) != 2 || cities[0] != "portland" || cities[1] != "seattle" {
		t.Fatalf("expected [portland seattle], got %v", cities)
	}
}

func TestDocAggregate_PushWithCompositeID(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "items"},
		[][]driver.Value{
			{`{"category":"books","year":"2026"}`, "{novel,memoir}"},
		})

	pipeline := []map[string]interface{}{
		{"$group": map[string]interface{}{
			"_id":   map[string]interface{}{"category": "$category", "year": "$year"},
			"items": map[string]interface{}{"$push": "$title"},
		}},
	}

	results, err := DocAggregate(ctx, db, "products", pipeline)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "json_build_object('category', data->>'category', 'year', data->>'year') AS _id")
	assertContains(t, last.query, "array_agg(data->>'title') AS items")
	assertContains(t, last.query, "GROUP BY data->>'category', data->>'year'")

	// Verify composite _id is parsed as map
	idMap, ok := results[0]["_id"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected _id to be a map, got %T", results[0]["_id"])
	}
	if idMap["category"] != "books" {
		t.Fatalf("expected category=books, got %v", idMap["category"])
	}

	// Verify array is parsed
	items, ok := results[0]["items"].([]string)
	if !ok {
		t.Fatalf("expected items to be []string, got %T", results[0]["items"])
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
}

func TestDocAggregate_AddToSetInvalidField(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	pipeline := []map[string]interface{}{
		{"$group": map[string]interface{}{
			"_id":   "$dept",
			"names": map[string]interface{}{"$addToSet": "not_a_field_ref"},
		}},
	}

	_, err := DocAggregate(ctx, db, "employees", pipeline)
	if err == nil {
		t.Fatal("expected error for invalid $addToSet field reference")
	}
	assertContains(t, err.Error(), "$addToSet")
}

// --- $project ---

func TestDocAggregate_ProjectIncludeFields(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "name", "email"},
		[][]driver.Value{
			{"11111111-1111-1111-1111-111111111111", "alice", "alice@example.com"},
		})

	pipeline := []map[string]interface{}{
		{"$project": map[string]interface{}{
			"name":  float64(1),
			"email": float64(1),
		}},
	}

	results, err := DocAggregate(ctx, db, "users", pipeline)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	// _id is projected by its own name (not aliased from `id`) since the
	// schema column IS _id.
	assertContains(t, last.query, "SELECT _id, data->>'email' AS email, data->>'name' AS name")
	assertContains(t, last.query, "FROM users")

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func TestDocAggregate_ProjectExcludeID(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"name"},
		[][]driver.Value{
			{"alice"},
		})

	pipeline := []map[string]interface{}{
		{"$project": map[string]interface{}{
			"_id":  float64(0),
			"name": float64(1),
		}},
	}

	_, err := DocAggregate(ctx, db, "users", pipeline)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	// _id excluded via $project: {_id: 0} — neither `id AS _id` nor a bare `_id`
	// selector should appear.
	assertNotContains(t, last.query, "id AS _id")
	assertNotContains(t, last.query, "SELECT _id,")
	assertContains(t, last.query, "data->>'name' AS name")
}

func TestDocAggregate_ProjectRename(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "full_name"},
		[][]driver.Value{
			{"11111111-1111-1111-1111-111111111111", "alice"},
		})

	pipeline := []map[string]interface{}{
		{"$project": map[string]interface{}{
			"full_name": "$name",
		}},
	}

	_, err := DocAggregate(ctx, db, "users", pipeline)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "data->>'name' AS full_name")
	// _id is selected by its schema column name (no alias needed).
	assertContains(t, last.query, "SELECT _id, data->>'name' AS full_name")
}

func TestDocAggregate_ProjectWithMatch(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"name", "email"},
		[][]driver.Value{
			{"alice", "alice@example.com"},
		})

	pipeline := []map[string]interface{}{
		{"$match": map[string]interface{}{"status": "active"}},
		{"$project": map[string]interface{}{
			"_id":   float64(0),
			"name":  float64(1),
			"email": float64(1),
		}},
	}

	_, err := DocAggregate(ctx, db, "users", pipeline)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	// _id excluded — neither the legacy `id AS _id` alias nor a bare `_id`
	// selector should appear in the SELECT list.
	assertNotContains(t, last.query, "id AS _id")
	assertNotContains(t, last.query, "SELECT _id,")
	assertContains(t, last.query, "data->>'email' AS email")
	assertContains(t, last.query, "data->>'name' AS name")
	assertContains(t, last.query, "WHERE data @> $1::jsonb")
}

func TestDocAggregate_ProjectInvalidField(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	pipeline := []map[string]interface{}{
		{"$project": map[string]interface{}{
			"bad;field": float64(1),
		}},
	}

	_, err := DocAggregate(ctx, db, "users", pipeline)
	if err == nil {
		t.Fatal("expected error for invalid $project field")
	}
	assertContains(t, err.Error(), "$project")
}

// --- $unwind ---

func TestDocAggregate_UnwindString(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	pipeline := []map[string]interface{}{
		{"$unwind": "$tags"},
	}

	_, err := DocAggregate(ctx, db, "items", pipeline)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "CROSS JOIN jsonb_array_elements_text(data->'tags') AS tags")
	assertContains(t, last.query, "FROM items")
}

func TestDocAggregate_UnwindMap(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		nil)

	pipeline := []map[string]interface{}{
		{"$unwind": map[string]interface{}{"path": "$colors"}},
	}

	_, err := DocAggregate(ctx, db, "products", pipeline)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "CROSS JOIN jsonb_array_elements_text(data->'colors') AS colors")
}

func TestDocAggregate_UnwindThenGroup(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "count"},
		[][]driver.Value{
			{"fiction", int64(5)},
		})

	pipeline := []map[string]interface{}{
		{"$unwind": "$tags"},
		{"$group": map[string]interface{}{
			"_id":   "$tags",
			"count": map[string]interface{}{"$sum": float64(1)},
		}},
	}

	results, err := DocAggregate(ctx, db, "books", pipeline)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "CROSS JOIN jsonb_array_elements_text(data->'tags') AS tags")
	// Group on the unwound alias, not data->>'tags'
	assertContains(t, last.query, "tags AS _id")
	assertContains(t, last.query, "GROUP BY tags")
	assertContains(t, last.query, "COUNT(*) AS count")

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0]["_id"] != "fiction" {
		t.Fatalf("expected _id=fiction, got %v", results[0]["_id"])
	}
}

func TestDocAggregate_UnwindInvalidField(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	pipeline := []map[string]interface{}{
		{"$unwind": "$bad;field"},
	}

	_, err := DocAggregate(ctx, db, "items", pipeline)
	if err == nil {
		t.Fatal("expected error for invalid $unwind field")
	}
	assertContains(t, err.Error(), "$unwind")
}

func TestDocAggregate_UnwindInvalidFormat(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	pipeline := []map[string]interface{}{
		{"$unwind": "no_dollar_prefix"},
	}

	_, err := DocAggregate(ctx, db, "items", pipeline)
	if err == nil {
		t.Fatal("expected error for $unwind without $ prefix")
	}
	assertContains(t, err.Error(), "$unwind")
}

// --- $lookup ---

func TestDocAggregate_Lookup(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"inventory_docs", "_id", "data", "created_at"},
		[][]driver.Value{
			{`[{"sku":"abc","qty":10}]`, "11111111-1111-1111-1111-111111111111", `{"item":"widget","sku":"abc"}`, "2026-01-01T00:00:00Z"},
		})

	pipeline := []map[string]interface{}{
		{"$lookup": map[string]interface{}{
			"from":         "inventory",
			"localField":   "sku",
			"foreignField": "sku",
			"as":           "inventory_docs",
		}},
	}

	results, err := DocAggregate(ctx, db, "orders", pipeline)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "SELECT (SELECT COALESCE(json_agg(inventory.data), '[]'::json) FROM inventory WHERE inventory.data->>'sku' = data->>'sku') AS inventory_docs")
	// _id now a direct column reference, not aliased from a legacy `id` column.
	assertContains(t, last.query, "_id, data, created_at")
	assertContains(t, last.query, "FROM orders")

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	// inventory_docs should be parsed as a JSON array
	inv, ok := results[0]["inventory_docs"].([]interface{})
	if !ok {
		t.Fatalf("expected inventory_docs to be []interface{}, got %T: %v", results[0]["inventory_docs"], results[0]["inventory_docs"])
	}
	if len(inv) != 1 {
		t.Fatalf("expected 1 inventory doc, got %d", len(inv))
	}
}

func TestDocAggregate_LookupMissingRequiredField(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	pipeline := []map[string]interface{}{
		{"$lookup": map[string]interface{}{
			"from":       "inventory",
			"localField": "sku",
			// missing foreignField and as
		}},
	}

	_, err := DocAggregate(ctx, db, "orders", pipeline)
	if err == nil {
		t.Fatal("expected error for missing $lookup field")
	}
	assertContains(t, err.Error(), "$lookup requires")
}

func TestDocAggregate_LookupInvalidFrom(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	pipeline := []map[string]interface{}{
		{"$lookup": map[string]interface{}{
			"from":         "bad;table",
			"localField":   "sku",
			"foreignField": "sku",
			"as":           "docs",
		}},
	}

	_, err := DocAggregate(ctx, db, "orders", pipeline)
	if err == nil {
		t.Fatal("expected error for invalid $lookup from collection")
	}
	assertContains(t, err.Error(), "$lookup")
	assertContains(t, err.Error(), "invalid")
}

// --- DocWatch ---

func TestDocWatch_TriggerDDL(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	// DocWatch executes DDL synchronously then starts a listener goroutine.
	// The goroutine will hang on a fake conn, but we only need to verify DDL.
	_, err := DocWatch(ctx, db, "postgresql://127.0.0.1:1/db", "events", func(op, data string) {})
	if err != nil {
		t.Fatal(err)
	}

	captures := drv.allCaptures()
	// Expect: CREATE TABLE, CREATE OR REPLACE FUNCTION, CREATE OR REPLACE TRIGGER.
	// CREATE OR REPLACE TRIGGER (PG14+) replaces the old DROP+CREATE pair
	// to avoid a race between concurrent DocWatch calls.
	found := false
	for _, c := range captures {
		if strings.Contains(c.query, "CREATE OR REPLACE FUNCTION events_notify_changes") {
			found = true
			assertContains(t, c.query, "pg_notify('events_changes'")
			assertContains(t, c.query, "TG_OP")
			assertContains(t, c.query, "LANGUAGE plpgsql")
		}
	}
	if !found {
		t.Fatal("expected CREATE OR REPLACE FUNCTION for watch trigger")
	}

	// Verify trigger creation uses atomic CREATE OR REPLACE.
	triggerFound := false
	for _, c := range captures {
		if strings.Contains(c.query, "CREATE OR REPLACE TRIGGER events_watch_trigger") {
			triggerFound = true
			assertContains(t, c.query, "AFTER INSERT OR UPDATE OR DELETE")
			assertContains(t, c.query, "FOR EACH ROW")
			assertContains(t, c.query, "events_notify_changes()")
		}
	}
	if !triggerFound {
		t.Fatal("expected CREATE OR REPLACE TRIGGER for watch")
	}

	// Ensure no racy DROP TRIGGER IF EXISTS + CREATE TRIGGER pair sneaks
	// back in later.
	for _, c := range captures {
		if strings.Contains(c.query, "DROP TRIGGER IF EXISTS events_watch_trigger") {
			t.Fatal("DocWatch should not emit DROP TRIGGER IF EXISTS (racy); use CREATE OR REPLACE TRIGGER")
		}
	}
}

func TestDocWatch_InvalidCollection(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	_, err := DocWatch(ctx, db, "postgresql://fake/db", "bad;name", func(op, data string) {})
	if err == nil {
		t.Fatal("expected error for invalid collection name")
	}
	assertContains(t, err.Error(), "invalid identifier")
}

// --- DocUnwatch ---

func TestDocUnwatch_DropsDDL(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	err := DocUnwatch(ctx, db, "events")
	if err != nil {
		t.Fatal(err)
	}

	captures := drv.allCaptures()
	if len(captures) < 2 {
		t.Fatalf("expected at least 2 DDL statements, got %d", len(captures))
	}
	assertContains(t, captures[0].query, "DROP TRIGGER IF EXISTS events_watch_trigger ON events")
	assertContains(t, captures[1].query, "DROP FUNCTION IF EXISTS events_notify_changes()")
}

func TestDocUnwatch_InvalidCollection(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	err := DocUnwatch(ctx, db, "bad name!")
	if err == nil {
		t.Fatal("expected error for invalid collection name")
	}
	assertContains(t, err.Error(), "invalid identifier")
}

// --- DocCreateTtlIndex ---

func TestDocCreateTtlIndex_TriggerDDL(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	err := DocCreateTtlIndex(ctx, db, "sessions", 3600)
	if err != nil {
		t.Fatal(err)
	}

	captures := drv.allCaptures()
	// Expect: CREATE TABLE, CREATE OR REPLACE FUNCTION, DROP TRIGGER, CREATE TRIGGER
	funcFound := false
	for _, c := range captures {
		if strings.Contains(c.query, "CREATE OR REPLACE FUNCTION sessions_ttl_cleanup") {
			funcFound = true
			assertContains(t, c.query, "INTERVAL '3600 seconds'")
			assertContains(t, c.query, "DELETE FROM sessions")
			assertContains(t, c.query, "created_at < NOW()")
		}
	}
	if !funcFound {
		t.Fatal("expected CREATE OR REPLACE FUNCTION for TTL cleanup")
	}

	triggerFound := false
	for _, c := range captures {
		if strings.Contains(c.query, "CREATE TRIGGER sessions_ttl_trigger") {
			triggerFound = true
			assertContains(t, c.query, "AFTER INSERT ON sessions")
			assertContains(t, c.query, "FOR EACH STATEMENT")
		}
	}
	if !triggerFound {
		t.Fatal("expected CREATE TRIGGER for TTL")
	}
}

func TestDocCreateTtlIndex_InvalidInputs(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	// Invalid collection
	err := DocCreateTtlIndex(ctx, db, "bad;name", 3600)
	if err == nil {
		t.Fatal("expected error for invalid collection name")
	}
	assertContains(t, err.Error(), "invalid identifier")

	// Zero TTL
	err = DocCreateTtlIndex(ctx, db, "sessions", 0)
	if err == nil {
		t.Fatal("expected error for zero TTL")
	}
	assertContains(t, err.Error(), "ttlSeconds must be positive")

	// Negative TTL
	err = DocCreateTtlIndex(ctx, db, "sessions", -10)
	if err == nil {
		t.Fatal("expected error for negative TTL")
	}
	assertContains(t, err.Error(), "ttlSeconds must be positive")
}

// --- DocRemoveTtlIndex ---

func TestDocRemoveTtlIndex_DropsDDL(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	err := DocRemoveTtlIndex(ctx, db, "sessions")
	if err != nil {
		t.Fatal(err)
	}

	captures := drv.allCaptures()
	if len(captures) < 2 {
		t.Fatalf("expected at least 2 DDL statements, got %d", len(captures))
	}
	assertContains(t, captures[0].query, "DROP TRIGGER IF EXISTS sessions_ttl_trigger ON sessions")
	assertContains(t, captures[1].query, "DROP FUNCTION IF EXISTS sessions_ttl_cleanup()")
}

// --- DocCreateCapped ---

func TestDocCreateCapped_TriggerDDL(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	err := DocCreateCapped(ctx, db, "logs", 1000)
	if err != nil {
		t.Fatal(err)
	}

	captures := drv.allCaptures()
	funcFound := false
	for _, c := range captures {
		if strings.Contains(c.query, "CREATE OR REPLACE FUNCTION logs_cap_enforce") {
			funcFound = true
			assertContains(t, c.query, "DELETE FROM logs")
			// UUIDs don't sort by insert order, so we keep the N most recent
			// rows by created_at (tie-broken on _id for determinism).
			assertContains(t, c.query, "ORDER BY created_at DESC, _id DESC LIMIT 1000")
			assertNotContains(t, c.query, "ORDER BY id ")
		}
	}
	if !funcFound {
		t.Fatal("expected CREATE OR REPLACE FUNCTION for cap enforcement")
	}

	triggerFound := false
	for _, c := range captures {
		if strings.Contains(c.query, "CREATE TRIGGER logs_cap_trigger") {
			triggerFound = true
			assertContains(t, c.query, "AFTER INSERT ON logs")
			assertContains(t, c.query, "FOR EACH STATEMENT")
		}
	}
	if !triggerFound {
		t.Fatal("expected CREATE TRIGGER for cap")
	}
}

func TestDocCreateCapped_InvalidInputs(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	// Invalid collection
	err := DocCreateCapped(ctx, db, "bad;name", 100)
	if err == nil {
		t.Fatal("expected error for invalid collection name")
	}
	assertContains(t, err.Error(), "invalid identifier")

	// Zero maxDocs
	err = DocCreateCapped(ctx, db, "logs", 0)
	if err == nil {
		t.Fatal("expected error for zero maxDocs")
	}
	assertContains(t, err.Error(), "maxDocs must be positive")

	// Negative maxDocs
	err = DocCreateCapped(ctx, db, "logs", -5)
	if err == nil {
		t.Fatal("expected error for negative maxDocs")
	}
	assertContains(t, err.Error(), "maxDocs must be positive")
}

// --- DocRemoveCap ---

func TestDocRemoveCap_DropsDDL(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	err := DocRemoveCap(ctx, db, "logs")
	if err != nil {
		t.Fatal(err)
	}

	captures := drv.allCaptures()
	if len(captures) < 2 {
		t.Fatalf("expected at least 2 DDL statements, got %d", len(captures))
	}
	assertContains(t, captures[0].query, "DROP TRIGGER IF EXISTS logs_cap_trigger ON logs")
	assertContains(t, captures[1].query, "DROP FUNCTION IF EXISTS logs_cap_enforce()")
}

// --- DocCreateCollection ---

func TestDocCreateCollection_Logged(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	if err := DocCreateCollection(ctx, db, "users", false); err != nil {
		t.Fatal(err)
	}
	last := drv.lastCapture()
	assertContains(t, last.query, "CREATE TABLE IF NOT EXISTS users")
	assertNotContains(t, last.query, "UNLOGGED")
	// v0.2 schema matches the 6-wrapper consensus (Python, JS, Ruby, Java,
	// PHP, .NET): _id UUID primary key, data JSONB, created_at TIMESTAMPTZ.
	assertContains(t, last.query, "_id UUID PRIMARY KEY DEFAULT gen_random_uuid()")
	assertContains(t, last.query, "data JSONB NOT NULL")
	assertContains(t, last.query, "created_at TIMESTAMPTZ")
	assertNotContains(t, last.query, "BIGSERIAL")
}

func TestDocCreateCollection_Unlogged(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	if err := DocCreateCollection(ctx, db, "events", true); err != nil {
		t.Fatal(err)
	}
	last := drv.lastCapture()
	assertContains(t, last.query, "CREATE UNLOGGED TABLE IF NOT EXISTS events")
}

func TestDocCreateCollection_InvalidIdentifier(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	if err := DocCreateCollection(ctx, db, "bad name!", false); err == nil {
		t.Fatal("expected error for invalid identifier")
	}
}

// --- DocFindOneAndUpdate ---

func TestDocFindOneAndUpdate_SQLAndReturn(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		[][]driver.Value{
			{"77777777-7777-7777-7777-777777777777", `{"name":"alice","role":"admin"}`, "2026-01-01T00:00:00Z"},
		})

	result, err := DocFindOneAndUpdate(ctx, db, "users",
		map[string]string{"name": "alice"},
		map[string]string{"role": "admin"})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "WITH target AS")
	assertContains(t, last.query, "SELECT _id FROM users")
	assertContains(t, last.query, "LIMIT 1")
	assertNotContains(t, last.query, "ORDER BY")
	assertContains(t, last.query, "UPDATE users SET data = data ||")
	assertContains(t, last.query, "RETURNING users._id, users.data, users.created_at")

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["_id"] != "77777777-7777-7777-7777-777777777777" {
		t.Fatalf("expected _id UUID string, got %v", result["_id"])
	}
	data, ok := result["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected data to be a map, got %T: %v", result["data"], result["data"])
	}
	if data["role"] != "admin" {
		t.Fatalf("expected data.role=admin, got %v", data["role"])
	}
}

func TestDocFindOneAndUpdate_NotFound(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"_id", "data", "created_at"}, nil)

	result, err := DocFindOneAndUpdate(ctx, db, "users",
		map[string]string{"name": "nobody"},
		map[string]string{"x": "y"})
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Fatalf("expected nil for no match, got %v", result)
	}
}

func TestDocFindOneAndUpdate_InvalidCollection(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := DocFindOneAndUpdate(ctx, db, "bad!!", nil, nil)
	if err == nil {
		t.Fatal("expected error for invalid collection")
	}
}

// --- DocFindOneAndDelete ---

func TestDocFindOneAndDelete_SQLAndReturn(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		[][]driver.Value{
			{"33333333-3333-3333-3333-333333333333", `{"name":"alice"}`, "2026-01-01T00:00:00Z"},
		})

	result, err := DocFindOneAndDelete(ctx, db, "users",
		map[string]string{"name": "alice"})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "WITH target AS")
	assertContains(t, last.query, "SELECT _id FROM users")
	assertContains(t, last.query, "DELETE FROM users USING target")
	assertContains(t, last.query, "RETURNING users._id, users.data, users.created_at")

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	data, ok := result["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected data map, got %T: %v", result["data"], result["data"])
	}
	if data["name"] != "alice" {
		t.Fatalf("expected data.name=alice, got %v", data["name"])
	}
}

func TestDocFindOneAndDelete_NotFound(t *testing.T) {
	db, _ := newTestDB(t, []string{"_id", "data", "created_at"}, nil)

	result, err := DocFindOneAndDelete(ctx, db, "users",
		map[string]string{"name": "nobody"})
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

// --- DocDistinct ---

func TestDocDistinct_NoFilter(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"role"},
		[][]driver.Value{
			{"admin"}, {"user"}, {"guest"},
		})

	results, err := DocDistinct(ctx, db, "users", "role", nil)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "SELECT DISTINCT data->>'role' FROM users")
	assertContains(t, last.query, "WHERE data->>'role' IS NOT NULL")

	if len(results) != 3 {
		t.Fatalf("expected 3 values, got %d: %v", len(results), results)
	}
}

func TestDocDistinct_WithFilter(t *testing.T) {
	db, drv := newTestDB(t, []string{"role"}, nil)

	_, err := DocDistinct(ctx, db, "users", "role",
		map[string]string{"active": "true"})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "IS NOT NULL AND data @> $1::jsonb")
}

func TestDocDistinct_InvalidField(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	_, err := DocDistinct(ctx, db, "users", "bad name!", nil)
	if err == nil {
		t.Fatal("expected error for invalid field")
	}
}

// --- DocFindCursor ---

func TestDocFindCursor_YieldsAll(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		[][]driver.Value{
			{"11111111-1111-1111-1111-111111111111", `{"n":1}`, "2026-01-01T00:00:00Z"},
			{"22222222-2222-2222-2222-222222222222", `{"n":2}`, "2026-01-01T00:00:00Z"},
			{"33333333-3333-3333-3333-333333333333", `{"n":3}`, "2026-01-01T00:00:00Z"},
		})

	var collected []map[string]interface{}
	err := DocFindCursor(ctx, db, "items", nil,
		func(doc map[string]interface{}) (bool, error) {
			collected = append(collected, doc)
			return true, nil
		})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "SELECT _id, data, created_at FROM items")
	// No default ORDER BY — UUIDs don't sort meaningfully, and matching the
	// 6-wrapper consensus. Callers wanting order pass DocSort.
	assertNotContains(t, last.query, "ORDER BY")
	assertNotContains(t, last.query, "LIMIT")

	if len(collected) != 3 {
		t.Fatalf("expected 3 docs, got %d", len(collected))
	}
	if collected[0]["_id"] != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("expected first _id UUID, got %v", collected[0]["_id"])
	}
}

func TestDocFindCursor_EarlyStop(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"_id", "data", "created_at"},
		[][]driver.Value{
			{"11111111-1111-1111-1111-111111111111", `{}`, "2026-01-01T00:00:00Z"},
			{"22222222-2222-2222-2222-222222222222", `{}`, "2026-01-01T00:00:00Z"},
			{"33333333-3333-3333-3333-333333333333", `{}`, "2026-01-01T00:00:00Z"},
		})

	count := 0
	err := DocFindCursor(ctx, db, "items", nil,
		func(doc map[string]interface{}) (bool, error) {
			count++
			return count < 2, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("expected callback to stop after 2 calls, got %d", count)
	}
}

func TestDocFindCursor_NilCallback(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	err := DocFindCursor(ctx, db, "items", nil, nil)
	if err == nil {
		t.Fatal("expected error for nil callback")
	}
}

func TestDocFindCursor_InvalidCollection(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	err := DocFindCursor(ctx, db, "1bad", nil,
		func(doc map[string]interface{}) (bool, error) { return true, nil })
	if err == nil {
		t.Fatal("expected error for invalid collection")
	}
}
