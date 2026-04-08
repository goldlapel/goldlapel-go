package goldlapel

import (
	"database/sql/driver"
	"strings"
	"testing"
)

// --- DocInsert ---

func TestDocInsert_SQLGeneration(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"id", "data", "created_at"},
		[][]driver.Value{{int64(1), `{"name":"alice"}`, "2026-01-01T00:00:00Z"}})

	result, err := DocInsert(db, "users", map[string]interface{}{"name": "alice"})
	if err != nil {
		t.Fatal(err)
	}

	captures := drv.allCaptures()
	// First capture: CREATE TABLE IF NOT EXISTS
	assertContains(t, captures[0].query, "CREATE TABLE IF NOT EXISTS users")
	assertContains(t, captures[0].query, "data JSONB NOT NULL")
	assertContains(t, captures[0].query, "BIGSERIAL PRIMARY KEY")

	// Second capture: INSERT RETURNING
	assertContains(t, captures[1].query, "INSERT INTO users")
	assertContains(t, captures[1].query, "$1::jsonb")
	assertContains(t, captures[1].query, "RETURNING id, data, created_at")

	if result["name"] != "alice" {
		t.Fatalf("expected name=alice, got %v", result["name"])
	}
	if result["_id"] != int64(1) {
		t.Fatalf("expected _id=1, got %v", result["_id"])
	}
	if result["_created_at"] != "2026-01-01T00:00:00Z" {
		t.Fatalf("expected _created_at, got %v", result["_created_at"])
	}
}

func TestDocInsert_InvalidCollection(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	_, err := DocInsert(db, "drop table;--", map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for invalid collection name")
	}
	assertContains(t, err.Error(), "invalid identifier")
}

// --- DocInsertMany ---

func TestDocInsertMany_SQLGeneration(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"id", "data", "created_at"},
		[][]driver.Value{
			{int64(1), `{"name":"alice"}`, "2026-01-01T00:00:00Z"},
			{int64(2), `{"name":"bob"}`, "2026-01-01T00:00:00Z"},
		})

	docs := []interface{}{
		map[string]interface{}{"name": "alice"},
		map[string]interface{}{"name": "bob"},
	}
	results, err := DocInsertMany(db, "users", docs)
	if err != nil {
		t.Fatal(err)
	}

	captures := drv.allCaptures()
	// Second capture is the INSERT (first is CREATE TABLE)
	insertQ := captures[1].query
	assertContains(t, insertQ, "INSERT INTO users (data) VALUES")
	assertContains(t, insertQ, "$1::jsonb")
	assertContains(t, insertQ, "$2::jsonb")
	assertContains(t, insertQ, "RETURNING id, data, created_at")

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0]["name"] != "alice" {
		t.Fatalf("expected name=alice, got %v", results[0]["name"])
	}
	if results[1]["name"] != "bob" {
		t.Fatalf("expected name=bob, got %v", results[1]["name"])
	}
}

func TestDocInsertMany_Empty(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	results, err := DocInsertMany(db, "users", nil)
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
		[]string{"id", "data", "created_at"},
		[][]driver.Value{
			{int64(1), `{"name":"alice"}`, "2026-01-01T00:00:00Z"},
		})

	results, err := DocFind(db, "users", nil)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "SELECT id, data, created_at FROM users")
	assertNotContains(t, last.query, "WHERE")
	assertContains(t, last.query, "ORDER BY id")
	assertContains(t, last.query, "LIMIT $1")

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func TestDocFind_WithFilter(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"id", "data", "created_at"},
		[][]driver.Value{
			{int64(1), `{"role":"admin"}`, "2026-01-01T00:00:00Z"},
		})

	_, err := DocFind(db, "users", map[string]string{"role": "admin"})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "WHERE data @> $1::jsonb")
	assertContains(t, last.query, "LIMIT $2")
}

func TestDocFind_EmptyFilter(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"id", "data", "created_at"},
		nil)

	_, err := DocFind(db, "users", map[string]string{})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	// Empty map {} should not produce a WHERE clause
	assertNotContains(t, last.query, "WHERE")
}

func TestDocFind_WithSort(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"id", "data", "created_at"},
		nil)

	_, err := DocFind(db, "users", nil, DocSort(map[string]int{"name": 1, "age": -1}))
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	// Keys sorted alphabetically: age DESC, name ASC
	assertContains(t, last.query, "ORDER BY data->>'age' DESC, data->>'name' ASC")
}

func TestDocFind_WithLimitAndSkip(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"id", "data", "created_at"},
		nil)

	_, err := DocFind(db, "users", nil, DocLimit(10), DocSkip(20))
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

	_, err := DocFind(db, "123bad", nil)
	if err == nil {
		t.Fatal("expected error for invalid collection name")
	}
}

// --- DocFindOne ---

func TestDocFindOne_WithFilter(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"id", "data", "created_at"},
		[][]driver.Value{
			{int64(1), `{"name":"alice"}`, "2026-01-01T00:00:00Z"},
		})

	result, err := DocFindOne(db, "users", map[string]string{"name": "alice"})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "WHERE data @> $1::jsonb")
	assertContains(t, last.query, "LIMIT 1")

	if result["name"] != "alice" {
		t.Fatalf("expected name=alice, got %v", result["name"])
	}
}

func TestDocFindOne_NotFound(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"id", "data", "created_at"},
		nil)

	result, err := DocFindOne(db, "users", map[string]string{"name": "nobody"})
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

	_, err := DocUpdate(db, "users",
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

	_, err := DocUpdate(db, "bad name!", nil, nil)
	if err == nil {
		t.Fatal("expected error for invalid collection name")
	}
}

// --- DocUpdateOne ---

func TestDocUpdateOne_CTEWithLimit1(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	_, err := DocUpdateOne(db, "users",
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

	_, err := DocDelete(db, "users", map[string]string{"role": "banned"})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "DELETE FROM users WHERE data @> $1::jsonb")
}

func TestDocDelete_InvalidCollection(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	_, err := DocDelete(db, "1invalid", nil)
	if err == nil {
		t.Fatal("expected error for invalid collection name")
	}
}

// --- DocDeleteOne ---

func TestDocDeleteOne_CTEWithLimit1(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	_, err := DocDeleteOne(db, "users", map[string]string{"name": "alice"})
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

	count, err := DocCount(db, "users", nil)
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

	count, err := DocCount(db, "users", map[string]string{"role": "admin"})
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

	_, err := DocCount(db, "users", map[string]string{})
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertNotContains(t, last.query, "WHERE")
}

func TestDocCount_InvalidCollection(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	_, err := DocCount(db, "bad;name", nil)
	if err == nil {
		t.Fatal("expected error for invalid collection name")
	}
}

// --- DocCreateIndex ---

func TestDocCreateIndex_FullIndex(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	err := DocCreateIndex(db, "users")
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

	err := DocCreateIndex(db, "users", "email")
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "CREATE INDEX IF NOT EXISTS users_email_idx")
	assertContains(t, last.query, "(data->'email')")
}

func TestDocCreateIndex_MultipleKeys(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	err := DocCreateIndex(db, "users", "first_name", "last_name")
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

	err := DocCreateIndex(db, "bad;name")
	if err == nil {
		t.Fatal("expected error for invalid collection name")
	}
}

func TestDocCreateIndex_InvalidKey(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	err := DocCreateIndex(db, "users", "bad;key")
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
		{"DocInsert", func() error { _, err := DocInsert(db, badName, map[string]string{}); return err }},
		{"DocInsertMany", func() error { _, err := DocInsertMany(db, badName, []interface{}{map[string]string{"a": "b"}}); return err }},
		{"DocFind", func() error { _, err := DocFind(db, badName, nil); return err }},
		{"DocFindOne", func() error { _, err := DocFindOne(db, badName, nil); return err }},
		{"DocUpdate", func() error { _, err := DocUpdate(db, badName, nil, nil); return err }},
		{"DocUpdateOne", func() error { _, err := DocUpdateOne(db, badName, nil, nil); return err }},
		{"DocDelete", func() error { _, err := DocDelete(db, badName, nil); return err }},
		{"DocDeleteOne", func() error { _, err := DocDeleteOne(db, badName, nil); return err }},
		{"DocCount", func() error { _, err := DocCount(db, badName, nil); return err }},
		{"DocCreateIndex", func() error { return DocCreateIndex(db, badName) }},
		{"DocAggregate", func() error {
			_, err := DocAggregate(db, badName, []map[string]interface{}{{"$match": nil}})
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
		[]string{"id", "data", "created_at"},
		nil)

	_, err := DocFind(db, "users", nil, DocSort(map[string]int{"bad;key": 1}))
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

	results, err := DocAggregate(db, "orders", pipeline)
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

	_, err := DocAggregate(db, "products", pipeline)
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

	results, err := DocAggregate(db, "orders", pipeline)
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
		[]string{"id", "data", "created_at"},
		[][]driver.Value{
			{int64(1), `{"status":"active","name":"alice"}`, "2026-01-01T00:00:00Z"},
		})

	pipeline := []map[string]interface{}{
		{"$match": map[string]interface{}{"status": "active"}},
	}

	results, err := DocAggregate(db, "users", pipeline)
	if err != nil {
		t.Fatal(err)
	}

	last := drv.lastCapture()
	assertContains(t, last.query, "SELECT id AS _id, data, created_at FROM users")
	assertContains(t, last.query, "WHERE data @> $1::jsonb")
	assertNotContains(t, last.query, "GROUP BY")

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0]["name"] != "alice" {
		t.Fatalf("expected name=alice, got %v", results[0]["name"])
	}
}

func TestDocAggregate_SortWithoutGroup(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"id", "data", "created_at"},
		nil)

	pipeline := []map[string]interface{}{
		{"$sort": map[string]interface{}{"name": float64(1), "age": float64(-1)}},
	}

	_, err := DocAggregate(db, "users", pipeline)
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

	_, err := DocAggregate(db, "products", pipeline)
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
		{"$lookup": map[string]interface{}{"from": "other"}},
	}

	_, err := DocAggregate(db, "users", pipeline)
	if err == nil {
		t.Fatal("expected error for unsupported pipeline stage")
	}
	assertContains(t, err.Error(), "unsupported pipeline stage")
}

func TestDocAggregate_EmptyPipeline(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	results, err := DocAggregate(db, "users", nil)
	if err != nil {
		t.Fatal(err)
	}
	if results != nil {
		t.Fatalf("expected nil for empty pipeline, got %v", results)
	}
}

func TestDocAggregate_InvalidCollection(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)

	_, err := DocAggregate(db, "bad;name", []map[string]interface{}{
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

	_, err := DocAggregate(db, "users", pipeline)
	if err == nil {
		t.Fatal("expected error for stage with multiple keys")
	}
	assertContains(t, err.Error(), "exactly one key")
}

// --- Comparison Operators ---

func TestBuildFilter_GtNumeric(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"id", "data", "created_at"},
		nil)

	_, err := DocFind(db, "users", map[string]interface{}{
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
		[]string{"id", "data", "created_at"},
		nil)

	_, err := DocFind(db, "users", map[string]interface{}{
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
		[]string{"id", "data", "created_at"},
		nil)

	_, err := DocFind(db, "users", map[string]interface{}{
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
		[]string{"id", "data", "created_at"},
		nil)

	_, err := DocFind(db, "users", map[string]interface{}{
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
		[]string{"id", "data", "created_at"},
		nil)

	_, err := DocFind(db, "users", map[string]interface{}{
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
		[]string{"id", "data", "created_at"},
		nil)

	_, err := DocFind(db, "users", map[string]interface{}{
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
		[]string{"id", "data", "created_at"},
		nil)

	_, err := DocFind(db, "users", map[string]interface{}{
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
		[]string{"id", "data", "created_at"},
		nil)

	_, err := DocFind(db, "users", map[string]interface{}{
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
		[]string{"id", "data", "created_at"},
		nil)

	_, err := DocFind(db, "users", map[string]interface{}{
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
		[]string{"id", "data", "created_at"},
		nil)

	_, err := DocFind(db, "users", map[string]interface{}{
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
		[]string{"id", "data", "created_at"},
		nil)

	_, err := DocFind(db, "users", map[string]interface{}{
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
		[]string{"id", "data", "created_at"},
		nil)

	_, err := DocFind(db, "users", map[string]interface{}{
		"bad;key.field": map[string]interface{}{"$gt": float64(1)},
	})
	if err == nil {
		t.Fatal("expected error for invalid dot-notation key")
	}
	assertContains(t, err.Error(), "invalid filter key")
}

func TestBuildFilter_UnsupportedOperator(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"id", "data", "created_at"},
		nil)

	_, err := DocFind(db, "users", map[string]interface{}{
		"age": map[string]interface{}{"$bogus": float64(1)},
	})
	if err == nil {
		t.Fatal("expected error for unsupported operator")
	}
	assertContains(t, err.Error(), "unsupported filter operator")
}

func TestBuildFilter_OperatorsInDocDelete(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)

	_, err := DocDelete(db, "users", map[string]interface{}{
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

	_, err := DocCount(db, "users", map[string]interface{}{
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

	results, err := DocAggregate(db, "sales", pipeline)
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

	_, err := DocAggregate(db, "users", pipeline)
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

	_, err := DocAggregate(db, "orders", pipeline)
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

	_, err := DocAggregate(db, "orders", pipeline)
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

	results, err := DocAggregate(db, "employees", pipeline)
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

	results, err := DocAggregate(db, "offices", pipeline)
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

	results, err := DocAggregate(db, "products", pipeline)
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

	_, err := DocAggregate(db, "employees", pipeline)
	if err == nil {
		t.Fatal("expected error for invalid $addToSet field reference")
	}
	assertContains(t, err.Error(), "$addToSet")
}
