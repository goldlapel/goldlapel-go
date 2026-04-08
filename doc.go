package goldlapel

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// DocFindOption configures a DocFind call.
type DocFindOption func(*docFindOptions)

type docFindOptions struct {
	sort  map[string]int // key -> 1 (ASC) or -1 (DESC)
	limit int
	skip  int
}

// DocSort sets the sort order for DocFind. Keys are JSONB field names,
// values are 1 for ascending or -1 for descending. Like MongoDB's sort.
func DocSort(fields map[string]int) DocFindOption {
	return func(o *docFindOptions) { o.sort = fields }
}

// DocLimit sets the maximum number of documents to return. Like MongoDB's limit.
func DocLimit(n int) DocFindOption {
	return func(o *docFindOptions) { o.limit = n }
}

// DocSkip sets the number of documents to skip before returning results.
// Like MongoDB's skip.
func DocSkip(n int) DocFindOption {
	return func(o *docFindOptions) { o.skip = n }
}

// ensureCollection creates the document store table if it doesn't exist.
// Schema: id BIGSERIAL PRIMARY KEY, data JSONB NOT NULL, created_at TIMESTAMPTZ.
func ensureCollection(db *sql.DB, collection string) error {
	if err := validateIdentifier(collection); err != nil {
		return err
	}
	_, err := db.Exec(
		"CREATE TABLE IF NOT EXISTS " + collection + " (" +
			"id BIGSERIAL PRIMARY KEY, " +
			"data JSONB NOT NULL, " +
			"created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())")
	if err != nil {
		return fmt.Errorf("create collection %s: %w", collection, err)
	}
	return nil
}

// DocInsert inserts a single document into a collection. Like MongoDB's insertOne().
// Creates the collection table if it doesn't exist. Returns the inserted document
// with id and created_at fields added.
func DocInsert(db *sql.DB, collection string, document interface{}) (map[string]interface{}, error) {
	if err := ensureCollection(db, collection); err != nil {
		return nil, err
	}

	data, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("marshal document: %w", err)
	}

	var id int64
	var createdAt string
	var rawData string
	err = db.QueryRow(
		"INSERT INTO "+collection+" (data) VALUES ($1::jsonb) RETURNING id, data, created_at",
		string(data)).Scan(&id, &rawData, &createdAt)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(rawData), &result); err != nil {
		return nil, fmt.Errorf("unmarshal result: %w", err)
	}
	result["_id"] = id
	result["_created_at"] = createdAt
	return result, nil
}

// DocInsertMany inserts multiple documents into a collection. Like MongoDB's insertMany().
// Creates the collection table if it doesn't exist. Returns inserted documents
// with id and created_at fields added.
func DocInsertMany(db *sql.DB, collection string, documents []interface{}) ([]map[string]interface{}, error) {
	if len(documents) == 0 {
		return nil, nil
	}

	if err := ensureCollection(db, collection); err != nil {
		return nil, err
	}

	// Build a batch INSERT with multiple VALUES rows
	placeholders := make([]string, len(documents))
	args := make([]interface{}, len(documents))
	for i, doc := range documents {
		data, err := json.Marshal(doc)
		if err != nil {
			return nil, fmt.Errorf("marshal document %d: %w", i, err)
		}
		placeholders[i] = fmt.Sprintf("($%d::jsonb)", i+1)
		args[i] = string(data)
	}

	q := "INSERT INTO " + collection + " (data) VALUES " +
		strings.Join(placeholders, ", ") +
		" RETURNING id, data, created_at"

	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var id int64
		var rawData string
		var createdAt string
		if err := rows.Scan(&id, &rawData, &createdAt); err != nil {
			return nil, err
		}
		var doc map[string]interface{}
		if err := json.Unmarshal([]byte(rawData), &doc); err != nil {
			return nil, fmt.Errorf("unmarshal result: %w", err)
		}
		doc["_id"] = id
		doc["_created_at"] = createdAt
		results = append(results, doc)
	}
	return results, rows.Err()
}

// DocFind queries documents from a collection. Like MongoDB's find().
// If filter is nil, returns all documents. Otherwise uses JSONB containment (@>)
// to match. Supports DocSort, DocLimit, and DocSkip options.
func DocFind(db *sql.DB, collection string, filter interface{}, opts ...DocFindOption) ([]map[string]interface{}, error) {
	if err := validateIdentifier(collection); err != nil {
		return nil, err
	}

	o := &docFindOptions{limit: 100}
	for _, fn := range opts {
		fn(o)
	}

	var q string
	var args []interface{}
	paramIdx := 1

	q = "SELECT id, data, created_at FROM " + collection

	if filter != nil {
		filterJSON, err := json.Marshal(filter)
		if err != nil {
			return nil, fmt.Errorf("marshal filter: %w", err)
		}
		filterStr := string(filterJSON)
		if filterStr != "{}" && filterStr != "null" {
			q += fmt.Sprintf(" WHERE data @> $%d::jsonb", paramIdx)
			args = append(args, filterStr)
			paramIdx++
		}
	}

	// ORDER BY
	if len(o.sort) > 0 {
		orderParts, err := buildSortClause(o.sort)
		if err != nil {
			return nil, err
		}
		q += " ORDER BY " + strings.Join(orderParts, ", ")
	} else {
		q += " ORDER BY id"
	}

	// LIMIT
	q += fmt.Sprintf(" LIMIT $%d", paramIdx)
	args = append(args, o.limit)
	paramIdx++

	// OFFSET
	if o.skip > 0 {
		q += fmt.Sprintf(" OFFSET $%d", paramIdx)
		args = append(args, o.skip)
	}

	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanDocRows(rows)
}

// DocFindOne queries a single document from a collection. Like MongoDB's findOne().
// If filter is nil, returns the first document. Otherwise uses JSONB containment (@>).
// Returns nil (not an error) if no document matches.
func DocFindOne(db *sql.DB, collection string, filter interface{}) (map[string]interface{}, error) {
	if err := validateIdentifier(collection); err != nil {
		return nil, err
	}

	var q string
	var args []interface{}

	q = "SELECT id, data, created_at FROM " + collection

	if filter != nil {
		filterJSON, err := json.Marshal(filter)
		if err != nil {
			return nil, fmt.Errorf("marshal filter: %w", err)
		}
		filterStr := string(filterJSON)
		if filterStr != "{}" && filterStr != "null" {
			q += " WHERE data @> $1::jsonb"
			args = append(args, filterStr)
		}
	}

	q += " ORDER BY id LIMIT 1"

	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results, err := scanDocRows(rows)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	return results[0], nil
}

// DocUpdate updates all documents matching a filter. Like MongoDB's updateMany().
// Uses JSONB containment (@>) for matching and || for merging the update.
// Returns the number of rows affected.
func DocUpdate(db *sql.DB, collection string, filter, update interface{}) (int64, error) {
	if err := validateIdentifier(collection); err != nil {
		return 0, err
	}

	filterJSON, err := json.Marshal(filter)
	if err != nil {
		return 0, fmt.Errorf("marshal filter: %w", err)
	}
	updateJSON, err := json.Marshal(update)
	if err != nil {
		return 0, fmt.Errorf("marshal update: %w", err)
	}

	q := "UPDATE " + collection + " SET data = data || $1::jsonb WHERE data @> $2::jsonb"
	result, err := db.Exec(q, string(updateJSON), string(filterJSON))
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// DocUpdateOne updates a single document matching a filter. Like MongoDB's updateOne().
// Uses a CTE with LIMIT 1 to ensure only one row is updated.
// Returns the number of rows affected (0 or 1).
func DocUpdateOne(db *sql.DB, collection string, filter, update interface{}) (int64, error) {
	if err := validateIdentifier(collection); err != nil {
		return 0, err
	}

	filterJSON, err := json.Marshal(filter)
	if err != nil {
		return 0, fmt.Errorf("marshal filter: %w", err)
	}
	updateJSON, err := json.Marshal(update)
	if err != nil {
		return 0, fmt.Errorf("marshal update: %w", err)
	}

	q := "WITH target AS (" +
		"SELECT id FROM " + collection + " WHERE data @> $1::jsonb ORDER BY id LIMIT 1" +
		") UPDATE " + collection + " SET data = data || $2::jsonb " +
		"FROM target WHERE " + collection + ".id = target.id"
	result, err := db.Exec(q, string(filterJSON), string(updateJSON))
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// DocDelete deletes all documents matching a filter. Like MongoDB's deleteMany().
// Uses JSONB containment (@>) for matching. Returns the number of rows deleted.
func DocDelete(db *sql.DB, collection string, filter interface{}) (int64, error) {
	if err := validateIdentifier(collection); err != nil {
		return 0, err
	}

	filterJSON, err := json.Marshal(filter)
	if err != nil {
		return 0, fmt.Errorf("marshal filter: %w", err)
	}

	q := "DELETE FROM " + collection + " WHERE data @> $1::jsonb"
	result, err := db.Exec(q, string(filterJSON))
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// DocDeleteOne deletes a single document matching a filter. Like MongoDB's deleteOne().
// Uses a CTE with LIMIT 1 to ensure only one row is deleted.
// Returns the number of rows deleted (0 or 1).
func DocDeleteOne(db *sql.DB, collection string, filter interface{}) (int64, error) {
	if err := validateIdentifier(collection); err != nil {
		return 0, err
	}

	filterJSON, err := json.Marshal(filter)
	if err != nil {
		return 0, fmt.Errorf("marshal filter: %w", err)
	}

	q := "WITH target AS (" +
		"SELECT id FROM " + collection + " WHERE data @> $1::jsonb ORDER BY id LIMIT 1" +
		") DELETE FROM " + collection + " USING target WHERE " + collection + ".id = target.id"
	result, err := db.Exec(q, string(filterJSON))
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// DocCount counts documents matching a filter. Like MongoDB's countDocuments().
// If filter is nil, counts all documents in the collection.
func DocCount(db *sql.DB, collection string, filter interface{}) (int64, error) {
	if err := validateIdentifier(collection); err != nil {
		return 0, err
	}

	var q string
	var args []interface{}

	q = "SELECT COUNT(*) FROM " + collection

	if filter != nil {
		filterJSON, err := json.Marshal(filter)
		if err != nil {
			return 0, fmt.Errorf("marshal filter: %w", err)
		}
		filterStr := string(filterJSON)
		if filterStr != "{}" && filterStr != "null" {
			q += " WHERE data @> $1::jsonb"
			args = append(args, filterStr)
		}
	}

	var count int64
	err := db.QueryRow(q, args...).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// DocCreateIndex creates a GIN index on one or more JSONB keys in a collection.
// Like MongoDB's createIndex(). Uses jsonb_path_ops for efficient containment queries.
// With a single key, creates a functional index on (data->'key').
// With no keys, creates a full GIN index on the entire data column.
func DocCreateIndex(db *sql.DB, collection string, keys ...string) error {
	if err := validateIdentifier(collection); err != nil {
		return err
	}

	for _, key := range keys {
		if err := validateIdentifier(key); err != nil {
			return fmt.Errorf("invalid index key: %w", err)
		}
	}

	var indexExpr, indexName string
	if len(keys) == 0 {
		// Full GIN index on data column
		indexExpr = "(data jsonb_path_ops)"
		indexName = collection + "_data_gin"
	} else {
		// Index on specific keys
		parts := make([]string, len(keys))
		nameParts := make([]string, len(keys))
		for i, key := range keys {
			parts[i] = fmt.Sprintf("(data->'%s')", key)
			nameParts[i] = key
		}
		indexExpr = "(" + strings.Join(parts, ", ") + ")"
		indexName = collection + "_" + strings.Join(nameParts, "_") + "_idx"
	}

	q := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s USING GIN %s",
		indexName, collection, indexExpr)
	_, err := db.Exec(q)
	return err
}

// scanDocRows reads document rows (id, data, created_at) and returns them
// as maps with _id and _created_at merged into the JSONB data.
func scanDocRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	for rows.Next() {
		var id int64
		var rawData string
		var createdAt string
		if err := rows.Scan(&id, &rawData, &createdAt); err != nil {
			return nil, err
		}
		var doc map[string]interface{}
		if err := json.Unmarshal([]byte(rawData), &doc); err != nil {
			return nil, fmt.Errorf("unmarshal document: %w", err)
		}
		doc["_id"] = id
		doc["_created_at"] = createdAt
		results = append(results, doc)
	}
	return results, rows.Err()
}

// buildSortClause generates ORDER BY expressions from a sort map.
// Keys are JSONB field names, values are 1 (ASC) or -1 (DESC).
// Sort keys are validated as identifiers and applied in deterministic
// (alphabetical) order.
func buildSortClause(sortMap map[string]int) ([]string, error) {
	keys := make([]string, 0, len(sortMap))
	for k := range sortMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, len(keys))
	for i, key := range keys {
		if err := validateIdentifier(key); err != nil {
			return nil, fmt.Errorf("invalid sort key: %w", err)
		}
		dir := "ASC"
		if sortMap[key] < 0 {
			dir = "DESC"
		}
		parts[i] = fmt.Sprintf("data->>'%s' %s", key, dir)
	}
	return parts, nil
}
