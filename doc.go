package goldlapel

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type docFindOptions struct {
	sort  map[string]int // key -> 1 (ASC) or -1 (DESC)
	limit int
	skip  int
}

// DocSort sets the sort order for DocFind. Keys are JSONB field names,
// values are 1 for ascending or -1 for descending. Like MongoDB's sort.
func DocSort(fields map[string]int) Option {
	return docOnly(func(o *docFindOptions) { o.sort = fields })
}

// DocLimit sets the maximum number of documents to return. Like MongoDB's limit.
func DocLimit(n int) Option {
	return docOnly(func(o *docFindOptions) { o.limit = n })
}

// DocSkip sets the number of documents to skip before returning results.
// Like MongoDB's skip.
func DocSkip(n int) Option {
	return docOnly(func(o *docFindOptions) { o.skip = n })
}

// ensureCollection creates the document store table if it doesn't exist.
// Schema matches the 6-wrapper consensus (Python, JS, Ruby, Java, PHP, .NET):
// _id UUID PRIMARY KEY DEFAULT gen_random_uuid(), data JSONB NOT NULL,
// created_at TIMESTAMPTZ DEFAULT NOW().
func ensureCollection(ctx context.Context, q execQuerier, collection string) error {
	return ensureCollectionOpts(ctx, q, collection, false)
}

func ensureCollectionOpts(ctx context.Context, q execQuerier, collection string, unlogged bool) error {
	if err := validateIdentifier(collection); err != nil {
		return err
	}
	prefix := "CREATE TABLE"
	if unlogged {
		prefix = "CREATE UNLOGGED TABLE"
	}
	_, err := q.ExecContext(ctx,
		prefix+" IF NOT EXISTS "+collection+" ("+
			"_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), "+
			"data JSONB NOT NULL, "+
			"created_at TIMESTAMPTZ DEFAULT NOW())")
	if err != nil {
		return fmt.Errorf("create collection %s: %w", collection, err)
	}
	return nil
}

// DocCreateCollection explicitly creates a collection table. Like MongoDB's
// createCollection(). Pass unlogged=true for an UNLOGGED table (higher
// throughput, not crash-safe; ideal for ephemeral data).
//
// Collections are also auto-created on first DocInsert/DocInsertMany, so
// calling DocCreateCollection is only strictly required when you want to
// control the unlogged flag or pre-create without inserting.
func DocCreateCollection(ctx context.Context, q execQuerier, collection string, unlogged bool) error {
	return ensureCollectionOpts(ctx, q, collection, unlogged)
}

// DocInsert inserts a single document into a collection. Like MongoDB's insertOne().
// Creates the collection table if it doesn't exist. Returns a map with three
// keys — "_id" (UUID string), "data" (the document), and "created_at"
// (RFC 3339 string) — matching the cross-wrapper shape so a collection written
// by any wrapper round-trips through any other.
func DocInsert(ctx context.Context, q execQuerier, collection string, document interface{}) (map[string]interface{}, error) {
	if err := ensureCollection(ctx, q, collection); err != nil {
		return nil, err
	}

	data, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("marshal document: %w", err)
	}

	var id string
	var createdAt string
	var rawData string
	err = q.QueryRowContext(ctx,
		"INSERT INTO "+collection+" (data) VALUES ($1::jsonb) RETURNING _id, data, created_at",
		string(data)).Scan(&id, &rawData, &createdAt)
	if err != nil {
		return nil, err
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(rawData), &parsed); err != nil {
		return nil, fmt.Errorf("unmarshal result: %w", err)
	}
	return map[string]interface{}{
		"_id":        id,
		"data":       parsed,
		"created_at": createdAt,
	}, nil
}

// DocInsertMany inserts multiple documents into a collection. Like MongoDB's insertMany().
func DocInsertMany(ctx context.Context, q execQuerier, collection string, documents []interface{}) ([]map[string]interface{}, error) {
	if len(documents) == 0 {
		return nil, nil
	}

	if err := ensureCollection(ctx, q, collection); err != nil {
		return nil, err
	}

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

	query := "INSERT INTO " + collection + " (data) VALUES " +
		strings.Join(placeholders, ", ") +
		" RETURNING _id, data, created_at"

	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanDocRows(rows)
}

// DocFind queries documents from a collection. Like MongoDB's find().
func DocFind(ctx context.Context, q execQuerier, collection string, filter interface{}, opts ...Option) ([]map[string]interface{}, error) {
	if err := validateIdentifier(collection); err != nil {
		return nil, err
	}

	o := &docFindOptions{limit: 100}
	for _, opt := range opts {
		opt.applyDoc(o)
	}

	query := "SELECT _id, data, created_at FROM " + collection
	paramIdx := 1
	var args []interface{}

	whereClause, filterParams, nextParam, err := buildFilter(filter, paramIdx)
	if err != nil {
		return nil, err
	}
	if whereClause != "" {
		query += " WHERE " + whereClause
		args = append(args, filterParams...)
		paramIdx = nextParam
	}

	if len(o.sort) > 0 {
		orderParts, err := buildSortClause(o.sort)
		if err != nil {
			return nil, err
		}
		query += " ORDER BY " + strings.Join(orderParts, ", ")
	}

	query += fmt.Sprintf(" LIMIT $%d", paramIdx)
	args = append(args, o.limit)
	paramIdx++

	if o.skip > 0 {
		query += fmt.Sprintf(" OFFSET $%d", paramIdx)
		args = append(args, o.skip)
	}

	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanDocRows(rows)
}

// DocFindOne queries a single document from a collection. Like MongoDB's findOne().
func DocFindOne(ctx context.Context, q execQuerier, collection string, filter interface{}) (map[string]interface{}, error) {
	if err := validateIdentifier(collection); err != nil {
		return nil, err
	}

	query := "SELECT _id, data, created_at FROM " + collection
	var args []interface{}

	whereClause, filterParams, _, err := buildFilter(filter, 1)
	if err != nil {
		return nil, err
	}
	if whereClause != "" {
		query += " WHERE " + whereClause
		args = append(args, filterParams...)
	}

	query += " LIMIT 1"

	rows, err := q.QueryContext(ctx, query, args...)
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
func DocUpdate(ctx context.Context, q execQuerier, collection string, filter, update interface{}) (int64, error) {
	if err := validateIdentifier(collection); err != nil {
		return 0, err
	}

	updateJSON, err := json.Marshal(update)
	if err != nil {
		return 0, fmt.Errorf("marshal update: %w", err)
	}

	query := "UPDATE " + collection + " SET data = data || $1::jsonb"
	args := []interface{}{string(updateJSON)}
	paramIdx := 2

	whereClause, filterParams, _, err := buildFilter(filter, paramIdx)
	if err != nil {
		return 0, err
	}
	if whereClause != "" {
		query += " WHERE " + whereClause
		args = append(args, filterParams...)
	}

	result, err := q.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// DocUpdateOne updates a single document matching a filter. Like MongoDB's updateOne().
func DocUpdateOne(ctx context.Context, q execQuerier, collection string, filter, update interface{}) (int64, error) {
	if err := validateIdentifier(collection); err != nil {
		return 0, err
	}

	updateJSON, err := json.Marshal(update)
	if err != nil {
		return 0, fmt.Errorf("marshal update: %w", err)
	}

	whereClause, filterParams, nextParam, err := buildFilter(filter, 1)
	if err != nil {
		return 0, err
	}

	cteWhere := ""
	if whereClause != "" {
		cteWhere = " WHERE " + whereClause
	}

	query := "WITH target AS (" +
		"SELECT _id FROM " + collection + cteWhere + " LIMIT 1" +
		") UPDATE " + collection + " SET data = data || $" + fmt.Sprintf("%d", nextParam) + "::jsonb " +
		"FROM target WHERE " + collection + "._id = target._id"

	args := append(filterParams, string(updateJSON))
	result, err := q.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// DocDelete deletes all documents matching a filter. Like MongoDB's deleteMany().
func DocDelete(ctx context.Context, q execQuerier, collection string, filter interface{}) (int64, error) {
	if err := validateIdentifier(collection); err != nil {
		return 0, err
	}

	query := "DELETE FROM " + collection
	var args []interface{}

	whereClause, filterParams, _, err := buildFilter(filter, 1)
	if err != nil {
		return 0, err
	}
	if whereClause != "" {
		query += " WHERE " + whereClause
		args = append(args, filterParams...)
	}

	result, err := q.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// DocDeleteOne deletes a single document matching a filter. Like MongoDB's deleteOne().
func DocDeleteOne(ctx context.Context, q execQuerier, collection string, filter interface{}) (int64, error) {
	if err := validateIdentifier(collection); err != nil {
		return 0, err
	}

	whereClause, filterParams, _, err := buildFilter(filter, 1)
	if err != nil {
		return 0, err
	}

	cteWhere := ""
	if whereClause != "" {
		cteWhere = " WHERE " + whereClause
	}

	query := "WITH target AS (" +
		"SELECT _id FROM " + collection + cteWhere + " LIMIT 1" +
		") DELETE FROM " + collection + " USING target WHERE " + collection + "._id = target._id"
	result, err := q.ExecContext(ctx, query, filterParams...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// DocFindOneAndUpdate atomically finds a single document matching filter and
// applies update (merged into data via JSONB concatenation), returning the
// updated document. Like MongoDB's findOneAndUpdate(). Returns nil if no
// document matches.
func DocFindOneAndUpdate(ctx context.Context, q execQuerier, collection string, filter, update interface{}) (map[string]interface{}, error) {
	if err := validateIdentifier(collection); err != nil {
		return nil, err
	}

	updateJSON, err := json.Marshal(update)
	if err != nil {
		return nil, fmt.Errorf("marshal update: %w", err)
	}

	whereClause, filterParams, nextParam, err := buildFilter(filter, 1)
	if err != nil {
		return nil, err
	}

	cteWhere := ""
	if whereClause != "" {
		cteWhere = " WHERE " + whereClause
	}

	query := "WITH target AS (" +
		"SELECT _id FROM " + collection + cteWhere + " LIMIT 1" +
		") UPDATE " + collection + " SET data = data || $" + fmt.Sprintf("%d", nextParam) + "::jsonb " +
		"FROM target WHERE " + collection + "._id = target._id " +
		"RETURNING " + collection + "._id, " + collection + ".data, " + collection + ".created_at"

	args := append(append([]interface{}{}, filterParams...), string(updateJSON))
	rows, err := q.QueryContext(ctx, query, args...)
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

// DocFindOneAndDelete atomically finds a single document matching filter,
// deletes it, and returns the deleted document. Like MongoDB's findOneAndDelete().
// Returns nil if no document matches.
func DocFindOneAndDelete(ctx context.Context, q execQuerier, collection string, filter interface{}) (map[string]interface{}, error) {
	if err := validateIdentifier(collection); err != nil {
		return nil, err
	}

	whereClause, filterParams, _, err := buildFilter(filter, 1)
	if err != nil {
		return nil, err
	}

	cteWhere := ""
	if whereClause != "" {
		cteWhere = " WHERE " + whereClause
	}

	query := "WITH target AS (" +
		"SELECT _id FROM " + collection + cteWhere + " LIMIT 1" +
		") DELETE FROM " + collection + " USING target WHERE " + collection + "._id = target._id " +
		"RETURNING " + collection + "._id, " + collection + ".data, " + collection + ".created_at"

	rows, err := q.QueryContext(ctx, query, filterParams...)
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

// DocDistinct returns the distinct values of the given JSONB field across
// documents matching filter. Like MongoDB's distinct().
func DocDistinct(ctx context.Context, q execQuerier, collection, field string, filter interface{}) ([]interface{}, error) {
	if err := validateIdentifier(collection); err != nil {
		return nil, err
	}
	fieldExpr, err := fieldPath(field)
	if err != nil {
		return nil, err
	}

	query := "SELECT DISTINCT " + fieldExpr + " FROM " + collection
	var args []interface{}

	whereParts := []string{fieldExpr + " IS NOT NULL"}

	whereClause, filterParams, _, err := buildFilter(filter, 1)
	if err != nil {
		return nil, err
	}
	if whereClause != "" {
		whereParts = append(whereParts, whereClause)
		args = append(args, filterParams...)
	}

	query += " WHERE " + strings.Join(whereParts, " AND ")

	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []interface{}
	for rows.Next() {
		var val interface{}
		if err := rows.Scan(&val); err != nil {
			return nil, err
		}
		if b, ok := val.([]byte); ok {
			val = string(b)
		}
		results = append(results, val)
	}
	return results, rows.Err()
}

// DocFindCursor streams documents matching filter to a callback, fetching in
// batches (default 100). Like MongoDB's find().cursor() — useful for large
// result sets where materializing the full list would be wasteful.
//
// The callback returns (continue bool, err error). Returning false halts
// iteration cleanly; returning an error aborts iteration and propagates.
// Accepts the same DocSort/DocLimit/DocSkip options as DocFind.
func DocFindCursor(ctx context.Context, q execQuerier, collection string, filter interface{}, callback func(doc map[string]interface{}) (bool, error), opts ...Option) error {
	if err := validateIdentifier(collection); err != nil {
		return err
	}
	if callback == nil {
		return fmt.Errorf("callback must not be nil")
	}

	o := &docFindOptions{limit: 0}
	for _, opt := range opts {
		opt.applyDoc(o)
	}

	query := "SELECT _id, data, created_at FROM " + collection
	paramIdx := 1
	var args []interface{}

	whereClause, filterParams, nextParam, err := buildFilter(filter, paramIdx)
	if err != nil {
		return err
	}
	if whereClause != "" {
		query += " WHERE " + whereClause
		args = append(args, filterParams...)
		paramIdx = nextParam
	}

	if len(o.sort) > 0 {
		orderParts, err := buildSortClause(o.sort)
		if err != nil {
			return err
		}
		query += " ORDER BY " + strings.Join(orderParts, ", ")
	}

	if o.limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", paramIdx)
		args = append(args, o.limit)
		paramIdx++
	}

	if o.skip > 0 {
		query += fmt.Sprintf(" OFFSET $%d", paramIdx)
		args = append(args, o.skip)
	}

	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		doc, err := scanDocRow(rows)
		if err != nil {
			return err
		}
		cont, cbErr := callback(doc)
		if cbErr != nil {
			return cbErr
		}
		if !cont {
			return nil
		}
	}
	return rows.Err()
}

// DocCount counts documents matching a filter. Like MongoDB's countDocuments().
func DocCount(ctx context.Context, q execQuerier, collection string, filter interface{}) (int64, error) {
	if err := validateIdentifier(collection); err != nil {
		return 0, err
	}

	query := "SELECT COUNT(*) FROM " + collection
	var args []interface{}

	whereClause, filterParams, _, err := buildFilter(filter, 1)
	if err != nil {
		return 0, err
	}
	if whereClause != "" {
		query += " WHERE " + whereClause
		args = append(args, filterParams...)
	}

	var count int64
	err = q.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// DocCreateIndex creates a GIN index on one or more JSONB keys in a collection.
// Pass keys as a slice so that functional options (e.g. WithTx) remain
// available as trailing variadic parameters.
func DocCreateIndex(ctx context.Context, q execQuerier, collection string, keys []string) error {
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
		indexExpr = "(data jsonb_path_ops)"
		indexName = collection + "_data_gin"
	} else {
		parts := make([]string, len(keys))
		nameParts := make([]string, len(keys))
		for i, key := range keys {
			parts[i] = fmt.Sprintf("(data->'%s')", key)
			nameParts[i] = key
		}
		indexExpr = "(" + strings.Join(parts, ", ") + ")"
		indexName = collection + "_" + strings.Join(nameParts, "_") + "_idx"
	}

	query := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s USING GIN %s",
		indexName, collection, indexExpr)
	_, err := q.ExecContext(ctx, query)
	return err
}

// scanDocRow reads a single (_id, data, created_at) row into the cross-wrapper
// document shape: {"_id": uuid-string, "data": parsed-json, "created_at": ts}.
// Assumes rows.Next() has already been called.
func scanDocRow(rows *sql.Rows) (map[string]interface{}, error) {
	var id string
	var rawData string
	var createdAt string
	if err := rows.Scan(&id, &rawData, &createdAt); err != nil {
		return nil, err
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(rawData), &parsed); err != nil {
		return nil, fmt.Errorf("unmarshal document: %w", err)
	}
	return map[string]interface{}{
		"_id":        id,
		"data":       parsed,
		"created_at": createdAt,
	}, nil
}

// scanDocRows reads document rows (_id, data, created_at) and returns them
// as maps with the three top-level keys _id, data, created_at — matching the
// Python/JS/Ruby/Java/PHP/.NET wrapper return shape so a collection written
// by any wrapper round-trips through any other.
func scanDocRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	for rows.Next() {
		doc, err := scanDocRow(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, doc)
	}
	return results, rows.Err()
}

// DocAggregate runs a MongoDB-style aggregation pipeline against a collection.
func DocAggregate(ctx context.Context, q execQuerier, collection string, pipeline []map[string]interface{}) ([]map[string]interface{}, error) {
	if err := validateIdentifier(collection); err != nil {
		return nil, err
	}
	if len(pipeline) == 0 {
		return nil, nil
	}

	var (
		matchFilter  interface{}
		groupStage   map[string]interface{}
		sortStage    map[string]interface{}
		projectStage map[string]interface{}
		unwindField  string
		lookupStage  map[string]interface{}
		limitVal     int
		skipVal      int
		hasGroup     bool
		hasLimit     bool
		hasSkip      bool
		hasProject   bool
		hasUnwind    bool
		hasLookup    bool
	)

	for _, stage := range pipeline {
		if len(stage) != 1 {
			return nil, fmt.Errorf("each pipeline stage must have exactly one key")
		}
		for key, val := range stage {
			switch key {
			case "$match":
				matchFilter = val
			case "$group":
				gm, ok := val.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("$group stage must be a map")
				}
				groupStage = gm
				hasGroup = true
			case "$sort":
				sm, ok := val.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("$sort stage must be a map")
				}
				sortStage = sm
			case "$limit":
				n, ok := numToInt(val)
				if !ok {
					return nil, fmt.Errorf("$limit must be a number")
				}
				limitVal = n
				hasLimit = true
			case "$skip":
				n, ok := numToInt(val)
				if !ok {
					return nil, fmt.Errorf("$skip must be a number")
				}
				skipVal = n
				hasSkip = true
			case "$project":
				pm, ok := val.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("$project stage must be a map")
				}
				projectStage = pm
				hasProject = true
			case "$unwind":
				switch v := val.(type) {
				case string:
					if len(v) == 0 || v[0] != '$' {
						return nil, fmt.Errorf("$unwind string must start with $")
					}
					unwindField = v[1:]
				case map[string]interface{}:
					pathVal, ok := v["path"]
					if !ok {
						return nil, fmt.Errorf("$unwind map requires a 'path' key")
					}
					pathStr, ok := pathVal.(string)
					if !ok || len(pathStr) == 0 || pathStr[0] != '$' {
						return nil, fmt.Errorf("$unwind path must be a $field reference")
					}
					unwindField = pathStr[1:]
				default:
					return nil, fmt.Errorf("$unwind must be a string or map")
				}
				if err := validateIdentifier(unwindField); err != nil {
					return nil, fmt.Errorf("$unwind: invalid field: %w", err)
				}
				hasUnwind = true
			case "$lookup":
				lm, ok := val.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("$lookup stage must be a map")
				}
				for _, reqKey := range []string{"from", "localField", "foreignField", "as"} {
					if _, exists := lm[reqKey]; !exists {
						return nil, fmt.Errorf("$lookup requires '%s' field", reqKey)
					}
				}
				lookupStage = lm
				hasLookup = true
			default:
				return nil, fmt.Errorf("unsupported pipeline stage: %s", key)
			}
		}
	}

	var args []interface{}
	paramIdx := 1

	var selectParts []string
	var groupByParts []string

	if hasProject {
		projKeys := make([]string, 0, len(projectStage))
		for k := range projectStage {
			projKeys = append(projKeys, k)
		}
		sort.Strings(projKeys)

		excludeID := false
		for _, key := range projKeys {
			val := projectStage[key]
			if key == "_id" {
				if n, ok := val.(float64); ok && n == 0 {
					excludeID = true
					continue
				}
			}
			if n, ok := val.(float64); ok && n == 1 {
				if err := validateIdentifier(key); err != nil {
					return nil, fmt.Errorf("$project: invalid field %q: %w", key, err)
				}
				fp, err := fieldPath(key)
				if err != nil {
					return nil, fmt.Errorf("$project: %w", err)
				}
				selectParts = append(selectParts, fmt.Sprintf("%s AS %s", fp, key))
			} else if s, ok := val.(string); ok && len(s) > 0 && s[0] == '$' {
				srcField := s[1:]
				if err := validateIdentifier(key); err != nil {
					return nil, fmt.Errorf("$project: invalid alias %q: %w", key, err)
				}
				fp, err := fieldPath(srcField)
				if err != nil {
					return nil, fmt.Errorf("$project: %w", err)
				}
				selectParts = append(selectParts, fmt.Sprintf("%s AS %s", fp, key))
			} else {
				return nil, fmt.Errorf("$project: unsupported value for key %q: %v", key, val)
			}
		}
		if !excludeID {
			selectParts = append([]string{"_id"}, selectParts...)
		}
	} else if hasGroup {
		idVal, hasID := groupStage["_id"]
		if !hasID {
			return nil, fmt.Errorf("$group stage requires an _id field")
		}

		if idVal == nil {
			selectParts = append(selectParts, "NULL AS _id")
		} else if idStr, ok := idVal.(string); ok && len(idStr) > 0 && idStr[0] == '$' {
			field := idStr[1:]
			if err := validateIdentifier(field); err != nil {
				return nil, fmt.Errorf("invalid $group _id field: %w", err)
			}
			if hasUnwind && field == unwindField {
				selectParts = append(selectParts, fmt.Sprintf("%s AS _id", unwindField))
				groupByParts = append(groupByParts, unwindField)
			} else {
				selectParts = append(selectParts, fmt.Sprintf("data->>'%s' AS _id", field))
				groupByParts = append(groupByParts, fmt.Sprintf("data->>'%s'", field))
			}
		} else if idMap, ok := idVal.(map[string]interface{}); ok {
			if len(idMap) == 0 {
				return nil, fmt.Errorf("$group _id map must not be empty")
			}
			idKeys := make([]string, 0, len(idMap))
			for k := range idMap {
				idKeys = append(idKeys, k)
			}
			sort.Strings(idKeys)

			jboParts := make([]string, 0, len(idKeys)*2)
			for _, alias := range idKeys {
				if err := validateIdentifier(alias); err != nil {
					return nil, fmt.Errorf("invalid $group _id key %q: %w", alias, err)
				}
				fieldRef, err := extractField(idMap[alias])
				if err != nil {
					return nil, fmt.Errorf("$group _id key %q: %w", alias, err)
				}
				jboParts = append(jboParts, fmt.Sprintf("'%s', data->>'%s'", alias, fieldRef))
				groupByParts = append(groupByParts, fmt.Sprintf("data->>'%s'", fieldRef))
			}
			selectParts = append(selectParts, fmt.Sprintf("json_build_object(%s) AS _id", strings.Join(jboParts, ", ")))
		} else {
			return nil, fmt.Errorf("$group _id must be null, a $field reference, or a map of $field references")
		}

		accKeys := make([]string, 0, len(groupStage)-1)
		for k := range groupStage {
			if k != "_id" {
				accKeys = append(accKeys, k)
			}
		}
		sort.Strings(accKeys)

		for _, accName := range accKeys {
			if err := validateIdentifier(accName); err != nil {
				return nil, fmt.Errorf("invalid accumulator name: %w", err)
			}
			accVal := groupStage[accName]
			accMap, ok := accVal.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("accumulator %s must be a map with an operator key", accName)
			}
			expr, err := buildAccumulator(accMap)
			if err != nil {
				return nil, fmt.Errorf("accumulator %s: %w", accName, err)
			}
			selectParts = append(selectParts, fmt.Sprintf("%s AS %s", expr, accName))
		}
	} else {
		selectParts = append(selectParts, "_id", "data", "created_at")
	}

	query := "SELECT " + strings.Join(selectParts, ", ") + " FROM " + collection

	if hasUnwind {
		query += fmt.Sprintf(" CROSS JOIN jsonb_array_elements_text(data->'%s') AS %s", unwindField, unwindField)
	}

	if matchFilter != nil {
		whereClause, filterParams, nextP, err := buildFilter(matchFilter, paramIdx)
		if err != nil {
			return nil, fmt.Errorf("$match: %w", err)
		}
		if whereClause != "" {
			query += " WHERE " + whereClause
			args = append(args, filterParams...)
			paramIdx = nextP
		}
	}

	if len(groupByParts) > 0 {
		query += " GROUP BY " + strings.Join(groupByParts, ", ")
	}

	if hasLookup {
		fromColl, _ := lookupStage["from"].(string)
		localField, _ := lookupStage["localField"].(string)
		foreignField, _ := lookupStage["foreignField"].(string)
		asField, _ := lookupStage["as"].(string)

		if err := validateIdentifier(fromColl); err != nil {
			return nil, fmt.Errorf("$lookup: invalid 'from' collection: %w", err)
		}
		if err := validateIdentifier(asField); err != nil {
			return nil, fmt.Errorf("$lookup: invalid 'as' field: %w", err)
		}

		localExpr, err := fieldPath(localField)
		if err != nil {
			return nil, fmt.Errorf("$lookup: invalid localField: %w", err)
		}
		foreignExpr, err := fieldPath(foreignField)
		if err != nil {
			return nil, fmt.Errorf("$lookup: invalid foreignField: %w", err)
		}
		foreignExpr = strings.Replace(foreignExpr, "data", fromColl+".data", 1)

		subquery := fmt.Sprintf(
			"(SELECT COALESCE(json_agg(%s.data), '[]'::json) FROM %s WHERE %s = %s) AS %s",
			fromColl, fromColl, foreignExpr, localExpr, asField)

		query = strings.Replace(query, "SELECT ", "SELECT "+subquery+", ", 1)
	}

	if sortStage != nil {
		sortKeys := make([]string, 0, len(sortStage))
		for k := range sortStage {
			sortKeys = append(sortKeys, k)
		}
		sort.Strings(sortKeys)

		orderParts := make([]string, 0, len(sortKeys))
		for _, key := range sortKeys {
			dir := "ASC"
			if v, ok := numToInt(sortStage[key]); ok && v < 0 {
				dir = "DESC"
			}
			if hasGroup {
				if err := validateIdentifier(key); err != nil {
					return nil, fmt.Errorf("invalid sort key: %w", err)
				}
				orderParts = append(orderParts, fmt.Sprintf("%s %s", key, dir))
			} else {
				if err := validateIdentifier(key); err != nil {
					return nil, fmt.Errorf("invalid sort key: %w", err)
				}
				orderParts = append(orderParts, fmt.Sprintf("data->>'%s' %s", key, dir))
			}
		}
		query += " ORDER BY " + strings.Join(orderParts, ", ")
	}

	if hasLimit {
		query += fmt.Sprintf(" LIMIT $%d", paramIdx)
		args = append(args, limitVal)
		paramIdx++
	}

	if hasSkip {
		query += fmt.Sprintf(" OFFSET $%d", paramIdx)
		args = append(args, skipVal)
		paramIdx++
	}

	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if hasGroup || hasProject || hasLookup {
		return scanAggregateRows(rows)
	}
	return scanDocRows(rows)
}

// buildAccumulator translates a MongoDB accumulator map to a SQL expression.
func buildAccumulator(acc map[string]interface{}) (string, error) {
	if len(acc) != 1 {
		return "", fmt.Errorf("accumulator must have exactly one operator")
	}
	for op, val := range acc {
		switch op {
		case "$sum":
			if n, ok := val.(float64); ok && n == 1 {
				return "COUNT(*)", nil
			}
			field, err := extractField(val)
			if err != nil {
				return "", fmt.Errorf("$sum: %w", err)
			}
			return fmt.Sprintf("SUM((data->>'%s')::numeric)", field), nil
		case "$avg":
			field, err := extractField(val)
			if err != nil {
				return "", fmt.Errorf("$avg: %w", err)
			}
			return fmt.Sprintf("AVG((data->>'%s')::numeric)", field), nil
		case "$min":
			field, err := extractField(val)
			if err != nil {
				return "", fmt.Errorf("$min: %w", err)
			}
			return fmt.Sprintf("MIN((data->>'%s')::numeric)", field), nil
		case "$max":
			field, err := extractField(val)
			if err != nil {
				return "", fmt.Errorf("$max: %w", err)
			}
			return fmt.Sprintf("MAX((data->>'%s')::numeric)", field), nil
		case "$count":
			return "COUNT(*)", nil
		case "$push":
			field, err := extractField(val)
			if err != nil {
				return "", fmt.Errorf("$push: %w", err)
			}
			return fmt.Sprintf("array_agg(data->>'%s')", field), nil
		case "$addToSet":
			field, err := extractField(val)
			if err != nil {
				return "", fmt.Errorf("$addToSet: %w", err)
			}
			return fmt.Sprintf("array_agg(DISTINCT data->>'%s')", field), nil
		default:
			return "", fmt.Errorf("unsupported accumulator: %s", op)
		}
	}
	return "", fmt.Errorf("empty accumulator")
}

func extractField(val interface{}) (string, error) {
	s, ok := val.(string)
	if !ok || len(s) == 0 || s[0] != '$' {
		return "", fmt.Errorf("expected a $field reference, got %v", val)
	}
	field := s[1:]
	if err := validateIdentifier(field); err != nil {
		return "", err
	}
	return field, nil
}

func numToInt(v interface{}) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case int:
		return n, true
	case int64:
		return int(n), true
	}
	return 0, false
}

func scanAggregateRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns: %w", err)
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(columns))
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				val = string(b)
			}
			s, isStr := val.(string)
			if isStr {
				if len(s) > 0 && s[0] == '[' {
					var arr []interface{}
					if err := json.Unmarshal([]byte(s), &arr); err == nil {
						row[col] = arr
						continue
					}
				}
				if len(s) > 0 && s[0] == '{' && col == "_id" {
					var m map[string]interface{}
					if err := json.Unmarshal([]byte(s), &m); err == nil {
						row[col] = m
						continue
					}
				}
				if len(s) > 1 && s[0] == '{' && s[len(s)-1] == '}' && col != "_id" {
					row[col] = parsePgArray(s)
					continue
				}
			}
			row[col] = val
		}
		results = append(results, row)
	}
	return results, rows.Err()
}

func parsePgArray(s string) []string {
	inner := s[1 : len(s)-1]
	if inner == "" {
		return []string{}
	}
	var result []string
	var current strings.Builder
	inQuote := false
	escaped := false
	for i := 0; i < len(inner); i++ {
		ch := inner[i]
		if escaped {
			current.WriteByte(ch)
			escaped = false
			continue
		}
		if ch == '\\' && inQuote {
			escaped = true
			continue
		}
		if ch == '"' {
			inQuote = !inQuote
			continue
		}
		if ch == ',' && !inQuote {
			result = append(result, current.String())
			current.Reset()
			continue
		}
		current.WriteByte(ch)
	}
	result = append(result, current.String())
	return result
}

var comparisonOps = map[string]string{
	"$gt": ">", "$gte": ">=", "$lt": "<", "$lte": "<=",
	"$eq": "=", "$ne": "!=",
}

var supportedFilterOps = map[string]bool{
	"$gt": true, "$gte": true, "$lt": true, "$lte": true,
	"$eq": true, "$ne": true, "$in": true, "$nin": true,
	"$exists": true, "$regex": true,
	"$elemMatch": true, "$text": true,
}

func expandDotKeys(m map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{}, len(m))
	for key, value := range m {
		parts := strings.Split(key, ".")
		if len(parts) == 1 {
			result[key] = value
			continue
		}
		cur := result
		for _, seg := range parts[:len(parts)-1] {
			if existing, ok := cur[seg]; ok {
				if nested, isMap := existing.(map[string]interface{}); isMap {
					cur = nested
				} else {
					nested := make(map[string]interface{})
					cur[seg] = nested
					cur = nested
				}
			} else {
				nested := make(map[string]interface{})
				cur[seg] = nested
				cur = nested
			}
		}
		cur[parts[len(parts)-1]] = value
	}
	return result
}

func fieldPath(key string) (string, error) {
	parts := strings.Split(key, ".")
	for _, part := range parts {
		if !identifierRe.MatchString(part) {
			return "", fmt.Errorf("invalid filter key: %s", key)
		}
	}
	if len(parts) == 1 {
		return fmt.Sprintf("data->>'%s'", parts[0]), nil
	}
	expr := "data"
	for _, part := range parts[:len(parts)-1] {
		expr += fmt.Sprintf("->'%s'", part)
	}
	expr += fmt.Sprintf("->>'%s'", parts[len(parts)-1])
	return expr, nil
}

// fieldPathJson is the JSONB-typed variant of fieldPath — returns the chain of
// arrow operators that produces a JSONB value (not text). Used by operators
// that need to expand into JSONB functions like jsonb_array_elements, e.g.
// $elemMatch.
func fieldPathJson(key string) (string, error) {
	parts := strings.Split(key, ".")
	for _, part := range parts {
		if !identifierRe.MatchString(part) {
			return "", fmt.Errorf("invalid filter key: %s", key)
		}
	}
	chain := "data"
	for _, part := range parts {
		chain += fmt.Sprintf("->'%s'", part)
	}
	return chain, nil
}

func isOperatorMap(m map[string]interface{}) bool {
	for k := range m {
		if len(k) > 0 && k[0] == '$' {
			return true
		}
	}
	return false
}

func buildFilter(filter interface{}, startParam int) (string, []interface{}, int, error) {
	if filter == nil {
		return "", nil, startParam, nil
	}

	filterMap, ok := filter.(map[string]interface{})
	if !ok {
		filterJSON, err := json.Marshal(filter)
		if err != nil {
			return "", nil, startParam, fmt.Errorf("marshal filter: %w", err)
		}
		filterStr := string(filterJSON)
		if filterStr == "{}" || filterStr == "null" {
			return "", nil, startParam, nil
		}
		clause := fmt.Sprintf("data @> $%d::jsonb", startParam)
		return clause, []interface{}{filterStr}, startParam + 1, nil
	}

	if len(filterMap) == 0 {
		return "", nil, startParam, nil
	}

	containment := make(map[string]interface{})
	var operatorKeys []string
	var topLevelTextKeys []string

	keys := make([]string, 0, len(filterMap))
	for k := range filterMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		if key == "$text" {
			topLevelTextKeys = append(topLevelTextKeys, key)
			continue
		}
		value := filterMap[key]
		valMap, isMap := value.(map[string]interface{})
		if isMap && isOperatorMap(valMap) {
			operatorKeys = append(operatorKeys, key)
		} else {
			containment[key] = value
		}
	}

	var allClauses []string
	var allParams []interface{}
	paramIdx := startParam

	if len(containment) > 0 {
		containment = expandDotKeys(containment)
		cJSON, err := json.Marshal(containment)
		if err != nil {
			return "", nil, startParam, fmt.Errorf("marshal containment filter: %w", err)
		}
		allClauses = append(allClauses, fmt.Sprintf("data @> $%d::jsonb", paramIdx))
		allParams = append(allParams, string(cJSON))
		paramIdx++
	}

	for _, key := range topLevelTextKeys {
		textMap, ok := filterMap[key].(map[string]interface{})
		if !ok {
			return "", nil, startParam, fmt.Errorf("$text requires {$search: 'query'}")
		}
		search, hasSearch := textMap["$search"]
		if !hasSearch {
			return "", nil, startParam, fmt.Errorf("$text requires {$search: 'query'}")
		}
		lang := "english"
		if l, ok := textMap["$language"].(string); ok {
			lang = l
		}
		allClauses = append(allClauses, fmt.Sprintf(
			"to_tsvector($%d, data::text) @@ plainto_tsquery($%d, $%d)",
			paramIdx, paramIdx+1, paramIdx+2))
		allParams = append(allParams, lang, lang, fmt.Sprintf("%v", search))
		paramIdx += 3
	}

	for _, key := range operatorKeys {
		valMap := filterMap[key].(map[string]interface{})
		fieldExpr, err := fieldPath(key)
		if err != nil {
			return "", nil, startParam, err
		}

		opKeys := make([]string, 0, len(valMap))
		for k := range valMap {
			opKeys = append(opKeys, k)
		}
		sort.Strings(opKeys)

		for _, op := range opKeys {
			operand := valMap[op]

			if !supportedFilterOps[op] {
				return "", nil, startParam, fmt.Errorf("unsupported filter operator: %s", op)
			}

			if sqlOp, isCmp := comparisonOps[op]; isCmp {
				if isNumeric(operand) {
					allClauses = append(allClauses, fmt.Sprintf("(%s)::numeric %s $%d", fieldExpr, sqlOp, paramIdx))
				} else {
					allClauses = append(allClauses, fmt.Sprintf("%s %s $%d", fieldExpr, sqlOp, paramIdx))
				}
				allParams = append(allParams, operand)
				paramIdx++
			} else if op == "$in" {
				arr, ok := operand.([]interface{})
				if !ok {
					return "", nil, startParam, fmt.Errorf("$in requires an array")
				}
				placeholders := make([]string, len(arr))
				for i, v := range arr {
					placeholders[i] = fmt.Sprintf("$%d", paramIdx)
					allParams = append(allParams, fmt.Sprintf("%v", v))
					paramIdx++
				}
				allClauses = append(allClauses, fmt.Sprintf("%s IN (%s)", fieldExpr, strings.Join(placeholders, ", ")))
			} else if op == "$nin" {
				arr, ok := operand.([]interface{})
				if !ok {
					return "", nil, startParam, fmt.Errorf("$nin requires an array")
				}
				placeholders := make([]string, len(arr))
				for i, v := range arr {
					placeholders[i] = fmt.Sprintf("$%d", paramIdx)
					allParams = append(allParams, fmt.Sprintf("%v", v))
					paramIdx++
				}
				allClauses = append(allClauses, fmt.Sprintf("%s NOT IN (%s)", fieldExpr, strings.Join(placeholders, ", ")))
			} else if op == "$exists" {
				topKey := strings.Split(key, ".")[0]
				if b, isBool := operand.(bool); isBool && b {
					allClauses = append(allClauses, fmt.Sprintf("data ? $%d", paramIdx))
				} else {
					allClauses = append(allClauses, fmt.Sprintf("NOT (data ? $%d)", paramIdx))
				}
				allParams = append(allParams, topKey)
				paramIdx++
			} else if op == "$regex" {
				allClauses = append(allClauses, fmt.Sprintf("%s ~ $%d", fieldExpr, paramIdx))
				allParams = append(allParams, operand)
				paramIdx++
			} else if op == "$elemMatch" {
				subMap, ok := operand.(map[string]interface{})
				if !ok {
					return "", nil, startParam, fmt.Errorf("$elemMatch value must be an object")
				}
				fj, err := fieldPathJson(key)
				if err != nil {
					return "", nil, startParam, err
				}
				// Sort sub-op keys for deterministic SQL output (matches Python's
				// dict insertion order + tests; Go maps have random iteration).
				subOps := make([]string, 0, len(subMap))
				for k := range subMap {
					subOps = append(subOps, k)
				}
				sort.Strings(subOps)

				var elemClauses []string
				for _, subOp := range subOps {
					subVal := subMap[subOp]
					if sqlOp, isCmp := comparisonOps[subOp]; isCmp {
						if isNumeric(subVal) {
							elemClauses = append(elemClauses,
								fmt.Sprintf("(elem#>>'{}')::numeric %s $%d", sqlOp, paramIdx))
						} else {
							elemClauses = append(elemClauses,
								fmt.Sprintf("elem#>>'{}' %s $%d", sqlOp, paramIdx))
						}
						allParams = append(allParams, subVal)
						paramIdx++
					} else if subOp == "$regex" {
						elemClauses = append(elemClauses,
							fmt.Sprintf("elem#>>'{}' ~ $%d", paramIdx))
						allParams = append(allParams, subVal)
						paramIdx++
					} else {
						return "", nil, startParam, fmt.Errorf("Unsupported $elemMatch operator: %s", subOp)
					}
				}
				if len(elemClauses) > 0 {
					allClauses = append(allClauses, fmt.Sprintf(
						"EXISTS (SELECT 1 FROM jsonb_array_elements(%s) AS elem WHERE %s)",
						fj, strings.Join(elemClauses, " AND ")))
				}
			} else if op == "$text" {
				textMap, ok := operand.(map[string]interface{})
				if !ok {
					return "", nil, startParam, fmt.Errorf("$text requires {$search: 'query'}")
				}
				search, hasSearch := textMap["$search"]
				if !hasSearch {
					return "", nil, startParam, fmt.Errorf("$text requires {$search: 'query'}")
				}
				lang := "english"
				if l, ok := textMap["$language"].(string); ok {
					lang = l
				}
				allClauses = append(allClauses, fmt.Sprintf(
					"to_tsvector($%d, %s) @@ plainto_tsquery($%d, $%d)",
					paramIdx, fieldExpr, paramIdx+1, paramIdx+2))
				allParams = append(allParams, lang, lang, fmt.Sprintf("%v", search))
				paramIdx += 3
			}
		}
	}

	return strings.Join(allClauses, " AND "), allParams, paramIdx, nil
}

func isNumeric(v interface{}) bool {
	switch v.(type) {
	case float64, int, int64:
		return true
	}
	return false
}

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

// DocCreateTtlIndex creates a TTL (time-to-live) trigger on a collection.
func DocCreateTtlIndex(ctx context.Context, q execQuerier, collection string, ttlSeconds int) error {
	if err := validateIdentifier(collection); err != nil {
		return err
	}
	if ttlSeconds <= 0 {
		return fmt.Errorf("ttlSeconds must be positive, got %d", ttlSeconds)
	}
	if err := ensureCollection(ctx, q, collection); err != nil {
		return err
	}

	funcName := collection + "_ttl_cleanup"
	triggerName := collection + "_ttl_trigger"

	createFunc := fmt.Sprintf(
		"CREATE OR REPLACE FUNCTION %s() RETURNS trigger AS $$ "+
			"BEGIN "+
			"DELETE FROM %s WHERE created_at < NOW() - INTERVAL '%d seconds'; "+
			"RETURN NEW; "+
			"END; "+
			"$$ LANGUAGE plpgsql",
		funcName, collection, ttlSeconds)
	if _, err := q.ExecContext(ctx, createFunc); err != nil {
		return fmt.Errorf("create ttl function: %w", err)
	}

	dropTrigger := "DROP TRIGGER IF EXISTS " + triggerName + " ON " + collection
	if _, err := q.ExecContext(ctx, dropTrigger); err != nil {
		return fmt.Errorf("drop existing ttl trigger: %w", err)
	}

	createTrigger := "CREATE TRIGGER " + triggerName +
		" AFTER INSERT ON " + collection +
		" FOR EACH STATEMENT EXECUTE FUNCTION " + funcName + "()"
	if _, err := q.ExecContext(ctx, createTrigger); err != nil {
		return fmt.Errorf("create ttl trigger: %w", err)
	}

	return nil
}

// DocRemoveTtlIndex removes the TTL trigger and function from a collection.
func DocRemoveTtlIndex(ctx context.Context, q execQuerier, collection string) error {
	if err := validateIdentifier(collection); err != nil {
		return err
	}

	triggerName := collection + "_ttl_trigger"
	funcName := collection + "_ttl_cleanup"

	dropTrigger := "DROP TRIGGER IF EXISTS " + triggerName + " ON " + collection
	if _, err := q.ExecContext(ctx, dropTrigger); err != nil {
		return fmt.Errorf("drop ttl trigger: %w", err)
	}

	dropFunc := "DROP FUNCTION IF EXISTS " + funcName + "()"
	if _, err := q.ExecContext(ctx, dropFunc); err != nil {
		return fmt.Errorf("drop ttl function: %w", err)
	}

	return nil
}

// DocCreateCapped creates a capped collection trigger.
func DocCreateCapped(ctx context.Context, q execQuerier, collection string, maxDocs int) error {
	if err := validateIdentifier(collection); err != nil {
		return err
	}
	if maxDocs <= 0 {
		return fmt.Errorf("maxDocs must be positive, got %d", maxDocs)
	}
	if err := ensureCollection(ctx, q, collection); err != nil {
		return err
	}

	funcName := collection + "_cap_enforce"
	triggerName := collection + "_cap_trigger"

	// UUIDs don't sort by insert order, so keep the N most recent rows using
	// created_at. Ties on created_at fall back to _id for determinism.
	createFunc := fmt.Sprintf(
		"CREATE OR REPLACE FUNCTION %s() RETURNS trigger AS $$ "+
			"BEGIN "+
			"DELETE FROM %s WHERE _id NOT IN (SELECT _id FROM %s ORDER BY created_at DESC, _id DESC LIMIT %d); "+
			"RETURN NULL; "+
			"END; "+
			"$$ LANGUAGE plpgsql",
		funcName, collection, collection, maxDocs)
	if _, err := q.ExecContext(ctx, createFunc); err != nil {
		return fmt.Errorf("create cap function: %w", err)
	}

	dropTrigger := "DROP TRIGGER IF EXISTS " + triggerName + " ON " + collection
	if _, err := q.ExecContext(ctx, dropTrigger); err != nil {
		return fmt.Errorf("drop existing cap trigger: %w", err)
	}

	createTrigger := "CREATE TRIGGER " + triggerName +
		" AFTER INSERT ON " + collection +
		" FOR EACH STATEMENT EXECUTE FUNCTION " + funcName + "()"
	if _, err := q.ExecContext(ctx, createTrigger); err != nil {
		return fmt.Errorf("create cap trigger: %w", err)
	}

	return nil
}

// DocRemoveCap removes the capped collection trigger and function.
func DocRemoveCap(ctx context.Context, q execQuerier, collection string) error {
	if err := validateIdentifier(collection); err != nil {
		return err
	}

	triggerName := collection + "_cap_trigger"
	funcName := collection + "_cap_enforce"

	dropTrigger := "DROP TRIGGER IF EXISTS " + triggerName + " ON " + collection
	if _, err := q.ExecContext(ctx, dropTrigger); err != nil {
		return fmt.Errorf("drop cap trigger: %w", err)
	}

	dropFunc := "DROP FUNCTION IF EXISTS " + funcName + "()"
	if _, err := q.ExecContext(ctx, dropFunc); err != nil {
		return fmt.Errorf("drop cap function: %w", err)
	}

	return nil
}
