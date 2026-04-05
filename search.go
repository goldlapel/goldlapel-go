package goldlapel

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

var identifierRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func validateIdentifier(name string) error {
	if name == "" {
		return fmt.Errorf("identifier must be a non-empty string")
	}
	if !identifierRe.MatchString(name) {
		return fmt.Errorf("invalid identifier: %s", name)
	}
	return nil
}

// searchOptions holds configuration for search functions.
type searchOptions struct {
	limit       int
	lang        string
	threshold   float64
	highlight   bool
	query       string
	queryColumn interface{}
	groupBy     string
	metadata    string
}

// SearchOption configures a search call.
type SearchOption func(*searchOptions)

// WithLimit sets the maximum number of results.
func WithLimit(n int) SearchOption {
	return func(o *searchOptions) { o.limit = n }
}

// WithLang sets the language for full-text search (default "english").
func WithLang(lang string) SearchOption {
	return func(o *searchOptions) { o.lang = lang }
}

// WithThreshold sets the similarity threshold for fuzzy search (default 0.3).
func WithThreshold(t float64) SearchOption {
	return func(o *searchOptions) { o.threshold = t }
}

// WithHighlight enables ts_headline highlighting for full-text search.
func WithHighlight(on bool) SearchOption {
	return func(o *searchOptions) { o.highlight = on }
}

// WithQuery sets the full-text search query string for filtering.
func WithQuery(q string) SearchOption {
	return func(o *searchOptions) { o.query = q }
}

// WithQueryColumn sets the column(s) to search against. Accepts string or []string.
func WithQueryColumn(col interface{}) SearchOption {
	return func(o *searchOptions) { o.queryColumn = col }
}

// WithGroupBy sets the GROUP BY column for aggregate queries.
func WithGroupBy(col string) SearchOption {
	return func(o *searchOptions) { o.groupBy = col }
}

// WithMetadata sets JSON metadata for percolator queries.
func WithMetadata(meta string) SearchOption {
	return func(o *searchOptions) { o.metadata = meta }
}

// scanRows reads all rows from a *sql.Rows using dynamic column scanning.
// Each row is returned as a map[string]interface{} with []byte values
// converted to strings.
func scanRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
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
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
	}
	return results, rows.Err()
}

// Search performs full-text search using tsvector/tsquery.
// Like Elasticsearch full-text queries. Returns rows with a _score field
// ranked by relevance. Default limit=50, lang="english".
// Accepts one or more column names. Multiple columns are concatenated
// for searching (like Python's multi-column support).
func Search(db *sql.DB, table string, columns interface{}, query string, opts ...SearchOption) ([]map[string]interface{}, error) {
	o := &searchOptions{limit: 50, lang: "english"}
	for _, fn := range opts {
		fn(o)
	}

	if err := validateIdentifier(table); err != nil {
		return nil, err
	}

	// Normalize columns to a slice
	var cols []string
	switch v := columns.(type) {
	case string:
		cols = []string{v}
	case []string:
		cols = v
	default:
		return nil, fmt.Errorf("columns must be a string or []string")
	}

	for _, col := range cols {
		if err := validateIdentifier(col); err != nil {
			return nil, err
		}
	}

	// Build tsvector expression: to_tsvector($1, coalesce(col1, '') || ' ' || coalesce(col2, ''))
	tsvParts := make([]string, len(cols))
	for i, col := range cols {
		tsvParts[i] = fmt.Sprintf("coalesce(%s, '')", col)
	}
	tsvExpr := fmt.Sprintf("to_tsvector($1, %s)", strings.Join(tsvParts, " || ' ' || "))
	tsq := "plainto_tsquery($2, $3)"

	// For highlight, use the first column
	highlightCol := cols[0]

	var q string
	var params []interface{}

	if o.highlight {
		q = fmt.Sprintf(
			"SELECT *, ts_rank(%s, %s) AS _score, ts_headline($4, coalesce(%s, ''), %s, 'StartSel=<mark>, StopSel=</mark>, MaxWords=35, MinWords=15') AS _highlight FROM %s WHERE %s @@ %s ORDER BY _score DESC LIMIT $5",
			tsvExpr, tsq, highlightCol, tsq, table, tsvExpr, tsq)
		params = []interface{}{o.lang, o.lang, query, o.lang, o.limit}
	} else {
		q = fmt.Sprintf(
			"SELECT *, ts_rank(%s, %s) AS _score FROM %s WHERE %s @@ %s ORDER BY _score DESC LIMIT $4",
			tsvExpr, tsq, table, tsvExpr, tsq)
		params = []interface{}{o.lang, o.lang, query, o.limit}
	}

	rows, err := db.Query(q, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

// SearchFuzzy performs trigram-based fuzzy search.
// Like Elasticsearch fuzzy queries. Uses pg_trgm similarity() to find
// approximate matches. Default limit=50, threshold=0.3.
func SearchFuzzy(db *sql.DB, table, column, query string, opts ...SearchOption) ([]map[string]interface{}, error) {
	o := &searchOptions{limit: 50, threshold: 0.3}
	for _, fn := range opts {
		fn(o)
	}

	if err := validateIdentifier(table); err != nil {
		return nil, err
	}
	if err := validateIdentifier(column); err != nil {
		return nil, err
	}

	_, err := db.Exec("CREATE EXTENSION IF NOT EXISTS pg_trgm")
	if err != nil {
		return nil, fmt.Errorf("create pg_trgm extension: %w", err)
	}

	q := fmt.Sprintf(
		"SELECT *, similarity(%s, $1) AS _score FROM %s WHERE similarity(%s, $1) > $2 ORDER BY _score DESC LIMIT $3",
		column, table, column)

	rows, err := db.Query(q, query, o.threshold, o.limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

// SearchPhonetic performs phonetic (sounds-like) search.
// Like Elasticsearch phonetic token filters. Uses soundex() for matching
// and similarity() for ranking. Requires fuzzystrmatch and pg_trgm.
// Default limit=50.
func SearchPhonetic(db *sql.DB, table, column, query string, opts ...SearchOption) ([]map[string]interface{}, error) {
	o := &searchOptions{limit: 50}
	for _, fn := range opts {
		fn(o)
	}

	if err := validateIdentifier(table); err != nil {
		return nil, err
	}
	if err := validateIdentifier(column); err != nil {
		return nil, err
	}

	_, err := db.Exec("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch")
	if err != nil {
		return nil, fmt.Errorf("create fuzzystrmatch extension: %w", err)
	}
	_, err = db.Exec("CREATE EXTENSION IF NOT EXISTS pg_trgm")
	if err != nil {
		return nil, fmt.Errorf("create pg_trgm extension: %w", err)
	}

	q := fmt.Sprintf(
		"SELECT *, similarity(%s, $1) AS _score FROM %s WHERE soundex(%s) = soundex($1) ORDER BY _score DESC, %s LIMIT $2",
		column, table, column, column)

	rows, err := db.Query(q, query, o.limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

// Similar performs vector similarity search using pgvector.
// Like Elasticsearch kNN queries. Uses the <=> (cosine distance) operator
// to find the nearest vectors. Default limit=10.
func Similar(db *sql.DB, table, column string, vector []float64, opts ...SearchOption) ([]map[string]interface{}, error) {
	o := &searchOptions{limit: 10}
	for _, fn := range opts {
		fn(o)
	}

	if err := validateIdentifier(table); err != nil {
		return nil, err
	}
	if err := validateIdentifier(column); err != nil {
		return nil, err
	}

	_, err := db.Exec("CREATE EXTENSION IF NOT EXISTS vector")
	if err != nil {
		return nil, fmt.Errorf("create vector extension: %w", err)
	}

	parts := make([]string, len(vector))
	for i, v := range vector {
		parts[i] = fmt.Sprintf("%g", v)
	}
	vectorLiteral := "[" + strings.Join(parts, ",") + "]"

	q := fmt.Sprintf(
		"SELECT *, (%s <=> $1::vector) AS _score FROM %s ORDER BY _score LIMIT $2",
		column, table)

	rows, err := db.Query(q, vectorLiteral, o.limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

// Suggest performs prefix-based autocomplete search.
// Like Elasticsearch completion suggesters. Uses ILIKE for prefix matching
// and similarity() for ranking. Requires pg_trgm. Default limit=10.
func Suggest(db *sql.DB, table, column, prefix string, opts ...SearchOption) ([]map[string]interface{}, error) {
	o := &searchOptions{limit: 10}
	for _, fn := range opts {
		fn(o)
	}

	if err := validateIdentifier(table); err != nil {
		return nil, err
	}
	if err := validateIdentifier(column); err != nil {
		return nil, err
	}

	_, err := db.Exec("CREATE EXTENSION IF NOT EXISTS pg_trgm")
	if err != nil {
		return nil, fmt.Errorf("create pg_trgm extension: %w", err)
	}

	q := fmt.Sprintf(
		"SELECT *, similarity(%s, $1) AS _score FROM %s WHERE %s ILIKE $2 ORDER BY _score DESC, %s LIMIT $3",
		column, table, column, column)

	rows, err := db.Query(q, prefix, prefix+"%", o.limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

// Facets performs a terms aggregation on a column.
// Like Elasticsearch terms aggregation. Returns [{value, count}] sorted by
// count descending. Optionally filters by full-text search when WithQuery
// and WithQueryColumn are provided. Default limit=50, lang="english".
func Facets(db *sql.DB, table, column string, opts ...SearchOption) ([]map[string]interface{}, error) {
	o := &searchOptions{limit: 50, lang: "english"}
	for _, fn := range opts {
		fn(o)
	}

	if err := validateIdentifier(table); err != nil {
		return nil, err
	}
	if err := validateIdentifier(column); err != nil {
		return nil, err
	}

	var q string
	var params []interface{}

	if o.query != "" && o.queryColumn != nil {
		// Normalize queryColumn to a slice
		var qCols []string
		switch v := o.queryColumn.(type) {
		case string:
			qCols = []string{v}
		case []string:
			qCols = v
		default:
			return nil, fmt.Errorf("queryColumn must be a string or []string")
		}

		for _, col := range qCols {
			if err := validateIdentifier(col); err != nil {
				return nil, err
			}
		}

		tsvParts := make([]string, len(qCols))
		for i, col := range qCols {
			tsvParts[i] = fmt.Sprintf("coalesce(%s, '')", col)
		}
		tsvExpr := fmt.Sprintf("to_tsvector($1, %s)", strings.Join(tsvParts, " || ' ' || "))

		q = fmt.Sprintf(
			"SELECT %s AS value, COUNT(*) AS count FROM %s WHERE %s @@ plainto_tsquery($2, $3) GROUP BY %s ORDER BY count DESC, %s LIMIT $4",
			column, table, tsvExpr, column, column)
		params = []interface{}{o.lang, o.lang, o.query, o.limit}
	} else {
		q = fmt.Sprintf(
			"SELECT %s AS value, COUNT(*) AS count FROM %s GROUP BY %s ORDER BY count DESC, %s LIMIT $1",
			column, table, column, column)
		params = []interface{}{o.limit}
	}

	rows, err := db.Query(q, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

// validAggregateFuncs is the set of allowed SQL aggregate function names.
var validAggregateFuncs = map[string]bool{
	"count": true,
	"sum":   true,
	"avg":   true,
	"min":   true,
	"max":   true,
}

// Aggregate performs an aggregate function on a column.
// Like Elasticsearch metric aggregations. funcName must be one of: count,
// sum, avg, min, max. With WithGroupBy, groups results by that column.
// Default limit=50.
func Aggregate(db *sql.DB, table, column, funcName string, opts ...SearchOption) ([]map[string]interface{}, error) {
	o := &searchOptions{limit: 50}
	for _, fn := range opts {
		fn(o)
	}

	if err := validateIdentifier(table); err != nil {
		return nil, err
	}
	if err := validateIdentifier(column); err != nil {
		return nil, err
	}

	funcLower := strings.ToLower(funcName)
	if !validAggregateFuncs[funcLower] {
		return nil, fmt.Errorf("invalid aggregate function: %s (must be one of: count, sum, avg, min, max)", funcName)
	}

	var aggExpr string
	if funcLower == "count" {
		aggExpr = "COUNT(*)"
	} else {
		aggExpr = fmt.Sprintf("%s(%s)", strings.ToUpper(funcLower), column)
	}

	var q string
	var params []interface{}

	if o.groupBy != "" {
		if err := validateIdentifier(o.groupBy); err != nil {
			return nil, err
		}
		q = fmt.Sprintf(
			"SELECT %s, %s AS value FROM %s GROUP BY %s ORDER BY value DESC LIMIT $1",
			o.groupBy, aggExpr, table, o.groupBy)
		params = []interface{}{o.limit}
	} else {
		q = fmt.Sprintf("SELECT %s AS value FROM %s", aggExpr, table)
		params = nil
	}

	rows, err := db.Query(q, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

// Analyze shows how PostgreSQL breaks text into searchable tokens.
// Returns one row per token with alias, description, token, dictionaries,
// dictionary, and lexemes columns. Useful for understanding why a search
// matches or doesn't match. Default lang="english".
func Analyze(db *sql.DB, text string, opts ...SearchOption) ([]map[string]interface{}, error) {
	o := &searchOptions{lang: "english"}
	for _, fn := range opts {
		fn(o)
	}

	rows, err := db.Query(
		"SELECT alias, description, token, dictionaries, dictionary, lexemes FROM ts_debug($1, $2)",
		o.lang, text)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

// ExplainScore shows why a specific row scores the way it does for a query.
// Returns a single map with document_text, document_tokens, query_tokens,
// matches, score, and headline fields. Returns nil (not an error) if the
// row is not found. Default lang="english".
func ExplainScore(db *sql.DB, table, column, query, idColumn string, idValue interface{}, opts ...SearchOption) (map[string]interface{}, error) {
	o := &searchOptions{lang: "english"}
	for _, fn := range opts {
		fn(o)
	}

	if err := validateIdentifier(table); err != nil {
		return nil, err
	}
	if err := validateIdentifier(column); err != nil {
		return nil, err
	}
	if err := validateIdentifier(idColumn); err != nil {
		return nil, err
	}

	q := fmt.Sprintf(
		`SELECT %s AS document_text, to_tsvector($1, %s)::text AS document_tokens,
    plainto_tsquery($1, $2)::text AS query_tokens,
    to_tsvector($1, %s) @@ plainto_tsquery($1, $2) AS matches,
    ts_rank(to_tsvector($1, %s), plainto_tsquery($1, $2)) AS score,
    ts_headline($1, %s, plainto_tsquery($1, $2), 'StartSel=**, StopSel=**, MaxWords=50, MinWords=20') AS headline
FROM %s WHERE %s = $3`,
		column, column, column, column, column, table, idColumn)

	rows, err := db.Query(q, o.lang, query, idValue)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results, err := scanRows(rows)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	return results[0], nil
}

// CreateSearchConfig creates a custom text search configuration.
// Copies from an existing configuration (default "english").
// No-ops if the configuration already exists.
func CreateSearchConfig(db *sql.DB, name, copyFrom string) error {
	if copyFrom == "" {
		copyFrom = "english"
	}

	if err := validateIdentifier(name); err != nil {
		return err
	}
	if err := validateIdentifier(copyFrom); err != nil {
		return err
	}

	var exists int
	err := db.QueryRow("SELECT 1 FROM pg_ts_config WHERE cfgname = $1", name).Scan(&exists)
	if err == nil {
		return nil
	}
	if err != sql.ErrNoRows {
		return err
	}

	_, err = db.Exec(fmt.Sprintf("CREATE TEXT SEARCH CONFIGURATION %s (COPY = %s)", name, copyFrom))
	return err
}
