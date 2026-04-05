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
	limit     int
	lang      string
	threshold float64
	highlight bool
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
func Search(db *sql.DB, table, column, query string, opts ...SearchOption) ([]map[string]interface{}, error) {
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

	tsv := fmt.Sprintf("to_tsvector($1, coalesce(%s, ''))", column)
	tsq := "plainto_tsquery($2, $3)"

	var q string
	var params []interface{}

	if o.highlight {
		q = fmt.Sprintf(
			"SELECT *, ts_rank(%s, %s) AS _score, ts_headline($4, coalesce(%s, ''), %s) AS _highlight FROM %s WHERE %s @@ %s ORDER BY _score DESC LIMIT $5",
			tsv, tsq, column, tsq, table, tsv, tsq)
		params = []interface{}{o.lang, o.lang, query, o.lang, o.limit}
	} else {
		q = fmt.Sprintf(
			"SELECT *, ts_rank(%s, %s) AS _score FROM %s WHERE %s @@ %s ORDER BY _score DESC LIMIT $4",
			tsv, tsq, table, tsv, tsq)
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
