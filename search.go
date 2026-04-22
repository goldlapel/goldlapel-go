package goldlapel

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

// identifierRe caps at 63 chars (Postgres NAMEDATALEN-1) so wrapper-side
// validation matches the proxy's server-side regex exactly:
// `^[A-Za-z_][A-Za-z0-9_]{0,62}$`.
var identifierRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)

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

// WithLimit sets the maximum number of results.
func WithLimit(n int) Option {
	return searchOnly(func(o *searchOptions) { o.limit = n })
}

// WithLang sets the language for full-text search (default "english").
func WithLang(lang string) Option {
	return searchOnly(func(o *searchOptions) { o.lang = lang })
}

// WithThreshold sets the similarity threshold for fuzzy search (default 0.3).
func WithThreshold(t float64) Option {
	return searchOnly(func(o *searchOptions) { o.threshold = t })
}

// WithHighlight enables ts_headline highlighting for full-text search.
func WithHighlight(on bool) Option {
	return searchOnly(func(o *searchOptions) { o.highlight = on })
}

// WithQuery sets the full-text search query string for filtering.
func WithQuery(q string) Option {
	return searchOnly(func(o *searchOptions) { o.query = q })
}

// WithQueryColumn sets the column(s) to search against. Accepts string or []string.
func WithQueryColumn(col interface{}) Option {
	return searchOnly(func(o *searchOptions) { o.queryColumn = col })
}

// WithGroupBy sets the GROUP BY column for aggregate queries.
func WithGroupBy(col string) Option {
	return searchOnly(func(o *searchOptions) { o.groupBy = col })
}

// WithMetadata sets JSON metadata for percolator queries.
func WithMetadata(meta string) Option {
	return searchOnly(func(o *searchOptions) { o.metadata = meta })
}

// scanRows reads all rows from a *sql.Rows using dynamic column scanning.
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
func Search(ctx context.Context, q execQuerier, table string, columns interface{}, query string, opts ...Option) ([]map[string]interface{}, error) {
	o := &searchOptions{limit: 50, lang: "english"}
	for _, opt := range opts {
		opt.applySearch(o)
	}

	if err := validateIdentifier(table); err != nil {
		return nil, err
	}

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

	tsvParts := make([]string, len(cols))
	for i, col := range cols {
		tsvParts[i] = fmt.Sprintf("coalesce(%s, '')", col)
	}
	tsvExpr := fmt.Sprintf("to_tsvector($1, %s)", strings.Join(tsvParts, " || ' ' || "))
	tsq := "plainto_tsquery($2, $3)"

	highlightCol := cols[0]

	var sqlQ string
	var params []interface{}

	if o.highlight {
		sqlQ = fmt.Sprintf(
			"SELECT *, ts_rank(%s, %s) AS _score, ts_headline($4, coalesce(%s, ''), %s, 'StartSel=<mark>, StopSel=</mark>, MaxWords=35, MinWords=15') AS _highlight FROM %s WHERE %s @@ %s ORDER BY _score DESC LIMIT $5",
			tsvExpr, tsq, highlightCol, tsq, table, tsvExpr, tsq)
		params = []interface{}{o.lang, o.lang, query, o.lang, o.limit}
	} else {
		sqlQ = fmt.Sprintf(
			"SELECT *, ts_rank(%s, %s) AS _score FROM %s WHERE %s @@ %s ORDER BY _score DESC LIMIT $4",
			tsvExpr, tsq, table, tsvExpr, tsq)
		params = []interface{}{o.lang, o.lang, query, o.limit}
	}

	rows, err := q.QueryContext(ctx, sqlQ, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

// SearchFuzzy performs trigram-based fuzzy search.
func SearchFuzzy(ctx context.Context, q execQuerier, table, column, query string, opts ...Option) ([]map[string]interface{}, error) {
	o := &searchOptions{limit: 50, threshold: 0.3}
	for _, opt := range opts {
		opt.applySearch(o)
	}

	if err := validateIdentifier(table); err != nil {
		return nil, err
	}
	if err := validateIdentifier(column); err != nil {
		return nil, err
	}

	sqlQ := fmt.Sprintf(
		"SELECT *, similarity(%s, $1) AS _score FROM %s WHERE similarity(%s, $1) > $2 ORDER BY _score DESC LIMIT $3",
		column, table, column)

	rows, err := q.QueryContext(ctx, sqlQ, query, o.threshold, o.limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

// SearchPhonetic performs phonetic (sounds-like) search.
func SearchPhonetic(ctx context.Context, q execQuerier, table, column, query string, opts ...Option) ([]map[string]interface{}, error) {
	o := &searchOptions{limit: 50}
	for _, opt := range opts {
		opt.applySearch(o)
	}

	if err := validateIdentifier(table); err != nil {
		return nil, err
	}
	if err := validateIdentifier(column); err != nil {
		return nil, err
	}

	sqlQ := fmt.Sprintf(
		"SELECT *, similarity(%s, $1) AS _score FROM %s WHERE soundex(%s) = soundex($1) ORDER BY _score DESC, %s LIMIT $2",
		column, table, column, column)

	rows, err := q.QueryContext(ctx, sqlQ, query, o.limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

// Similar performs vector similarity search using pgvector.
func Similar(ctx context.Context, q execQuerier, table, column string, vector []float64, opts ...Option) ([]map[string]interface{}, error) {
	o := &searchOptions{limit: 10}
	for _, opt := range opts {
		opt.applySearch(o)
	}

	if err := validateIdentifier(table); err != nil {
		return nil, err
	}
	if err := validateIdentifier(column); err != nil {
		return nil, err
	}

	parts := make([]string, len(vector))
	for i, v := range vector {
		parts[i] = fmt.Sprintf("%g", v)
	}
	vectorLiteral := "[" + strings.Join(parts, ",") + "]"

	sqlQ := fmt.Sprintf(
		"SELECT *, (%s <=> $1::vector) AS _score FROM %s ORDER BY _score LIMIT $2",
		column, table)

	rows, err := q.QueryContext(ctx, sqlQ, vectorLiteral, o.limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

// Suggest performs prefix-based autocomplete search.
func Suggest(ctx context.Context, q execQuerier, table, column, prefix string, opts ...Option) ([]map[string]interface{}, error) {
	o := &searchOptions{limit: 10}
	for _, opt := range opts {
		opt.applySearch(o)
	}

	if err := validateIdentifier(table); err != nil {
		return nil, err
	}
	if err := validateIdentifier(column); err != nil {
		return nil, err
	}

	sqlQ := fmt.Sprintf(
		"SELECT *, similarity(%s, $1) AS _score FROM %s WHERE %s ILIKE $2 ORDER BY _score DESC, %s LIMIT $3",
		column, table, column, column)

	rows, err := q.QueryContext(ctx, sqlQ, prefix, prefix+"%", o.limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

// Facets performs a terms aggregation on a column.
func Facets(ctx context.Context, q execQuerier, table, column string, opts ...Option) ([]map[string]interface{}, error) {
	o := &searchOptions{limit: 50, lang: "english"}
	for _, opt := range opts {
		opt.applySearch(o)
	}

	if err := validateIdentifier(table); err != nil {
		return nil, err
	}
	if err := validateIdentifier(column); err != nil {
		return nil, err
	}

	var sqlQ string
	var params []interface{}

	if o.query != "" && o.queryColumn != nil {
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

		sqlQ = fmt.Sprintf(
			"SELECT %s AS value, COUNT(*) AS count FROM %s WHERE %s @@ plainto_tsquery($2, $3) GROUP BY %s ORDER BY count DESC, %s LIMIT $4",
			column, table, tsvExpr, column, column)
		params = []interface{}{o.lang, o.lang, o.query, o.limit}
	} else {
		sqlQ = fmt.Sprintf(
			"SELECT %s AS value, COUNT(*) AS count FROM %s GROUP BY %s ORDER BY count DESC, %s LIMIT $1",
			column, table, column, column)
		params = []interface{}{o.limit}
	}

	rows, err := q.QueryContext(ctx, sqlQ, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

var validAggregateFuncs = map[string]bool{
	"count": true,
	"sum":   true,
	"avg":   true,
	"min":   true,
	"max":   true,
}

// Aggregate performs an aggregate function on a column.
func Aggregate(ctx context.Context, q execQuerier, table, column, funcName string, opts ...Option) ([]map[string]interface{}, error) {
	o := &searchOptions{limit: 50}
	for _, opt := range opts {
		opt.applySearch(o)
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

	var sqlQ string
	var params []interface{}

	if o.groupBy != "" {
		if err := validateIdentifier(o.groupBy); err != nil {
			return nil, err
		}
		sqlQ = fmt.Sprintf(
			"SELECT %s, %s AS value FROM %s GROUP BY %s ORDER BY value DESC LIMIT $1",
			o.groupBy, aggExpr, table, o.groupBy)
		params = []interface{}{o.limit}
	} else {
		sqlQ = fmt.Sprintf("SELECT %s AS value FROM %s", aggExpr, table)
		params = nil
	}

	rows, err := q.QueryContext(ctx, sqlQ, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

// Analyze shows how PostgreSQL breaks text into searchable tokens.
func Analyze(ctx context.Context, q execQuerier, text string, opts ...Option) ([]map[string]interface{}, error) {
	o := &searchOptions{lang: "english"}
	for _, opt := range opts {
		opt.applySearch(o)
	}

	rows, err := q.QueryContext(ctx,
		"SELECT alias, description, token, dictionaries, dictionary, lexemes FROM ts_debug($1, $2)",
		o.lang, text)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

// ExplainScore shows why a specific row scores the way it does for a query.
func ExplainScore(ctx context.Context, q execQuerier, table, column, query, idColumn string, idValue interface{}, opts ...Option) (map[string]interface{}, error) {
	o := &searchOptions{lang: "english"}
	for _, opt := range opts {
		opt.applySearch(o)
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

	sqlQ := fmt.Sprintf(
		`SELECT %s AS document_text, to_tsvector($1, %s)::text AS document_tokens,
    plainto_tsquery($1, $2)::text AS query_tokens,
    to_tsvector($1, %s) @@ plainto_tsquery($1, $2) AS matches,
    ts_rank(to_tsvector($1, %s), plainto_tsquery($1, $2)) AS score,
    ts_headline($1, %s, plainto_tsquery($1, $2), 'StartSel=**, StopSel=**, MaxWords=50, MinWords=20') AS headline
FROM %s WHERE %s = $3`,
		column, column, column, column, column, table, idColumn)

	rows, err := q.QueryContext(ctx, sqlQ, o.lang, query, idValue)
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
func CreateSearchConfig(ctx context.Context, q execQuerier, name, copyFrom string) error {
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
	err := q.QueryRowContext(ctx, "SELECT 1 FROM pg_ts_config WHERE cfgname = $1", name).Scan(&exists)
	if err == nil {
		return nil
	}
	if err != sql.ErrNoRows {
		return err
	}

	_, err = q.ExecContext(ctx, fmt.Sprintf("CREATE TEXT SEARCH CONFIGURATION %s (COPY = %s)", name, copyFrom))
	return err
}
