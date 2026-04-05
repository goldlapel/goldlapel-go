package goldlapel

import (
	"database/sql"
	"fmt"
)

// PercolateAdd registers a named query for reverse matching.
// Like Elasticsearch percolator. Auto-creates the percolator table and GIN
// index if they don't exist, then upserts the query.
// Options: WithLang (default "english"), WithMetadata (JSON string).
func PercolateAdd(db *sql.DB, name, queryID, query string, opts ...SearchOption) error {
	o := &searchOptions{lang: "english"}
	for _, fn := range opts {
		fn(o)
	}

	if err := validateIdentifier(name); err != nil {
		return err
	}

	// Auto-create percolator table
	createTable := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		query_id TEXT PRIMARY KEY,
		query_text TEXT NOT NULL,
		tsquery TSQUERY NOT NULL,
		lang TEXT NOT NULL DEFAULT 'english',
		metadata JSONB,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`, name)
	if _, err := db.Exec(createTable); err != nil {
		return fmt.Errorf("create percolator table: %w", err)
	}

	// Auto-create GIN index on tsquery column
	createIndex := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_tsq_idx ON %s USING GIN (tsquery)", name, name)
	if _, err := db.Exec(createIndex); err != nil {
		return fmt.Errorf("create percolator index: %w", err)
	}

	// Upsert the query
	var metadataArg interface{}
	if o.metadata != "" {
		metadataArg = o.metadata
	}

	upsert := fmt.Sprintf(`INSERT INTO %s (query_id, query_text, tsquery, lang, metadata)
		VALUES ($1, $2, plainto_tsquery($3, $2), $3, $4)
		ON CONFLICT (query_id) DO UPDATE SET
			query_text = EXCLUDED.query_text,
			tsquery = EXCLUDED.tsquery,
			lang = EXCLUDED.lang,
			metadata = EXCLUDED.metadata`, name)
	_, err := db.Exec(upsert, queryID, query, o.lang, metadataArg)
	return err
}

// Percolate matches a document against stored queries.
// Like Elasticsearch percolate API. Returns matching queries ranked by
// relevance score. Default limit=50, lang="english".
func Percolate(db *sql.DB, name, text string, opts ...SearchOption) ([]map[string]interface{}, error) {
	o := &searchOptions{limit: 50, lang: "english"}
	for _, fn := range opts {
		fn(o)
	}

	if err := validateIdentifier(name); err != nil {
		return nil, err
	}

	q := fmt.Sprintf(`SELECT query_id, query_text, metadata,
		ts_rank(to_tsvector($1, $2), tsquery) AS _score
		FROM %s
		WHERE to_tsvector($1, $2) @@ tsquery
		ORDER BY _score DESC LIMIT $3`, name)

	rows, err := db.Query(q, o.lang, text, o.limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

// PercolateDelete removes a stored query from a percolator index.
// Returns true if a row was deleted, false if the query_id was not found.
func PercolateDelete(db *sql.DB, name, queryID string) (bool, error) {
	if err := validateIdentifier(name); err != nil {
		return false, err
	}

	q := fmt.Sprintf("DELETE FROM %s WHERE query_id = $1", name)
	result, err := db.Exec(q, queryID)
	if err != nil {
		return false, err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}
