package goldlapel

import (
	"context"
	"fmt"
)

// PercolateAdd registers a named query for reverse matching.
// Like Elasticsearch percolator. Auto-creates the percolator table and GIN
// index if they don't exist, then upserts the query.
func PercolateAdd(ctx context.Context, q execQuerier, name, queryID, query string, opts ...SearchOption) error {
	o := &searchOptions{lang: "english"}
	for _, fn := range opts {
		fn(o)
	}

	if err := validateIdentifier(name); err != nil {
		return err
	}

	createTable := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		query_id TEXT PRIMARY KEY,
		query_text TEXT NOT NULL,
		tsquery TSQUERY NOT NULL,
		lang TEXT NOT NULL DEFAULT 'english',
		metadata JSONB,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`, name)
	if _, err := q.ExecContext(ctx, createTable); err != nil {
		return fmt.Errorf("create percolator table: %w", err)
	}

	createIndex := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_tsq_idx ON %s USING GIST (tsquery)", name, name)
	if _, err := q.ExecContext(ctx, createIndex); err != nil {
		return fmt.Errorf("create percolator index: %w", err)
	}

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
	_, err := q.ExecContext(ctx, upsert, queryID, query, o.lang, metadataArg)
	return err
}

// Percolate matches a document against stored queries.
func Percolate(ctx context.Context, q execQuerier, name, text string, opts ...SearchOption) ([]map[string]interface{}, error) {
	o := &searchOptions{limit: 50, lang: "english"}
	for _, fn := range opts {
		fn(o)
	}

	if err := validateIdentifier(name); err != nil {
		return nil, err
	}

	sqlQ := fmt.Sprintf(`SELECT query_id, query_text, metadata,
		ts_rank(to_tsvector($1, $2), tsquery) AS _score
		FROM %s
		WHERE to_tsvector($1, $2) @@ tsquery
		ORDER BY _score DESC LIMIT $3`, name)

	rows, err := q.QueryContext(ctx, sqlQ, o.lang, text, o.limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

// PercolateDelete removes a stored query from a percolator index.
func PercolateDelete(ctx context.Context, q execQuerier, name, queryID string) (bool, error) {
	if err := validateIdentifier(name); err != nil {
		return false, err
	}

	sqlQ := fmt.Sprintf("DELETE FROM %s WHERE query_id = $1", name)
	result, err := q.ExecContext(ctx, sqlQ, queryID)
	if err != nil {
		return false, err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}
