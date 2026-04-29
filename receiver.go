package goldlapel

import (
	"context"
)

// This file wires the still-flat package-level helpers (Search, Pub/Sub,
// Percolate, CountDistinct, Script) onto *GoldLapel as receiver methods.
// Each method resolves the query target (transaction override via WithTx,
// scoped transaction from InTx, or the default *sql.DB opened at Start)
// and forwards.
//
// Schema-to-core families have moved to nested sub-API namespaces:
//   - gl.Documents.<Verb> (Phase 4) — see documents.go
//   - gl.Streams.<Verb>   (Phase 4) — see streams.go
//   - gl.Counters.<Verb>  (Phase 5) — see counters.go
//   - gl.Zsets.<Verb>     (Phase 5) — see zsets.go
//   - gl.Hashes.<Verb>    (Phase 5) — see hashes.go
//   - gl.Queues.<Verb>    (Phase 5) — see queues.go
//   - gl.Geos.<Verb>      (Phase 5) — see geos.go
//
// Hard cut (no aliases): the legacy flat methods (Enqueue/Dequeue, Incr/
// GetCounter, Hset/Hget/Hgetall/Hdel, Zadd/Zincrby/Zrange/Zrank/Zscore/Zrem,
// Geoadd/Geodist/Georadius) ran against user-named tables and silently
// bypassed proxy DDL. They are removed in Phase 5 — callers must migrate
// to the nested namespaces. Schema-to-core moves DDL ownership into the
// proxy, and the nested namespaces are the only path that materializes
// the canonical _goldlapel.<family>_<name> tables.

// --- Search ---

func (gl *GoldLapel) Search(ctx context.Context, table string, columns interface{}, query string, opts ...Option) ([]map[string]interface{}, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return Search(ctx, q, table, columns, query, opts...)
}

func (gl *GoldLapel) SearchFuzzy(ctx context.Context, table, column, query string, opts ...Option) ([]map[string]interface{}, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return SearchFuzzy(ctx, q, table, column, query, opts...)
}

func (gl *GoldLapel) SearchPhonetic(ctx context.Context, table, column, query string, opts ...Option) ([]map[string]interface{}, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return SearchPhonetic(ctx, q, table, column, query, opts...)
}

func (gl *GoldLapel) Similar(ctx context.Context, table, column string, vector []float64, opts ...Option) ([]map[string]interface{}, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return Similar(ctx, q, table, column, vector, opts...)
}

func (gl *GoldLapel) Suggest(ctx context.Context, table, column, prefix string, opts ...Option) ([]map[string]interface{}, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return Suggest(ctx, q, table, column, prefix, opts...)
}

func (gl *GoldLapel) Facets(ctx context.Context, table, column string, opts ...Option) ([]map[string]interface{}, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return Facets(ctx, q, table, column, opts...)
}

func (gl *GoldLapel) Aggregate(ctx context.Context, table, column, funcName string, opts ...Option) ([]map[string]interface{}, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return Aggregate(ctx, q, table, column, funcName, opts...)
}

func (gl *GoldLapel) CreateSearchConfig(ctx context.Context, name, copyFrom string, opts ...Option) error {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return err
	}
	return CreateSearchConfig(ctx, q, name, copyFrom)
}

func (gl *GoldLapel) Analyze(ctx context.Context, text string, opts ...Option) ([]map[string]interface{}, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return Analyze(ctx, q, text, opts...)
}

func (gl *GoldLapel) ExplainScore(ctx context.Context, table, column, query, idColumn string, idValue interface{}, opts ...Option) (map[string]interface{}, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return ExplainScore(ctx, q, table, column, query, idColumn, idValue, opts...)
}

// --- Pub/Sub ---

func (gl *GoldLapel) Publish(ctx context.Context, channel, message string, opts ...Option) error {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return err
	}
	return Publish(ctx, q, channel, message)
}

func (gl *GoldLapel) Subscribe(ctx context.Context, channel string, callback func(channel, payload string)) error {
	gl.mu.Lock()
	url := gl.proxyURL
	gl.mu.Unlock()
	if url == "" {
		return ErrNotConnected
	}
	return Subscribe(ctx, url, channel, callback)
}

func (gl *GoldLapel) SubscribeAsync(ctx context.Context, channel string, callback func(channel, payload string)) chan error {
	gl.mu.Lock()
	url := gl.proxyURL
	gl.mu.Unlock()
	if url == "" {
		ch := make(chan error, 1)
		ch <- ErrNotConnected
		return ch
	}
	return SubscribeAsync(ctx, url, channel, callback)
}

// --- Misc ---

func (gl *GoldLapel) CountDistinct(ctx context.Context, table, column string, opts ...Option) (int64, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	return CountDistinct(ctx, q, table, column)
}

func (gl *GoldLapel) Script(ctx context.Context, luaCode string, args ...string) (*string, error) {
	q, err := gl.resolveExec(nil)
	if err != nil {
		return nil, err
	}
	return Script(ctx, q, luaCode, args...)
}

// --- Streams ---
//
// Stream<Verb> moved to gl.Streams.<Verb> — see streams.go.

// --- Percolate ---

func (gl *GoldLapel) PercolateAdd(ctx context.Context, name, queryID, query string, opts ...Option) error {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return err
	}
	return PercolateAdd(ctx, q, name, queryID, query, opts...)
}

func (gl *GoldLapel) Percolate(ctx context.Context, name, text string, opts ...Option) ([]map[string]interface{}, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return Percolate(ctx, q, name, text, opts...)
}

func (gl *GoldLapel) PercolateDelete(ctx context.Context, name, queryID string, opts ...Option) (bool, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return false, err
	}
	return PercolateDelete(ctx, q, name, queryID)
}

// Operational Doc<Verb> methods (Watch/Unwatch/TTL/Capped) moved to
// gl.Documents.<Verb> — see documents.go.
