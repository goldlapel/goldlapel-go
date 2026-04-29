package goldlapel

import (
	"context"
	"encoding/json"
)

// This file wires the package-level helper functions (Search, Hset, etc.)
// onto *GoldLapel as receiver methods. Each method resolves the query
// target (transaction override via WithTx, scoped transaction from InTx,
// or the default *sql.DB opened at Start) and forwards.
//
// Doc<Verb> and Stream<Verb> have moved off *GoldLapel — call them as
// gl.Documents.<Verb>(...) and gl.Streams.<Verb>(...). See documents.go
// and streams.go. This is a hard cut (no aliases): Phase 4 of
// schema-to-core moves doc-store DDL ownership into the proxy, so the
// nested namespaces are the only path that materializes the canonical
// _goldlapel.doc_<name> tables. The old gl.DocX flat methods would have
// run against user-named tables and silently bypassed proxy DDL.

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

// --- Queue ---

func (gl *GoldLapel) Enqueue(ctx context.Context, queueTable string, payload interface{}, opts ...Option) error {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return err
	}
	return Enqueue(ctx, q, queueTable, payload)
}

func (gl *GoldLapel) Dequeue(ctx context.Context, queueTable string, opts ...Option) (json.RawMessage, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return Dequeue(ctx, q, queueTable)
}

// --- Counter ---

func (gl *GoldLapel) Incr(ctx context.Context, table, key string, amount int64, opts ...Option) (int64, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	return Incr(ctx, q, table, key, amount)
}

func (gl *GoldLapel) GetCounter(ctx context.Context, table, key string, opts ...Option) (int64, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	return GetCounter(ctx, q, table, key)
}

// --- Hash ---

func (gl *GoldLapel) Hset(ctx context.Context, table, key, field string, value interface{}, opts ...Option) error {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return err
	}
	return Hset(ctx, q, table, key, field, value)
}

func (gl *GoldLapel) Hget(ctx context.Context, table, key, field string, opts ...Option) (json.RawMessage, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return Hget(ctx, q, table, key, field)
}

func (gl *GoldLapel) Hgetall(ctx context.Context, table, key string, opts ...Option) (json.RawMessage, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return Hgetall(ctx, q, table, key)
}

func (gl *GoldLapel) Hdel(ctx context.Context, table, key, field string, opts ...Option) (bool, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return false, err
	}
	return Hdel(ctx, q, table, key, field)
}

// --- Sorted set ---

func (gl *GoldLapel) Zadd(ctx context.Context, table, member string, score float64, opts ...Option) error {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return err
	}
	return Zadd(ctx, q, table, member, score)
}

func (gl *GoldLapel) Zincrby(ctx context.Context, table, member string, amount float64, opts ...Option) (float64, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	return Zincrby(ctx, q, table, member, amount)
}

func (gl *GoldLapel) Zrange(ctx context.Context, table string, start, stop int, desc bool, opts ...Option) ([]ZMember, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return Zrange(ctx, q, table, start, stop, desc)
}

func (gl *GoldLapel) Zrank(ctx context.Context, table, member string, desc bool, opts ...Option) (*int, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return Zrank(ctx, q, table, member, desc)
}

func (gl *GoldLapel) Zscore(ctx context.Context, table, member string, opts ...Option) (*float64, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return Zscore(ctx, q, table, member)
}

func (gl *GoldLapel) Zrem(ctx context.Context, table, member string, opts ...Option) (bool, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return false, err
	}
	return Zrem(ctx, q, table, member)
}

// --- Geo ---

func (gl *GoldLapel) Georadius(ctx context.Context, table, geomColumn string, lon, lat, radiusMeters float64, limit int, opts ...Option) ([]map[string]interface{}, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return Georadius(ctx, q, table, geomColumn, lon, lat, radiusMeters, limit)
}

func (gl *GoldLapel) Geoadd(ctx context.Context, table, nameColumn, geomColumn, name string, lon, lat float64, opts ...Option) error {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return err
	}
	return Geoadd(ctx, q, table, nameColumn, geomColumn, name, lon, lat)
}

func (gl *GoldLapel) Geodist(ctx context.Context, table, geomColumn, nameColumn, nameA, nameB string, opts ...Option) (*float64, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return Geodist(ctx, q, table, geomColumn, nameColumn, nameA, nameB)
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
