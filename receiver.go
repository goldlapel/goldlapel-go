package goldlapel

import (
	"context"
	"encoding/json"
)

// This file wires the package-level helper functions (DocInsert, Search,
// Hset, etc.) onto *GoldLapel as receiver methods. Each method resolves
// the query target (transaction override via WithTx, scoped transaction
// from InTx, or the default *sql.DB opened at Start) and forwards.

// --- Document store ---

// DocCreateCollection explicitly creates a collection table. Pass unlogged=true
// for an UNLOGGED table (higher throughput, not crash-safe). Collections are
// auto-created on first insert, so this is only needed to preset unlogged or
// to materialize the table without inserting.
func (gl *GoldLapel) DocCreateCollection(ctx context.Context, collection string, unlogged bool, opts ...Option) error {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return err
	}
	return DocCreateCollection(ctx, q, collection, unlogged)
}

func (gl *GoldLapel) DocInsert(ctx context.Context, collection string, document interface{}, opts ...Option) (map[string]interface{}, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return DocInsert(ctx, q, collection, document)
}

func (gl *GoldLapel) DocInsertMany(ctx context.Context, collection string, documents []interface{}, opts ...Option) ([]map[string]interface{}, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return DocInsertMany(ctx, q, collection, documents)
}

// DocFind queries documents in a collection. Accepts DocSort/DocLimit/DocSkip
// along with generic options such as WithTx for per-call transaction routing.
func (gl *GoldLapel) DocFind(ctx context.Context, collection string, filter interface{}, opts ...Option) ([]map[string]interface{}, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return DocFind(ctx, q, collection, filter, opts...)
}

func (gl *GoldLapel) DocFindOne(ctx context.Context, collection string, filter interface{}, opts ...Option) (map[string]interface{}, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return DocFindOne(ctx, q, collection, filter)
}

func (gl *GoldLapel) DocUpdate(ctx context.Context, collection string, filter, update interface{}, opts ...Option) (int64, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	return DocUpdate(ctx, q, collection, filter, update)
}

func (gl *GoldLapel) DocUpdateOne(ctx context.Context, collection string, filter, update interface{}, opts ...Option) (int64, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	return DocUpdateOne(ctx, q, collection, filter, update)
}

func (gl *GoldLapel) DocDelete(ctx context.Context, collection string, filter interface{}, opts ...Option) (int64, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	return DocDelete(ctx, q, collection, filter)
}

func (gl *GoldLapel) DocDeleteOne(ctx context.Context, collection string, filter interface{}, opts ...Option) (int64, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	return DocDeleteOne(ctx, q, collection, filter)
}

func (gl *GoldLapel) DocCount(ctx context.Context, collection string, filter interface{}, opts ...Option) (int64, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	return DocCount(ctx, q, collection, filter)
}

// DocFindOneAndUpdate atomically finds a single document matching filter,
// merges update into data, and returns the updated document (nil if no match).
func (gl *GoldLapel) DocFindOneAndUpdate(ctx context.Context, collection string, filter, update interface{}, opts ...Option) (map[string]interface{}, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return DocFindOneAndUpdate(ctx, q, collection, filter, update)
}

// DocFindOneAndDelete atomically finds a single document matching filter,
// deletes it, and returns the deleted document (nil if no match).
func (gl *GoldLapel) DocFindOneAndDelete(ctx context.Context, collection string, filter interface{}, opts ...Option) (map[string]interface{}, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return DocFindOneAndDelete(ctx, q, collection, filter)
}

// DocDistinct returns the distinct values of the given JSONB field across
// documents matching filter.
func (gl *GoldLapel) DocDistinct(ctx context.Context, collection, field string, filter interface{}, opts ...Option) ([]interface{}, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return DocDistinct(ctx, q, collection, field, filter)
}

// DocFindCursor streams documents through a callback, fetching in batches.
// Useful for large result sets. Callback returns (continue, error); returning
// false halts cleanly.
func (gl *GoldLapel) DocFindCursor(ctx context.Context, collection string, filter interface{}, callback func(doc map[string]interface{}) (bool, error), opts ...Option) error {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return err
	}
	return DocFindCursor(ctx, q, collection, filter, callback, opts...)
}

func (gl *GoldLapel) DocCreateIndex(ctx context.Context, collection string, keys []string, opts ...Option) error {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return err
	}
	return DocCreateIndex(ctx, q, collection, keys)
}

func (gl *GoldLapel) DocAggregate(ctx context.Context, collection string, pipeline []map[string]interface{}, opts ...Option) ([]map[string]interface{}, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return DocAggregate(ctx, q, collection, pipeline)
}

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

// --- Stream ---

func (gl *GoldLapel) StreamAdd(ctx context.Context, stream string, payload string, opts ...Option) (int64, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	return StreamAdd(ctx, q, stream, payload)
}

func (gl *GoldLapel) StreamCreateGroup(ctx context.Context, stream, group string, opts ...Option) error {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return err
	}
	return StreamCreateGroup(ctx, q, stream, group)
}

func (gl *GoldLapel) StreamRead(ctx context.Context, stream, group, consumer string, count int, opts ...Option) ([]StreamMessage, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return StreamRead(ctx, q, stream, group, consumer, count)
}

func (gl *GoldLapel) StreamAck(ctx context.Context, stream, group string, messageID int64, opts ...Option) (bool, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return false, err
	}
	return StreamAck(ctx, q, stream, group, messageID)
}

func (gl *GoldLapel) StreamClaim(ctx context.Context, stream, group, consumer string, minIdleMs int64, opts ...Option) ([]StreamMessage, error) {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return StreamClaim(ctx, q, stream, group, consumer, minIdleMs)
}

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

// --- Operational Doc methods ---

func (gl *GoldLapel) DocWatch(ctx context.Context, collection string, callback func(op, data string)) (chan error, error) {
	q, err := gl.resolveExec(nil)
	if err != nil {
		return nil, err
	}
	gl.mu.Lock()
	url := gl.proxyURL
	gl.mu.Unlock()
	if url == "" {
		return nil, ErrNotConnected
	}
	return DocWatch(ctx, q, url, collection, callback)
}

func (gl *GoldLapel) DocUnwatch(ctx context.Context, collection string, opts ...Option) error {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return err
	}
	return DocUnwatch(ctx, q, collection)
}

func (gl *GoldLapel) DocCreateTtlIndex(ctx context.Context, collection string, ttlSeconds int, opts ...Option) error {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return err
	}
	return DocCreateTtlIndex(ctx, q, collection, ttlSeconds)
}

func (gl *GoldLapel) DocRemoveTtlIndex(ctx context.Context, collection string, opts ...Option) error {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return err
	}
	return DocRemoveTtlIndex(ctx, q, collection)
}

func (gl *GoldLapel) DocCreateCapped(ctx context.Context, collection string, maxDocs int, opts ...Option) error {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return err
	}
	return DocCreateCapped(ctx, q, collection, maxDocs)
}

func (gl *GoldLapel) DocRemoveCap(ctx context.Context, collection string, opts ...Option) error {
	q, err := gl.resolveExec(opts)
	if err != nil {
		return err
	}
	return DocRemoveCap(ctx, q, collection)
}
