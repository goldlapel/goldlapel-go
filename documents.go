package goldlapel

// Documents namespace API — gl.Documents.<Verb>(...).
//
// Wraps the doc-store methods in a sub-API instance held on the parent
// GoldLapel client. The instance shares all state (license, dashboard token,
// http session, db, ddl cache) by reference back to the parent — no
// duplication.
//
// The proxy owns doc-store DDL (Phase 4 of schema-to-core). Each call here:
//
//   1. POSTs /api/ddl/doc_store/create (idempotent) to materialize the
//      canonical _goldlapel.doc_<name> table and pull its query patterns.
//   2. Caches {tables, query_patterns} on the parent GoldLapel instance for
//      the session's lifetime (one HTTP round-trip per (family, name)).
//   3. Hands the patterned table name off to the existing doc_*On helpers
//      so they execute against the canonical table instead of CREATE-ing
//      their own.
//
// Sub-API class shape mirrors *Streams — this is the canonical pattern for
// the wrapper rollout. Other namespaces (cache, search, queues, counters,
// hashes, zsets, geo, auth, …) stay flat for now; they migrate to nested
// form one-at-a-time as their own schema-to-core phase fires.

import (
	"context"
	"fmt"
)

// Documents is the documents sub-API — accessible as gl.Documents.
//
// All methods take the collection name as the first argument; remaining
// args mirror the legacy gl.Doc<Verb> signatures. State (dashboard token,
// dashboard port, internal db, DDL pattern cache) is shared via the
// parent GoldLapel reference held in d.gl. We never copy lifecycle state
// here — always read through d.gl so a config change on the parent
// (e.g. proxy restart with a new dashboard token) is reflected on the
// next call.
type Documents struct {
	gl *GoldLapel
}

// docOptions holds creation-time options for a doc_store family. Mirrors
// the Python `_patterns(unlogged=...)` keyword.
type docOptions struct {
	unlogged bool
}

// DocOption customises a documents-namespace call. Only used by
// CreateCollection today; pass-through to the proxy's doc_store options
// payload.
type DocOption func(*docOptions)

// DocUnlogged toggles UNLOGGED storage for the canonical doc-store table.
// Only honoured on the call that materializes the table — once it exists,
// its storage type is fixed (the wrapper does not migrate). Pass through
// CreateCollection on first use:
//
//	gl.Documents.CreateCollection(ctx, "sessions", goldlapel.DocUnlogged(true))
func DocUnlogged(unlogged bool) DocOption {
	return func(o *docOptions) { o.unlogged = unlogged }
}

// patterns fetches (and caches) the canonical doc-store DDL + query
// patterns from the proxy. Cache lives on the parent GoldLapel instance.
//
// Per-family options like unlogged=true are passed only on the first call
// for a given (family, name); the proxy's CREATE TABLE IF NOT EXISTS
// makes subsequent calls no-op DDL-wise. If a caller flips unlogged
// across calls in the same session, the table's storage type is
// whatever it was on first create.
func (d *Documents) patterns(ctx context.Context, collection string, opts ...DocOption) (*DdlEntry, error) {
	if err := validateIdentifier(collection); err != nil {
		return nil, err
	}
	o := docOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	var ddlOpts []DDLOption
	if o.unlogged {
		ddlOpts = append(ddlOpts, WithDDLOptions(map[string]interface{}{
			"unlogged": true,
		}))
	}
	return d.gl.FetchPatterns(ctx, "doc_store", collection, ddlOpts...)
}

// resolveTable runs patterns + extracts the canonical Postgres table name
// from tables.main. Returns an error if the proxy response is missing the
// expected key (defensive: shouldn't happen for a healthy proxy).
func (d *Documents) resolveTable(ctx context.Context, collection string, opts ...DocOption) (string, error) {
	entry, err := d.patterns(ctx, collection, opts...)
	if err != nil {
		return "", err
	}
	if entry == nil || entry.Tables == nil {
		return "", fmt.Errorf("doc_store/%s: proxy returned no tables map", collection)
	}
	tableName, ok := entry.Tables["main"]
	if !ok || tableName == "" {
		return "", fmt.Errorf("doc_store/%s: proxy returned no 'main' table", collection)
	}
	return tableName, nil
}

// resolveExec routes the call to the active query target — honours InTx,
// WithTx, or falls back to the instance pool. Mirrors the existing
// resolveExec on GoldLapel.
func (d *Documents) resolveExec(opts []Option) (execQuerier, error) {
	return d.gl.resolveExec(opts)
}

// --- Collection lifecycle ---

// CreateCollection eagerly materializes the doc-store table on the proxy.
// Other methods will also materialize on first use, so calling this is
// optional — provided for callers that want explicit setup at startup
// time, or to control the unlogged option.
func (d *Documents) CreateCollection(ctx context.Context, collection string, opts ...DocOption) error {
	_, err := d.patterns(ctx, collection, opts...)
	return err
}

// --- CRUD ---

// Insert inserts a single document into a collection. Like MongoDB's insertOne().
func (d *Documents) Insert(ctx context.Context, collection string, document interface{}, opts ...Option) (map[string]interface{}, error) {
	tableName, err := d.resolveTable(ctx, collection)
	if err != nil {
		return nil, err
	}
	q, err := d.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return docInsertOn(ctx, q, tableName, document)
}

// InsertMany inserts multiple documents. Like MongoDB's insertMany().
func (d *Documents) InsertMany(ctx context.Context, collection string, documents []interface{}, opts ...Option) ([]map[string]interface{}, error) {
	tableName, err := d.resolveTable(ctx, collection)
	if err != nil {
		return nil, err
	}
	q, err := d.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return docInsertManyOn(ctx, q, tableName, documents)
}

// Find queries documents from a collection. Like MongoDB's find().
// Accepts DocSort/DocLimit/DocSkip alongside generic options like WithTx.
func (d *Documents) Find(ctx context.Context, collection string, filter interface{}, opts ...Option) ([]map[string]interface{}, error) {
	tableName, err := d.resolveTable(ctx, collection)
	if err != nil {
		return nil, err
	}
	q, err := d.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return docFindOn(ctx, q, tableName, filter, opts...)
}

// FindOne queries a single document. Like MongoDB's findOne().
func (d *Documents) FindOne(ctx context.Context, collection string, filter interface{}, opts ...Option) (map[string]interface{}, error) {
	tableName, err := d.resolveTable(ctx, collection)
	if err != nil {
		return nil, err
	}
	q, err := d.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return docFindOneOn(ctx, q, tableName, filter)
}

// FindCursor streams documents through a callback. Like MongoDB's find().cursor().
func (d *Documents) FindCursor(ctx context.Context, collection string, filter interface{}, callback func(doc map[string]interface{}) (bool, error), opts ...Option) error {
	tableName, err := d.resolveTable(ctx, collection)
	if err != nil {
		return err
	}
	q, err := d.resolveExec(opts)
	if err != nil {
		return err
	}
	return docFindCursorOn(ctx, q, tableName, filter, callback, opts...)
}

// Update updates all documents matching filter. Like MongoDB's updateMany().
func (d *Documents) Update(ctx context.Context, collection string, filter, update interface{}, opts ...Option) (int64, error) {
	tableName, err := d.resolveTable(ctx, collection)
	if err != nil {
		return 0, err
	}
	q, err := d.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	return docUpdateOn(ctx, q, tableName, filter, update)
}

// UpdateOne updates a single document. Like MongoDB's updateOne().
func (d *Documents) UpdateOne(ctx context.Context, collection string, filter, update interface{}, opts ...Option) (int64, error) {
	tableName, err := d.resolveTable(ctx, collection)
	if err != nil {
		return 0, err
	}
	q, err := d.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	return docUpdateOneOn(ctx, q, tableName, filter, update)
}

// Delete deletes all documents matching filter. Like MongoDB's deleteMany().
func (d *Documents) Delete(ctx context.Context, collection string, filter interface{}, opts ...Option) (int64, error) {
	tableName, err := d.resolveTable(ctx, collection)
	if err != nil {
		return 0, err
	}
	q, err := d.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	return docDeleteOn(ctx, q, tableName, filter)
}

// DeleteOne deletes a single document. Like MongoDB's deleteOne().
func (d *Documents) DeleteOne(ctx context.Context, collection string, filter interface{}, opts ...Option) (int64, error) {
	tableName, err := d.resolveTable(ctx, collection)
	if err != nil {
		return 0, err
	}
	q, err := d.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	return docDeleteOneOn(ctx, q, tableName, filter)
}

// FindOneAndUpdate atomically finds + updates a single document. Like MongoDB's findOneAndUpdate().
func (d *Documents) FindOneAndUpdate(ctx context.Context, collection string, filter, update interface{}, opts ...Option) (map[string]interface{}, error) {
	tableName, err := d.resolveTable(ctx, collection)
	if err != nil {
		return nil, err
	}
	q, err := d.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return docFindOneAndUpdateOn(ctx, q, tableName, filter, update)
}

// FindOneAndDelete atomically finds + deletes a single document.
func (d *Documents) FindOneAndDelete(ctx context.Context, collection string, filter interface{}, opts ...Option) (map[string]interface{}, error) {
	tableName, err := d.resolveTable(ctx, collection)
	if err != nil {
		return nil, err
	}
	q, err := d.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return docFindOneAndDeleteOn(ctx, q, tableName, filter)
}

// Distinct returns the distinct values of a JSONB field. Like MongoDB's distinct().
func (d *Documents) Distinct(ctx context.Context, collection, field string, filter interface{}, opts ...Option) ([]interface{}, error) {
	tableName, err := d.resolveTable(ctx, collection)
	if err != nil {
		return nil, err
	}
	q, err := d.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return docDistinctOn(ctx, q, tableName, field, filter)
}

// Count counts documents matching filter. Like MongoDB's countDocuments().
func (d *Documents) Count(ctx context.Context, collection string, filter interface{}, opts ...Option) (int64, error) {
	tableName, err := d.resolveTable(ctx, collection)
	if err != nil {
		return 0, err
	}
	q, err := d.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	return docCountOn(ctx, q, tableName, filter)
}

// CreateIndex creates a GIN index on JSONB keys. Pass keys as a slice so
// trailing variadic options (e.g. WithTx) remain available.
func (d *Documents) CreateIndex(ctx context.Context, collection string, keys []string, opts ...Option) error {
	tableName, err := d.resolveTable(ctx, collection)
	if err != nil {
		return err
	}
	q, err := d.resolveExec(opts)
	if err != nil {
		return err
	}
	return docCreateIndexOn(ctx, q, tableName, collection, keys)
}

// Aggregate runs a Mongo-style aggregation pipeline.
//
// $lookup.from references are resolved to their canonical proxy tables
// (_goldlapel.doc_<name>) — each unique from-collection in the pipeline
// triggers an idempotent describe/create against the proxy and is cached
// for the session.
func (d *Documents) Aggregate(ctx context.Context, collection string, pipeline []map[string]interface{}, opts ...Option) ([]map[string]interface{}, error) {
	tableName, err := d.resolveTable(ctx, collection)
	if err != nil {
		return nil, err
	}
	// Walk the pipeline once to find every $lookup.from collection, fetch
	// patterns for each (cached after first call), and pass the resolved
	// map down to docAggregateOn.
	lookupTables := map[string]string{}
	for _, stage := range pipeline {
		spec, ok := stage["$lookup"]
		if !ok {
			continue
		}
		specMap, ok := spec.(map[string]interface{})
		if !ok {
			continue
		}
		fromVal, ok := specMap["from"]
		if !ok {
			continue
		}
		fromName, ok := fromVal.(string)
		if !ok || fromName == "" {
			continue
		}
		if _, alreadyResolved := lookupTables[fromName]; alreadyResolved {
			continue
		}
		fromTable, err := d.resolveTable(ctx, fromName)
		if err != nil {
			return nil, fmt.Errorf("$lookup: resolve from %q: %w", fromName, err)
		}
		lookupTables[fromName] = fromTable
	}
	q, err := d.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	return docAggregateOn(ctx, q, tableName, pipeline, lookupTables)
}

// --- Watch / TTL / capped ---

// Watch listens for changes on a collection. Returns a channel that is
// closed once LISTEN succeeds, or receives an error if setup fails. The
// goroutine exits on ctx cancel.
func (d *Documents) Watch(ctx context.Context, collection string, callback func(op, data string)) (chan error, error) {
	tableName, err := d.resolveTable(ctx, collection)
	if err != nil {
		return nil, err
	}
	q, err := d.resolveExec(nil)
	if err != nil {
		return nil, err
	}
	d.gl.mu.Lock()
	url := d.gl.proxyURL
	d.gl.mu.Unlock()
	if url == "" {
		return nil, ErrNotConnected
	}
	return docWatchOn(ctx, q, url, tableName, collection, callback)
}

// Unwatch removes the change-stream trigger and function.
func (d *Documents) Unwatch(ctx context.Context, collection string, opts ...Option) error {
	tableName, err := d.resolveTable(ctx, collection)
	if err != nil {
		return err
	}
	q, err := d.resolveExec(opts)
	if err != nil {
		return err
	}
	return docUnwatchOn(ctx, q, tableName, collection)
}

// CreateTtlIndex installs a TTL trigger that prunes rows older than ttlSeconds.
func (d *Documents) CreateTtlIndex(ctx context.Context, collection string, ttlSeconds int, opts ...Option) error {
	tableName, err := d.resolveTable(ctx, collection)
	if err != nil {
		return err
	}
	q, err := d.resolveExec(opts)
	if err != nil {
		return err
	}
	return docCreateTtlIndexOn(ctx, q, tableName, collection, ttlSeconds)
}

// RemoveTtlIndex removes the TTL trigger / function.
func (d *Documents) RemoveTtlIndex(ctx context.Context, collection string, opts ...Option) error {
	tableName, err := d.resolveTable(ctx, collection)
	if err != nil {
		return err
	}
	q, err := d.resolveExec(opts)
	if err != nil {
		return err
	}
	return docRemoveTtlIndexOn(ctx, q, tableName, collection)
}

// CreateCapped installs a capped-collection trigger that keeps the N most
// recent rows.
func (d *Documents) CreateCapped(ctx context.Context, collection string, maxDocs int, opts ...Option) error {
	tableName, err := d.resolveTable(ctx, collection)
	if err != nil {
		return err
	}
	q, err := d.resolveExec(opts)
	if err != nil {
		return err
	}
	return docCreateCappedOn(ctx, q, tableName, collection, maxDocs)
}

// RemoveCap removes the capped-collection trigger / function.
func (d *Documents) RemoveCap(ctx context.Context, collection string, opts ...Option) error {
	tableName, err := d.resolveTable(ctx, collection)
	if err != nil {
		return err
	}
	q, err := d.resolveExec(opts)
	if err != nil {
		return err
	}
	return docRemoveCapOn(ctx, q, tableName, collection)
}
