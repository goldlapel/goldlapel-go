package goldlapel

import (
	"context"
)

// Querier is the interface that CachedConn wraps. It matches the subset of
// pgx.Conn and pgxpool.Pool used for queries. Any type with Query, QueryRow,
// and Exec methods can be wrapped.
type Querier interface {
	Query(ctx context.Context, sql string, args ...interface{}) (Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) Row
	Exec(ctx context.Context, sql string, args ...interface{}) (interface{}, error)
}

// Rows is the interface for iterating query results. Matches pgx.Rows.
type Rows interface {
	Next() bool
	Scan(dest ...interface{}) error
	Close()
	Err() error
	FieldDescriptions() []FieldDescription
	Values() ([]interface{}, error)
	RawValues() [][]byte
}

// Row is the interface for a single-row query. Matches pgx.Row.
type Row interface {
	Scan(dest ...interface{}) error
}

// FieldDescription describes a column in query results.
type FieldDescription struct {
	Name string
}

// CachedConn wraps a Querier (e.g. pgx.Conn) with L1 native cache.
// Reads are served from cache when possible. Writes trigger invalidation.
// Transactions bypass the cache entirely.
type CachedConn struct {
	real          Querier
	cache         *NativeCache
	inTransaction bool
}

// Wrap wraps a Querier with L1 native cache and starts the invalidation
// listener if not already running.
func Wrap(conn Querier, invalidationPort int) *CachedConn {
	cache := GetNativeCache()
	if invalidationPort <= 0 {
		invalidationPort = detectInvalidationPort()
	}
	if !cache.Connected() {
		cache.ConnectInvalidation(invalidationPort)
	}
	return &CachedConn{
		real:  conn,
		cache: cache,
	}
}

func detectInvalidationPort() int {
	singletonMu.Lock()
	defer singletonMu.Unlock()
	if instance != nil {
		port := instance.port
		if instance.config != nil {
			if ip, ok := instance.config["invalidation_port"]; ok {
				return toInt(ip)
			}
		}
		return port + 2
	}
	return DefaultPort + 2
}

// Unwrap returns the underlying Querier.
func (cc *CachedConn) Unwrap() Querier {
	return cc.real
}

// Query intercepts SQL queries. Writes trigger invalidation and are forwarded.
// Reads check the L1 cache first, falling back to the real connection on miss.
func (cc *CachedConn) Query(ctx context.Context, sql string, args ...interface{}) (Rows, error) {
	// Transaction tracking
	if isTxStart(sql) {
		cc.inTransaction = true
		return cc.real.Query(ctx, sql, args...)
	}
	if isTxEnd(sql) {
		cc.inTransaction = false
		return cc.real.Query(ctx, sql, args...)
	}

	// Write detection + self-invalidation
	writeTable := detectWrite(sql)
	if writeTable != "" {
		if writeTable == ddlSentinel {
			cc.cache.InvalidateAll()
		} else {
			cc.cache.InvalidateTable(writeTable)
		}
		return cc.real.Query(ctx, sql, args...)
	}

	// Inside transaction: bypass cache
	if cc.inTransaction {
		return cc.real.Query(ctx, sql, args...)
	}

	// Read path: check L1 cache
	entry := cc.cache.Get(sql, args)
	if entry != nil {
		rows, ok := entry.Rows.([][]interface{})
		if ok {
			var fields []FieldDescription
			if entry.Fields != nil {
				if f, ok2 := entry.Fields.([]FieldDescription); ok2 {
					fields = f
				}
			}
			return &cachedRows{rows: rows, fields: fields, pos: -1}, nil
		}
	}

	// Cache miss: execute for real
	rows, err := cc.real.Query(ctx, sql, args...)
	if err != nil {
		return rows, err
	}

	// Collect rows to cache
	var collected [][]interface{}
	var fields []FieldDescription
	if rows != nil {
		fields = rows.FieldDescriptions()
		for rows.Next() {
			vals, vErr := rows.Values()
			if vErr != nil {
				// Can't cache this result, return the rows as-is
				// (but rows is consumed at this point, so wrap collected + remaining)
				break
			}
			dup := make([]interface{}, len(vals))
			copy(dup, vals)
			collected = append(collected, dup)
		}
		rows.Close()
		if rows.Err() != nil {
			return &cachedRows{rows: collected, fields: fields, pos: -1, err: rows.Err()}, nil
		}
	}

	// Cache the result
	cc.cache.Put(sql, args, collected, fields)

	return &cachedRows{rows: collected, fields: fields, pos: -1}, nil
}

// QueryRow intercepts single-row queries.
func (cc *CachedConn) QueryRow(ctx context.Context, sql string, args ...interface{}) Row {
	// Transaction tracking
	if isTxStart(sql) {
		cc.inTransaction = true
		return cc.real.QueryRow(ctx, sql, args...)
	}
	if isTxEnd(sql) {
		cc.inTransaction = false
		return cc.real.QueryRow(ctx, sql, args...)
	}

	// Write detection
	writeTable := detectWrite(sql)
	if writeTable != "" {
		if writeTable == ddlSentinel {
			cc.cache.InvalidateAll()
		} else {
			cc.cache.InvalidateTable(writeTable)
		}
		return cc.real.QueryRow(ctx, sql, args...)
	}

	// Inside transaction: bypass cache
	if cc.inTransaction {
		return cc.real.QueryRow(ctx, sql, args...)
	}

	// Read path: check L1 cache
	entry := cc.cache.Get(sql, args)
	if entry != nil {
		rows, ok := entry.Rows.([][]interface{})
		if ok && len(rows) > 0 {
			return &cachedRow{values: rows[0]}
		}
		return &cachedRow{values: nil}
	}

	// Cache miss: forward to real
	return cc.real.QueryRow(ctx, sql, args...)
}

// Exec intercepts write commands. It always forwards to the real connection
// and invalidates the cache for any detected writes.
func (cc *CachedConn) Exec(ctx context.Context, sql string, args ...interface{}) (interface{}, error) {
	// Transaction tracking
	if isTxStart(sql) {
		cc.inTransaction = true
		return cc.real.Exec(ctx, sql, args...)
	}
	if isTxEnd(sql) {
		cc.inTransaction = false
		return cc.real.Exec(ctx, sql, args...)
	}

	// Write detection + self-invalidation
	writeTable := detectWrite(sql)
	if writeTable != "" {
		if writeTable == ddlSentinel {
			cc.cache.InvalidateAll()
		} else {
			cc.cache.InvalidateTable(writeTable)
		}
	}

	return cc.real.Exec(ctx, sql, args...)
}

// --- Cached result types ---

type cachedRows struct {
	rows   [][]interface{}
	fields []FieldDescription
	pos    int
	err    error
}

func (cr *cachedRows) Next() bool {
	cr.pos++
	return cr.pos < len(cr.rows)
}

func (cr *cachedRows) Scan(dest ...interface{}) error {
	if cr.pos < 0 || cr.pos >= len(cr.rows) {
		return nil
	}
	row := cr.rows[cr.pos]
	for i := 0; i < len(dest) && i < len(row); i++ {
		if p, ok := dest[i].(*interface{}); ok {
			*p = row[i]
		}
	}
	return nil
}

func (cr *cachedRows) Close() {}

func (cr *cachedRows) Err() error {
	return cr.err
}

func (cr *cachedRows) FieldDescriptions() []FieldDescription {
	return cr.fields
}

func (cr *cachedRows) Values() ([]interface{}, error) {
	if cr.pos < 0 || cr.pos >= len(cr.rows) {
		return nil, nil
	}
	return cr.rows[cr.pos], nil
}

func (cr *cachedRows) RawValues() [][]byte {
	return nil
}

type cachedRow struct {
	values []interface{}
}

func (cr *cachedRow) Scan(dest ...interface{}) error {
	if cr.values == nil {
		return nil
	}
	for i := 0; i < len(dest) && i < len(cr.values); i++ {
		if p, ok := dest[i].(*interface{}); ok {
			*p = cr.values[i]
		}
	}
	return nil
}
