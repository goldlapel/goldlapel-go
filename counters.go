package goldlapel

// Counters namespace API — gl.Counters.<Verb>(...).
//
// Phase 5 of schema-to-core: the proxy owns counter DDL. Each call here:
//
//   1. POSTs /api/ddl/counter/create (idempotent) to materialize the
//      canonical _goldlapel.counter_<name> table and pull its query
//      patterns.
//   2. Caches {tables, query_patterns} on the parent GoldLapel instance for
//      the session's lifetime (one HTTP round-trip per (family, name)).
//   3. Executes the proxy's canonical patterns verbatim — pgx/lib/pq accept
//      $N natively so no translation is needed.
//
// Phase 5 contract reminder: every UPSERT pattern stamps `updated_at = NOW()`.
// The wrapper does not paper over this — if the proxy returns a pattern
// missing the timestamp, calls execute it as-is and the column won't drift.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Counters is the counters sub-API — accessible as gl.Counters. State
// (dashboard token, dashboard port, db, DDL pattern cache) is shared via
// the parent GoldLapel reference held in c.gl.
type Counters struct {
	gl *GoldLapel
}

func (c *Counters) patterns(ctx context.Context, name string) (*DdlEntry, error) {
	if err := validateIdentifier(name); err != nil {
		return nil, err
	}
	return c.gl.FetchPatterns(ctx, "counter", name)
}

func (c *Counters) requirePattern(entry *DdlEntry, key, verb string) (string, error) {
	if entry == nil || entry.QueryPatterns == nil {
		return "", fmt.Errorf("counter/%s: proxy returned no query_patterns", verb)
	}
	sqlStr, ok := entry.QueryPatterns[key]
	if !ok {
		return "", fmt.Errorf("counter/%s: missing pattern %q", verb, key)
	}
	return sqlStr, nil
}

// --- Lifecycle ---

// Create eagerly materializes the counter table on the proxy. Other methods
// will also materialize on first use, so calling this is optional.
func (c *Counters) Create(ctx context.Context, name string) error {
	_, err := c.patterns(ctx, name)
	return err
}

// --- Per-key ops ---

// Incr increment-or-inserts a counter; returns the new value.
func (c *Counters) Incr(ctx context.Context, name, key string, amount int64, opts ...Option) (int64, error) {
	entry, err := c.patterns(ctx, name)
	if err != nil {
		return 0, err
	}
	sqlStr, err := c.requirePattern(entry, "incr", "incr")
	if err != nil {
		return 0, err
	}
	q, err := c.gl.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	var out int64
	if err := q.QueryRowContext(ctx, sqlStr, key, amount).Scan(&out); err != nil {
		return 0, fmt.Errorf("counter incr %s/%s: %w", name, key, err)
	}
	return out, nil
}

// Decr decrement-or-inserts; provided as a separate method so callers don't
// need to remember the sign convention. Equivalent to Incr(-amount).
func (c *Counters) Decr(ctx context.Context, name, key string, amount int64, opts ...Option) (int64, error) {
	return c.Incr(ctx, name, key, -amount, opts...)
}

// Set idempotently writes a counter value; returns the value just stored.
func (c *Counters) Set(ctx context.Context, name, key string, value int64, opts ...Option) (int64, error) {
	entry, err := c.patterns(ctx, name)
	if err != nil {
		return 0, err
	}
	sqlStr, err := c.requirePattern(entry, "set", "set")
	if err != nil {
		return 0, err
	}
	q, err := c.gl.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	var out int64
	if err := q.QueryRowContext(ctx, sqlStr, key, value).Scan(&out); err != nil {
		return 0, fmt.Errorf("counter set %s/%s: %w", name, key, err)
	}
	return out, nil
}

// Get fetches a counter's current value. Returns 0 for unknown keys (Redis
// convention — no NULL surprise on cold cache).
func (c *Counters) Get(ctx context.Context, name, key string, opts ...Option) (int64, error) {
	entry, err := c.patterns(ctx, name)
	if err != nil {
		return 0, err
	}
	sqlStr, err := c.requirePattern(entry, "get", "get")
	if err != nil {
		return 0, err
	}
	q, err := c.gl.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	var out int64
	row := q.QueryRowContext(ctx, sqlStr, key)
	if err := row.Scan(&out); err != nil {
		// sql.ErrNoRows → unknown key → 0 (matches Python).
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("counter get %s/%s: %w", name, key, err)
	}
	return out, nil
}

// Delete drops a counter row. Returns true if a row was deleted, false if
// the key was already absent.
func (c *Counters) Delete(ctx context.Context, name, key string, opts ...Option) (bool, error) {
	entry, err := c.patterns(ctx, name)
	if err != nil {
		return false, err
	}
	sqlStr, err := c.requirePattern(entry, "delete", "delete")
	if err != nil {
		return false, err
	}
	q, err := c.gl.resolveExec(opts)
	if err != nil {
		return false, err
	}
	res, err := q.ExecContext(ctx, sqlStr, key)
	if err != nil {
		return false, fmt.Errorf("counter delete %s/%s: %w", name, key, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// CountKeys returns the total number of distinct keys in the counter
// namespace.
func (c *Counters) CountKeys(ctx context.Context, name string, opts ...Option) (int64, error) {
	entry, err := c.patterns(ctx, name)
	if err != nil {
		return 0, err
	}
	sqlStr, err := c.requirePattern(entry, "count_keys", "count_keys")
	if err != nil {
		return 0, err
	}
	q, err := c.gl.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	var out int64
	if err := q.QueryRowContext(ctx, sqlStr).Scan(&out); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("counter count_keys %s: %w", name, err)
	}
	return out, nil
}
