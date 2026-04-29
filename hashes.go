package goldlapel

// Hashes namespace API — gl.Hashes.<Verb>(ctx, name, hashKey, ...).
//
// Phase 5 of schema-to-core. The proxy's v1 hash schema is row-per-field
// (`hash_key`, `field`, `value`) — NOT the legacy JSONB-blob-per-key shape.
// Every method threads `hashKey` as the first positional arg after the
// namespace `name`. `value` is JSON-encoded so callers can store arbitrary
// structured payloads. GetAll rebuilds the per-key map client-side from the
// row stream.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// Hashes is the hashes sub-API — accessible as gl.Hashes.
type Hashes struct {
	gl *GoldLapel
}

func (h *Hashes) patterns(ctx context.Context, name string) (*DdlEntry, error) {
	if err := validateIdentifier(name); err != nil {
		return nil, err
	}
	return h.gl.FetchPatterns(ctx, "hash", name)
}

func (h *Hashes) requirePattern(entry *DdlEntry, key, verb string) (string, error) {
	if entry == nil || entry.QueryPatterns == nil {
		return "", fmt.Errorf("hash/%s: proxy returned no query_patterns", verb)
	}
	sqlStr, ok := entry.QueryPatterns[key]
	if !ok {
		return "", fmt.Errorf("hash/%s: missing pattern %q", verb, key)
	}
	return sqlStr, nil
}

// Create eagerly materializes the hash table on the proxy.
func (h *Hashes) Create(ctx context.Context, name string) error {
	_, err := h.patterns(ctx, name)
	return err
}

// Set writes a (hashKey, field) row. The value is JSON-encoded. Returns the
// raw JSONB stored — callers can json.Unmarshal it into any shape.
func (h *Hashes) Set(ctx context.Context, name, hashKey, field string, value interface{}, opts ...Option) (json.RawMessage, error) {
	entry, err := h.patterns(ctx, name)
	if err != nil {
		return nil, err
	}
	sqlStr, err := h.requirePattern(entry, "hset", "set")
	if err != nil {
		return nil, err
	}
	q, err := h.gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("hash set marshal: %w", err)
	}
	var raw []byte
	row := q.QueryRowContext(ctx, sqlStr, hashKey, field, string(encoded))
	if err := row.Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("hash set %s/%s/%s: %w", name, hashKey, field, err)
	}
	return json.RawMessage(raw), nil
}

// Get fetches a field's value, or (nil, nil) if (hashKey, field) absent.
func (h *Hashes) Get(ctx context.Context, name, hashKey, field string, opts ...Option) (json.RawMessage, error) {
	entry, err := h.patterns(ctx, name)
	if err != nil {
		return nil, err
	}
	sqlStr, err := h.requirePattern(entry, "hget", "get")
	if err != nil {
		return nil, err
	}
	q, err := h.gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	var raw []byte
	row := q.QueryRowContext(ctx, sqlStr, hashKey, field)
	if err := row.Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("hash get %s/%s/%s: %w", name, hashKey, field, err)
	}
	return json.RawMessage(raw), nil
}

// GetAll reassembles every (field, value) under hashKey into a Go map.
// Empty map (not nil) if the key has no fields. Each value is the raw JSONB
// bytes — callers json.Unmarshal individually.
func (h *Hashes) GetAll(ctx context.Context, name, hashKey string, opts ...Option) (map[string]json.RawMessage, error) {
	entry, err := h.patterns(ctx, name)
	if err != nil {
		return nil, err
	}
	sqlStr, err := h.requirePattern(entry, "hgetall", "get_all")
	if err != nil {
		return nil, err
	}
	q, err := h.gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	rows, err := q.QueryContext(ctx, sqlStr, hashKey)
	if err != nil {
		return nil, fmt.Errorf("hash get_all %s/%s: %w", name, hashKey, err)
	}
	defer rows.Close()
	out := map[string]json.RawMessage{}
	for rows.Next() {
		var field string
		var raw []byte
		if err := rows.Scan(&field, &raw); err != nil {
			return nil, fmt.Errorf("hash get_all scan: %w", err)
		}
		// Copy because raw may be reused by the driver on next iteration.
		copyRaw := make(json.RawMessage, len(raw))
		copy(copyRaw, raw)
		out[field] = copyRaw
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// Keys lists every field name under hashKey.
func (h *Hashes) Keys(ctx context.Context, name, hashKey string, opts ...Option) ([]string, error) {
	entry, err := h.patterns(ctx, name)
	if err != nil {
		return nil, err
	}
	sqlStr, err := h.requirePattern(entry, "hkeys", "keys")
	if err != nil {
		return nil, err
	}
	q, err := h.gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	rows, err := q.QueryContext(ctx, sqlStr, hashKey)
	if err != nil {
		return nil, fmt.Errorf("hash keys %s/%s: %w", name, hashKey, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var field string
		if err := rows.Scan(&field); err != nil {
			return nil, fmt.Errorf("hash keys scan: %w", err)
		}
		out = append(out, field)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// Values lists every value under hashKey (in field-name order).
func (h *Hashes) Values(ctx context.Context, name, hashKey string, opts ...Option) ([]json.RawMessage, error) {
	entry, err := h.patterns(ctx, name)
	if err != nil {
		return nil, err
	}
	sqlStr, err := h.requirePattern(entry, "hvals", "values")
	if err != nil {
		return nil, err
	}
	q, err := h.gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	rows, err := q.QueryContext(ctx, sqlStr, hashKey)
	if err != nil {
		return nil, fmt.Errorf("hash values %s/%s: %w", name, hashKey, err)
	}
	defer rows.Close()
	var out []json.RawMessage
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("hash values scan: %w", err)
		}
		copyRaw := make(json.RawMessage, len(raw))
		copy(copyRaw, raw)
		out = append(out, copyRaw)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// Exists reports whether (hashKey, field) is present.
func (h *Hashes) Exists(ctx context.Context, name, hashKey, field string, opts ...Option) (bool, error) {
	entry, err := h.patterns(ctx, name)
	if err != nil {
		return false, err
	}
	sqlStr, err := h.requirePattern(entry, "hexists", "exists")
	if err != nil {
		return false, err
	}
	q, err := h.gl.resolveExec(opts)
	if err != nil {
		return false, err
	}
	var present bool
	row := q.QueryRowContext(ctx, sqlStr, hashKey, field)
	if err := row.Scan(&present); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("hash exists %s/%s/%s: %w", name, hashKey, field, err)
	}
	return present, nil
}

// Delete drops a (hashKey, field) row; true if deleted, false if absent.
func (h *Hashes) Delete(ctx context.Context, name, hashKey, field string, opts ...Option) (bool, error) {
	entry, err := h.patterns(ctx, name)
	if err != nil {
		return false, err
	}
	sqlStr, err := h.requirePattern(entry, "hdel", "delete")
	if err != nil {
		return false, err
	}
	q, err := h.gl.resolveExec(opts)
	if err != nil {
		return false, err
	}
	res, err := q.ExecContext(ctx, sqlStr, hashKey, field)
	if err != nil {
		return false, fmt.Errorf("hash delete %s/%s/%s: %w", name, hashKey, field, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// Len returns the number of fields under hashKey.
func (h *Hashes) Len(ctx context.Context, name, hashKey string, opts ...Option) (int64, error) {
	entry, err := h.patterns(ctx, name)
	if err != nil {
		return 0, err
	}
	sqlStr, err := h.requirePattern(entry, "hlen", "len")
	if err != nil {
		return 0, err
	}
	q, err := h.gl.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	var out int64
	if err := q.QueryRowContext(ctx, sqlStr, hashKey).Scan(&out); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("hash len %s/%s: %w", name, hashKey, err)
	}
	return out, nil
}
