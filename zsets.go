package goldlapel

// Zsets (sorted-set) namespace API — gl.Zsets.<Verb>(ctx, name, zsetKey, ...).
//
// Phase 5 of schema-to-core. The proxy's v1 zset schema introduces a
// `zset_key` column so a single namespace table holds many sorted sets —
// matching Redis's mental model. Every method here threads `zsetKey` as the
// first positional arg after the namespace `name`. This is the breaking
// change: pre-Phase-5 callers passed (table, member, score); Phase 5 callers
// pass (name, zsetKey, member, score) — no aliases.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Zsets is the sorted-set sub-API — accessible as gl.Zsets.
type Zsets struct {
	gl *GoldLapel
}

// ZMember is one (member, score) pair returned by Range / RangeByScore.
type ZMember struct {
	Member string
	Score  float64
}

func (z *Zsets) patterns(ctx context.Context, name string) (*DdlEntry, error) {
	if err := validateIdentifier(name); err != nil {
		return nil, err
	}
	return z.gl.FetchPatterns(ctx, "zset", name)
}

func (z *Zsets) requirePattern(entry *DdlEntry, key, verb string) (string, error) {
	if entry == nil || entry.QueryPatterns == nil {
		return "", fmt.Errorf("zset/%s: proxy returned no query_patterns", verb)
	}
	sqlStr, ok := entry.QueryPatterns[key]
	if !ok {
		return "", fmt.Errorf("zset/%s: missing pattern %q", verb, key)
	}
	return sqlStr, nil
}

// Create eagerly materializes the zset table on the proxy.
func (z *Zsets) Create(ctx context.Context, name string) error {
	_, err := z.patterns(ctx, name)
	return err
}

// Add sets-or-updates a member's score under zsetKey; returns the new score.
func (z *Zsets) Add(ctx context.Context, name, zsetKey, member string, score float64, opts ...Option) (float64, error) {
	entry, err := z.patterns(ctx, name)
	if err != nil {
		return 0, err
	}
	sqlStr, err := z.requirePattern(entry, "zadd", "add")
	if err != nil {
		return 0, err
	}
	q, err := z.gl.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	var out float64
	if err := q.QueryRowContext(ctx, sqlStr, zsetKey, member, score).Scan(&out); err != nil {
		return 0, fmt.Errorf("zset add %s/%s/%s: %w", name, zsetKey, member, err)
	}
	return out, nil
}

// IncrBy atomically increment-or-inserts a member's score; returns the new
// score.
func (z *Zsets) IncrBy(ctx context.Context, name, zsetKey, member string, delta float64, opts ...Option) (float64, error) {
	entry, err := z.patterns(ctx, name)
	if err != nil {
		return 0, err
	}
	sqlStr, err := z.requirePattern(entry, "zincrby", "incr_by")
	if err != nil {
		return 0, err
	}
	q, err := z.gl.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	var out float64
	if err := q.QueryRowContext(ctx, sqlStr, zsetKey, member, delta).Scan(&out); err != nil {
		return 0, fmt.Errorf("zset incr_by %s/%s/%s: %w", name, zsetKey, member, err)
	}
	return out, nil
}

// Score returns a member's score; the bool is false when the member is
// absent (matches Python's `None` shape via Go's two-value pattern).
func (z *Zsets) Score(ctx context.Context, name, zsetKey, member string, opts ...Option) (float64, bool, error) {
	entry, err := z.patterns(ctx, name)
	if err != nil {
		return 0, false, err
	}
	sqlStr, err := z.requirePattern(entry, "zscore", "score")
	if err != nil {
		return 0, false, err
	}
	q, err := z.gl.resolveExec(opts)
	if err != nil {
		return 0, false, err
	}
	var out float64
	row := q.QueryRowContext(ctx, sqlStr, zsetKey, member)
	if err := row.Scan(&out); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("zset score %s/%s/%s: %w", name, zsetKey, member, err)
	}
	return out, true, nil
}

// Rank returns the 0-based rank of a member within zsetKey. The bool is
// false when the member is absent.
func (z *Zsets) Rank(ctx context.Context, name, zsetKey, member string, desc bool, opts ...Option) (int64, bool, error) {
	entry, err := z.patterns(ctx, name)
	if err != nil {
		return 0, false, err
	}
	key := "zrank_desc"
	if !desc {
		key = "zrank_asc"
	}
	sqlStr, err := z.requirePattern(entry, key, "rank")
	if err != nil {
		return 0, false, err
	}
	q, err := z.gl.resolveExec(opts)
	if err != nil {
		return 0, false, err
	}
	var out int64
	row := q.QueryRowContext(ctx, sqlStr, zsetKey, member)
	if err := row.Scan(&out); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("zset rank %s/%s/%s: %w", name, zsetKey, member, err)
	}
	return out, true, nil
}

// Range returns members by rank within zsetKey. start/stop are 0-based
// inclusive bounds Redis-style; the SQL converts to LIMIT/OFFSET. Pass
// stop=-1 to mean "to the end" (mapped to a large limit, as in Python).
func (z *Zsets) Range(ctx context.Context, name, zsetKey string, start, stop int, desc bool, opts ...Option) ([]ZMember, error) {
	if stop == -1 {
		stop = 9999
	}
	entry, err := z.patterns(ctx, name)
	if err != nil {
		return nil, err
	}
	key := "zrange_desc"
	if !desc {
		key = "zrange_asc"
	}
	sqlStr, err := z.requirePattern(entry, key, "range")
	if err != nil {
		return nil, err
	}
	q, err := z.gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	limit := stop - start + 1
	if limit < 0 {
		limit = 0
	}
	rows, err := q.QueryContext(ctx, sqlStr, zsetKey, limit, start)
	if err != nil {
		return nil, fmt.Errorf("zset range %s/%s: %w", name, zsetKey, err)
	}
	defer rows.Close()
	var out []ZMember
	for rows.Next() {
		var m ZMember
		if err := rows.Scan(&m.Member, &m.Score); err != nil {
			return nil, fmt.Errorf("zset range scan: %w", err)
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// RangeByScore returns members whose score is between minScore/maxScore
// (inclusive).
func (z *Zsets) RangeByScore(ctx context.Context, name, zsetKey string, minScore, maxScore float64, limit, offset int, opts ...Option) ([]ZMember, error) {
	entry, err := z.patterns(ctx, name)
	if err != nil {
		return nil, err
	}
	sqlStr, err := z.requirePattern(entry, "zrangebyscore", "range_by_score")
	if err != nil {
		return nil, err
	}
	q, err := z.gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	rows, err := q.QueryContext(ctx, sqlStr, zsetKey, minScore, maxScore, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("zset range_by_score %s/%s: %w", name, zsetKey, err)
	}
	defer rows.Close()
	var out []ZMember
	for rows.Next() {
		var m ZMember
		if err := rows.Scan(&m.Member, &m.Score); err != nil {
			return nil, fmt.Errorf("zset range_by_score scan: %w", err)
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// Remove removes a member from zsetKey; true if removed, false if absent.
func (z *Zsets) Remove(ctx context.Context, name, zsetKey, member string, opts ...Option) (bool, error) {
	entry, err := z.patterns(ctx, name)
	if err != nil {
		return false, err
	}
	sqlStr, err := z.requirePattern(entry, "zrem", "remove")
	if err != nil {
		return false, err
	}
	q, err := z.gl.resolveExec(opts)
	if err != nil {
		return false, err
	}
	res, err := q.ExecContext(ctx, sqlStr, zsetKey, member)
	if err != nil {
		return false, fmt.Errorf("zset remove %s/%s/%s: %w", name, zsetKey, member, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// Card returns the cardinality (member count) of one zsetKey.
func (z *Zsets) Card(ctx context.Context, name, zsetKey string, opts ...Option) (int64, error) {
	entry, err := z.patterns(ctx, name)
	if err != nil {
		return 0, err
	}
	sqlStr, err := z.requirePattern(entry, "zcard", "card")
	if err != nil {
		return 0, err
	}
	q, err := z.gl.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	var out int64
	if err := q.QueryRowContext(ctx, sqlStr, zsetKey).Scan(&out); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("zset card %s/%s: %w", name, zsetKey, err)
	}
	return out, nil
}
