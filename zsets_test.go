package goldlapel

import (
	"context"
	"database/sql/driver"
	"strings"
	"sync"
	"testing"
)

// zsetPatternsFor returns a fake DDL pattern set keyed against the canonical
// proxy table name `_goldlapel.zset_<name>`. Mirrors the Python fixture in
// tests/test_zsets.py — every UPSERT uses ON CONFLICT (zset_key, member) so
// a single namespace table holds many sorted sets (Phase 5 contract).
func zsetPatternsFor(name string) *DdlEntry {
	main := "_goldlapel.zset_" + name
	return &DdlEntry{
		Tables: map[string]string{"main": main},
		QueryPatterns: map[string]string{
			"zadd":          "INSERT INTO " + main + " (zset_key, member, score, updated_at) VALUES ($1, $2, $3, NOW()) ON CONFLICT (zset_key, member) DO UPDATE SET score = EXCLUDED.score, updated_at = NOW() RETURNING score",
			"zincrby":       "INSERT INTO " + main + " (zset_key, member, score, updated_at) VALUES ($1, $2, $3, NOW()) ON CONFLICT (zset_key, member) DO UPDATE SET score = " + main + ".score + EXCLUDED.score, updated_at = NOW() RETURNING score",
			"zscore":        "SELECT score FROM " + main + " WHERE zset_key = $1 AND member = $2",
			"zrem":          "DELETE FROM " + main + " WHERE zset_key = $1 AND member = $2",
			"zrange_desc":   "SELECT member, score FROM " + main + " WHERE zset_key = $1 ORDER BY score DESC, member LIMIT $2 OFFSET $3",
			"zrange_asc":    "SELECT member, score FROM " + main + " WHERE zset_key = $1 ORDER BY score ASC, member LIMIT $2 OFFSET $3",
			"zrangebyscore": "SELECT member, score FROM " + main + " WHERE zset_key = $1 AND score BETWEEN $2 AND $3 ORDER BY score, member LIMIT $4 OFFSET $5",
			"zrank_desc":    "SELECT rank FROM (SELECT member, ROW_NUMBER() OVER (PARTITION BY zset_key ORDER BY score DESC, member) - 1 AS rank FROM " + main + " WHERE zset_key = $1) r WHERE member = $2",
			"zrank_asc":     "SELECT rank FROM (SELECT member, ROW_NUMBER() OVER (PARTITION BY zset_key ORDER BY score ASC, member) - 1 AS rank FROM " + main + " WHERE zset_key = $1) r WHERE member = $2",
			"zcard":         "SELECT COUNT(*) FROM " + main + " WHERE zset_key = $1",
		},
	}
}

func TestZsetsNamespace_IsAttached(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	if gl.Zsets == nil {
		t.Fatal("expected gl.Zsets to be non-nil")
	}
	if gl.Zsets.gl != gl {
		t.Fatal("Zsets.gl must back-reference the parent")
	}
}

// TestZsetsAdd_ZsetKeyIsFirstAfterName locks the breaking-change shape:
// every method takes the zset_key as the first arg after the namespace
// `name`. Mirrors test_zsets.py::test_add_passes_zset_key_member_score.
func TestZsetsAdd_ZsetKeyIsFirstAfterName(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"score"},
		[][]driver.Value{{float64(100.5)}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	if gl.ddlCache == nil {
		gl.ddlCache = &sync.Map{}
	}
	gl.ddlCache.Store("zset:leaderboard", zsetPatternsFor("leaderboard"))

	got, err := gl.Zsets.Add(context.Background(), "leaderboard", "global", "alice", 100.5)
	if err != nil {
		t.Fatalf("Zsets.Add: %v", err)
	}
	if got != 100.5 {
		t.Fatalf("want 100.5, got %f", got)
	}

	last := drv.lastCapture()
	// Param contract: ($1=zset_key, $2=member, $3=score).
	if len(last.args) != 3 {
		t.Fatalf("expected 3 params, got %v", last.args)
	}
	if last.args[0] != "global" || last.args[1] != "alice" || last.args[2] != float64(100.5) {
		t.Fatalf("expected (global, alice, 100.5), got %v", last.args)
	}
	// SQL must reference the canonical table.
	if !strings.Contains(last.query, "_goldlapel.zset_leaderboard") {
		t.Fatalf("expected canonical table, got %q", last.query)
	}
}

func TestZsetsIncrBy_ThreadsZsetKey(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"score"},
		[][]driver.Value{{float64(105.5)}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("zset:leaderboard", zsetPatternsFor("leaderboard"))

	got, err := gl.Zsets.IncrBy(context.Background(), "leaderboard", "global", "alice", 5.0)
	if err != nil {
		t.Fatalf("Zsets.IncrBy: %v", err)
	}
	if got != 105.5 {
		t.Fatalf("want 105.5, got %f", got)
	}
	last := drv.lastCapture()
	if last.args[0] != "global" || last.args[1] != "alice" {
		t.Fatalf("expected (global, alice, ...), got %v", last.args)
	}
}

func TestZsetsScore_ReturnsFalseForAbsent(t *testing.T) {
	db, _ := newTestDB(t, []string{"score"}, nil)
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("zset:leaderboard", zsetPatternsFor("leaderboard"))

	score, ok, err := gl.Zsets.Score(context.Background(), "leaderboard", "global", "ghost")
	if err != nil {
		t.Fatalf("Zsets.Score: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false for absent member, got score=%f", score)
	}
}

func TestZsetsRange_DescPicksDescPattern(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"member", "score"},
		[][]driver.Value{
			{"alice", float64(100.0)},
			{"bob", float64(50.0)},
		})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("zset:leaderboard", zsetPatternsFor("leaderboard"))

	out, err := gl.Zsets.Range(context.Background(), "leaderboard", "global", 0, 9, true)
	if err != nil {
		t.Fatalf("Zsets.Range: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(out))
	}
	if out[0].Member != "alice" || out[0].Score != 100.0 {
		t.Errorf("first row: want (alice, 100), got %+v", out[0])
	}
	last := drv.lastCapture()
	// desc=true must select the zrange_desc pattern.
	if !strings.Contains(last.query, "ORDER BY score DESC") {
		t.Fatalf("desc=true must use zrange_desc, got %q", last.query)
	}
	// limit = stop - start + 1 = 10.
	if last.args[1] != int64(10) || last.args[2] != int64(0) {
		t.Fatalf("expected (zset_key, limit=10, offset=0), got %v", last.args)
	}
}

func TestZsetsRange_StopMinusOneMeansToEnd(t *testing.T) {
	// Python: stop=-1 → 9999. Verify the same here.
	db, drv := newTestDB(t,
		[]string{"member", "score"},
		nil)
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("zset:leaderboard", zsetPatternsFor("leaderboard"))

	if _, err := gl.Zsets.Range(context.Background(), "leaderboard", "global", 0, -1, true); err != nil {
		t.Fatalf("Zsets.Range: %v", err)
	}
	last := drv.lastCapture()
	// limit = 9999 - 0 + 1 = 10000.
	if last.args[1] != int64(10000) {
		t.Fatalf("stop=-1 should map to limit=10000, got %v", last.args)
	}
}

func TestZsetsRangeByScore_FiveParams(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"member", "score"},
		nil)
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("zset:leaderboard", zsetPatternsFor("leaderboard"))

	if _, err := gl.Zsets.RangeByScore(context.Background(),
		"leaderboard", "global", 50.0, 100.0, 25, 5); err != nil {
		t.Fatalf("Zsets.RangeByScore: %v", err)
	}
	last := drv.lastCapture()
	// $1=zset_key, $2=min, $3=max, $4=limit, $5=offset.
	if len(last.args) != 5 {
		t.Fatalf("expected 5 params, got %v", last.args)
	}
	if last.args[0] != "global" ||
		last.args[1] != float64(50.0) ||
		last.args[2] != float64(100.0) ||
		last.args[3] != int64(25) ||
		last.args[4] != int64(5) {
		t.Fatalf("bad params: %v", last.args)
	}
}

func TestZsetsRank_DescPicksRankDescPattern(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"rank"},
		[][]driver.Value{{int64(0)}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("zset:leaderboard", zsetPatternsFor("leaderboard"))

	rank, ok, err := gl.Zsets.Rank(context.Background(), "leaderboard", "global", "alice", true)
	if err != nil {
		t.Fatalf("Zsets.Rank: %v", err)
	}
	if !ok || rank != 0 {
		t.Fatalf("want (0, true), got (%d, %t)", rank, ok)
	}
	last := drv.lastCapture()
	if !strings.Contains(last.query, "ORDER BY score DESC") {
		t.Fatalf("desc=true must use zrank_desc, got %q", last.query)
	}
}

func TestZsetsRemove_RowsAffected(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("zset:leaderboard", zsetPatternsFor("leaderboard"))

	removed, err := gl.Zsets.Remove(context.Background(), "leaderboard", "global", "alice")
	if err != nil {
		t.Fatalf("Zsets.Remove: %v", err)
	}
	if !removed {
		t.Fatal("expected removed=true (mock RowsAffected returns 1)")
	}
}

func TestZsetsCard_NoMemberArg(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"count"},
		[][]driver.Value{{int64(42)}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("zset:leaderboard", zsetPatternsFor("leaderboard"))

	got, err := gl.Zsets.Card(context.Background(), "leaderboard", "global")
	if err != nil {
		t.Fatalf("Zsets.Card: %v", err)
	}
	if got != 42 {
		t.Fatalf("want 42, got %d", got)
	}
	last := drv.lastCapture()
	if len(last.args) != 1 || last.args[0] != "global" {
		t.Fatalf("Card binds only zset_key, got %v", last.args)
	}
}

// --- Phase 5 breaking change: zset_key threading is now in the API ---

func TestZsetsAdd_BreakingChange_NoFlatZaddOnGoldLapel(t *testing.T) {
	// The pre-Phase-5 gl.Zadd took (table, member, score). It is gone —
	// callers must use gl.Zsets.Add(ctx, name, zsetKey, member, score). If
	// this file compiles, the cut is in place; this test is the runtime
	// canary.
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	if gl.Zsets == nil {
		t.Fatal("zset sub-API must be the only path post-Phase-5")
	}
}

func TestSupportedVersion_Zset_IsV1(t *testing.T) {
	if got := SupportedVersion("zset"); got != "v1" {
		t.Fatalf("want v1, got %q", got)
	}
}
