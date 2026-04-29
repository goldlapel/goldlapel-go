package goldlapel

import (
	"context"
	"testing"
)

// Regression: helpers must reject injection-shaped identifier args before
// any I/O fires. See v0.2 security review finding C1.
//
// Phase 5: the legacy flat helpers (Enqueue/Dequeue/Incr/Hset/Zadd/Geoadd/
// ...) are gone — replaced by the gl.Counters / gl.Zsets / gl.Hashes /
// gl.Queues / gl.Geos sub-API namespaces. Each new method calls
// validateIdentifier on the namespace name BEFORE fetching DDL or touching
// the connection, so a malicious name short-circuits at the wrapper layer.

const badIdent = "foo; DROP TABLE users--"

func TestPublish_RejectsBadChannel(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if err := Publish(context.Background(), db, badIdent, "m"); err == nil {
		t.Fatal("expected error for malicious channel")
	}
}

func TestSubscribe_RejectsBadChannel(t *testing.T) {
	// Subscribe takes a DSN string, not a *sql.DB. Validation happens before
	// any connection attempt, so an empty conn string is safe.
	if err := Subscribe(context.Background(), "", badIdent, func(ch, p string) {}); err == nil {
		t.Fatal("expected error for malicious channel")
	}
}

func TestCountDistinct_RejectsBadTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if _, err := CountDistinct(context.Background(), db, badIdent, "col"); err == nil {
		t.Fatal("expected error for malicious table")
	}
}

func TestCountDistinct_RejectsBadColumn(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if _, err := CountDistinct(context.Background(), db, "tbl", badIdent); err == nil {
		t.Fatal("expected error for malicious column")
	}
}

// --- Phase 5 namespace-name validation ---
//
// Each helper namespace validates the name argument before fetching DDL
// or executing SQL. We exercise that via a new gl with no connection — the
// validateIdentifier call returns an error before reaching FetchPatterns.

func TestCounters_RejectsBadName(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	ctx := context.Background()
	if _, err := gl.Counters.Incr(ctx, badIdent, "k", 1); err == nil {
		t.Fatal("Counters.Incr: expected error for malicious name")
	}
	if _, err := gl.Counters.Get(ctx, badIdent, "k"); err == nil {
		t.Fatal("Counters.Get: expected error for malicious name")
	}
	if _, err := gl.Counters.Set(ctx, badIdent, "k", 1); err == nil {
		t.Fatal("Counters.Set: expected error for malicious name")
	}
	if _, err := gl.Counters.Delete(ctx, badIdent, "k"); err == nil {
		t.Fatal("Counters.Delete: expected error for malicious name")
	}
	if _, err := gl.Counters.CountKeys(ctx, badIdent); err == nil {
		t.Fatal("Counters.CountKeys: expected error for malicious name")
	}
}

func TestZsets_RejectsBadName(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	ctx := context.Background()
	if _, err := gl.Zsets.Add(ctx, badIdent, "k", "m", 1.0); err == nil {
		t.Fatal("Zsets.Add: expected error for malicious name")
	}
	if _, err := gl.Zsets.IncrBy(ctx, badIdent, "k", "m", 1.0); err == nil {
		t.Fatal("Zsets.IncrBy: expected error for malicious name")
	}
	if _, _, err := gl.Zsets.Score(ctx, badIdent, "k", "m"); err == nil {
		t.Fatal("Zsets.Score: expected error for malicious name")
	}
	if _, _, err := gl.Zsets.Rank(ctx, badIdent, "k", "m", true); err == nil {
		t.Fatal("Zsets.Rank: expected error for malicious name")
	}
	if _, err := gl.Zsets.Range(ctx, badIdent, "k", 0, 10, true); err == nil {
		t.Fatal("Zsets.Range: expected error for malicious name")
	}
	if _, err := gl.Zsets.Remove(ctx, badIdent, "k", "m"); err == nil {
		t.Fatal("Zsets.Remove: expected error for malicious name")
	}
}

func TestHashes_RejectsBadName(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	ctx := context.Background()
	if _, err := gl.Hashes.Set(ctx, badIdent, "k", "f", "v"); err == nil {
		t.Fatal("Hashes.Set: expected error for malicious name")
	}
	if _, err := gl.Hashes.Get(ctx, badIdent, "k", "f"); err == nil {
		t.Fatal("Hashes.Get: expected error for malicious name")
	}
	if _, err := gl.Hashes.GetAll(ctx, badIdent, "k"); err == nil {
		t.Fatal("Hashes.GetAll: expected error for malicious name")
	}
	if _, err := gl.Hashes.Delete(ctx, badIdent, "k", "f"); err == nil {
		t.Fatal("Hashes.Delete: expected error for malicious name")
	}
}

func TestQueues_RejectsBadName(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	ctx := context.Background()
	if _, err := gl.Queues.Enqueue(ctx, badIdent, map[string]any{}); err == nil {
		t.Fatal("Queues.Enqueue: expected error for malicious name")
	}
	if _, err := gl.Queues.Claim(ctx, badIdent, 30000); err == nil {
		t.Fatal("Queues.Claim: expected error for malicious name")
	}
	if _, err := gl.Queues.Ack(ctx, badIdent, 1); err == nil {
		t.Fatal("Queues.Ack: expected error for malicious name")
	}
}

func TestGeos_RejectsBadName(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	ctx := context.Background()
	if _, err := gl.Geos.Add(ctx, badIdent, "m", 0, 0); err == nil {
		t.Fatal("Geos.Add: expected error for malicious name")
	}
	if _, err := gl.Geos.Radius(ctx, badIdent, 0, 0, 100, "m", 10); err == nil {
		t.Fatal("Geos.Radius: expected error for malicious name")
	}
	if _, _, err := gl.Geos.Dist(ctx, badIdent, "a", "b", "m"); err == nil {
		t.Fatal("Geos.Dist: expected error for malicious name")
	}
}

// --- Stream validation (still flat helper; Phase 5 doesn't move streams) ---

func TestStreamAdd_RejectsBadStream(t *testing.T) {
	if _, err := StreamAdd(context.Background(), nil, badIdent, "{}"); err == nil {
		t.Fatal("expected error for malicious stream")
	}
}

func TestStreamCreateGroup_RejectsBadStream(t *testing.T) {
	if err := StreamCreateGroup(context.Background(), nil, badIdent, "g"); err == nil {
		t.Fatal("expected error for malicious stream")
	}
}

func TestStreamRead_RejectsBadStream(t *testing.T) {
	if _, err := StreamRead(context.Background(), nil, badIdent, "g", "c", 1); err == nil {
		t.Fatal("expected error for malicious stream")
	}
}

func TestStreamAck_RejectsBadStream(t *testing.T) {
	if _, err := StreamAck(context.Background(), nil, badIdent, "g", 1); err == nil {
		t.Fatal("expected error for malicious stream")
	}
}

func TestStreamClaim_RejectsBadStream(t *testing.T) {
	if _, err := StreamClaim(context.Background(), nil, badIdent, "g", "c", 60000); err == nil {
		t.Fatal("expected error for malicious stream")
	}
}
