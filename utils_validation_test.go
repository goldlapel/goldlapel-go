package goldlapel

import (
	"context"
	"testing"
)

// Regression: Redis-compat helpers must reject injection-shaped identifier args.
// See v0.2 security review finding C1.

const badIdent = "foo; DROP TABLE users--"

func TestPublish_RejectsBadChannel(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if err := Publish(context.Background(), db, badIdent, "m"); err == nil {
		t.Fatal("expected error for malicious channel")
	}
}

func TestEnqueue_RejectsBadTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if err := Enqueue(context.Background(), db, badIdent, map[string]any{}); err == nil {
		t.Fatal("expected error for malicious queue table")
	}
}

func TestDequeue_RejectsBadTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if _, err := Dequeue(context.Background(), db, badIdent); err == nil {
		t.Fatal("expected error for malicious queue table")
	}
}

func TestIncr_RejectsBadTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if _, err := Incr(context.Background(), db, badIdent, "k", 1); err == nil {
		t.Fatal("expected error for malicious table")
	}
}

func TestGetCounter_RejectsBadTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if _, err := GetCounter(context.Background(), db, badIdent, "k"); err == nil {
		t.Fatal("expected error for malicious table")
	}
}

func TestZadd_RejectsBadTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if err := Zadd(context.Background(), db, badIdent, "m", 1.0); err == nil {
		t.Fatal("expected error for malicious table")
	}
}

func TestZincrby_RejectsBadTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if _, err := Zincrby(context.Background(), db, badIdent, "m", 1.0); err == nil {
		t.Fatal("expected error for malicious table")
	}
}

func TestZrange_RejectsBadTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if _, err := Zrange(context.Background(), db, badIdent, 0, 10, true); err == nil {
		t.Fatal("expected error for malicious table")
	}
}

func TestZrank_RejectsBadTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if _, err := Zrank(context.Background(), db, badIdent, "m", true); err == nil {
		t.Fatal("expected error for malicious table")
	}
}

func TestZscore_RejectsBadTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if _, err := Zscore(context.Background(), db, badIdent, "m"); err == nil {
		t.Fatal("expected error for malicious table")
	}
}

func TestZrem_RejectsBadTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if _, err := Zrem(context.Background(), db, badIdent, "m"); err == nil {
		t.Fatal("expected error for malicious table")
	}
}

func TestHset_RejectsBadTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if err := Hset(context.Background(), db, badIdent, "k", "f", "v"); err == nil {
		t.Fatal("expected error for malicious table")
	}
}

func TestHget_RejectsBadTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if _, err := Hget(context.Background(), db, badIdent, "k", "f"); err == nil {
		t.Fatal("expected error for malicious table")
	}
}

func TestHgetall_RejectsBadTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if _, err := Hgetall(context.Background(), db, badIdent, "k"); err == nil {
		t.Fatal("expected error for malicious table")
	}
}

func TestHdel_RejectsBadTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if _, err := Hdel(context.Background(), db, badIdent, "k", "f"); err == nil {
		t.Fatal("expected error for malicious table")
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

func TestGeoadd_RejectsBadTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if err := Geoadd(context.Background(), db, badIdent, "name", "geom", "x", 0, 0); err == nil {
		t.Fatal("expected error for malicious table")
	}
}

func TestGeoadd_RejectsBadNameColumn(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if err := Geoadd(context.Background(), db, "tbl", badIdent, "geom", "x", 0, 0); err == nil {
		t.Fatal("expected error for malicious name column")
	}
}

func TestGeoadd_RejectsBadGeomColumn(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if err := Geoadd(context.Background(), db, "tbl", "name", badIdent, "x", 0, 0); err == nil {
		t.Fatal("expected error for malicious geom column")
	}
}

func TestGeoradius_RejectsBadTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if _, err := Georadius(context.Background(), db, badIdent, "geom", 0, 0, 100, 10); err == nil {
		t.Fatal("expected error for malicious table")
	}
}

func TestGeoradius_RejectsBadGeomColumn(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if _, err := Georadius(context.Background(), db, "tbl", badIdent, 0, 0, 100, 10); err == nil {
		t.Fatal("expected error for malicious geom column")
	}
}

func TestGeodist_RejectsBadTable(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if _, err := Geodist(context.Background(), db, badIdent, "geom", "name", "a", "b"); err == nil {
		t.Fatal("expected error for malicious table")
	}
}

func TestStreamAdd_RejectsBadStream(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if _, err := StreamAdd(context.Background(), db, badIdent, "{}"); err == nil {
		t.Fatal("expected error for malicious stream")
	}
}

func TestStreamCreateGroup_RejectsBadStream(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if err := StreamCreateGroup(context.Background(), db, badIdent, "g"); err == nil {
		t.Fatal("expected error for malicious stream")
	}
}

func TestStreamRead_RejectsBadStream(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if _, err := StreamRead(context.Background(), db, badIdent, "g", "c", 1); err == nil {
		t.Fatal("expected error for malicious stream")
	}
}

func TestStreamAck_RejectsBadStream(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if _, err := StreamAck(context.Background(), db, badIdent, "g", 1); err == nil {
		t.Fatal("expected error for malicious stream")
	}
}

func TestStreamClaim_RejectsBadStream(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	if _, err := StreamClaim(context.Background(), db, badIdent, "g", "c", 60000); err == nil {
		t.Fatal("expected error for malicious stream")
	}
}

func TestSubscribe_RejectsBadChannel(t *testing.T) {
	// Subscribe takes a DSN string, not a *sql.DB. validation happens before
	// any connection attempt, so an empty conn string is safe.
	if err := Subscribe(context.Background(), "", badIdent, func(ch, p string) {}); err == nil {
		t.Fatal("expected error for malicious channel")
	}
}
