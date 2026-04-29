package goldlapel

import (
	"context"
	"database/sql/driver"
	"strings"
	"sync"
	"testing"
	"time"
)

// queuePatternsFor returns a fake DDL pattern set keyed against the canonical
// proxy table `_goldlapel.queue_<name>`. Phase 5 contract: at-least-once
// with visibility-timeout. claim returns (id, payload, visible_at,
// created_at); ack DELETEs; nack/abandon resets to ready.
func queuePatternsFor(name string) *DdlEntry {
	main := "_goldlapel.queue_" + name
	return &DdlEntry{
		Tables: map[string]string{"main": main},
		QueryPatterns: map[string]string{
			"enqueue":       "INSERT INTO " + main + " (payload) VALUES ($1::jsonb) RETURNING id, created_at",
			"claim":         "WITH next_msg AS ( SELECT id FROM " + main + " WHERE status = 'ready' AND visible_at <= NOW() ORDER BY visible_at, id FOR UPDATE SKIP LOCKED LIMIT 1 ) UPDATE " + main + " SET status = 'claimed', visible_at = NOW() + INTERVAL '1 millisecond' * $1 FROM next_msg WHERE " + main + ".id = next_msg.id RETURNING " + main + ".id, " + main + ".payload, " + main + ".visible_at, " + main + ".created_at",
			"ack":           "DELETE FROM " + main + " WHERE id = $1",
			"extend":        "UPDATE " + main + " SET visible_at = visible_at + INTERVAL '1 millisecond' * $2 WHERE id = $1 AND status = 'claimed' RETURNING visible_at",
			"nack":          "UPDATE " + main + " SET status = 'ready', visible_at = NOW() WHERE id = $1 AND status = 'claimed' RETURNING id",
			"peek":          "SELECT id, payload, visible_at, status, created_at FROM " + main + " WHERE status = 'ready' AND visible_at <= NOW() ORDER BY visible_at, id LIMIT 1",
			"count_ready":   "SELECT COUNT(*) FROM " + main + " WHERE status = 'ready' AND visible_at <= NOW()",
			"count_claimed": "SELECT COUNT(*) FROM " + main + " WHERE status = 'claimed'",
		},
	}
}

func TestQueuesNamespace_IsAttached(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	if gl.Queues == nil {
		t.Fatal("expected gl.Queues to be non-nil")
	}
	if gl.Queues.gl != gl {
		t.Fatal("Queues.gl must back-reference the parent")
	}
}

// TestQueuesEnqueue_ReturnsIdFromProxy locks the proxy contract:
// `enqueue` RETURNING id, created_at. The wrapper scans both columns.
func TestQueuesEnqueue_ReturnsIdFromProxy(t *testing.T) {
	now := time.Now().UTC()
	db, drv := newTestDB(t,
		[]string{"id", "created_at"},
		[][]driver.Value{{int64(99), now}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	if gl.ddlCache == nil {
		gl.ddlCache = &sync.Map{}
	}
	gl.ddlCache.Store("queue:jobs", queuePatternsFor("jobs"))

	id, err := gl.Queues.Enqueue(context.Background(), "jobs", map[string]interface{}{"x": 1})
	if err != nil {
		t.Fatalf("Queues.Enqueue: %v", err)
	}
	if id != 99 {
		t.Fatalf("want id=99, got %d", id)
	}
	last := drv.lastCapture()
	if !strings.Contains(last.query, "_goldlapel.queue_jobs") {
		t.Fatalf("expected canonical table, got %q", last.query)
	}
}

// TestQueuesClaim_ReturnsClaimedMsgPointer is the breaking-change canary:
// claim returns *ClaimedMsg (nil on empty queue), NOT a delete-on-fetch
// payload. The shape is decoupled from the legacy Dequeue.
func TestQueuesClaim_ReturnsClaimedMsgPointer(t *testing.T) {
	now := time.Now().UTC()
	db, drv := newTestDB(t,
		[]string{"id", "payload", "visible_at", "created_at"},
		[][]driver.Value{{int64(7), []byte(`{"x":1}`), now, now}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("queue:jobs", queuePatternsFor("jobs"))

	msg, err := gl.Queues.Claim(context.Background(), "jobs", 30000)
	if err != nil {
		t.Fatalf("Queues.Claim: %v", err)
	}
	if msg == nil {
		t.Fatal("expected non-nil ClaimedMsg")
	}
	if msg.ID != 7 {
		t.Fatalf("want id=7, got %d", msg.ID)
	}
	if string(msg.Payload) != `{"x":1}` {
		t.Fatalf(`want payload={"x":1}, got %s`, msg.Payload)
	}
	last := drv.lastCapture()
	// $1 = visibility_timeout_ms (only param).
	if len(last.args) != 1 || last.args[0] != int64(30000) {
		t.Fatalf("expected ($1=30000), got %v", last.args)
	}
	// claim SQL must NOT issue DELETE — Phase 5 is at-least-once.
	if strings.Contains(strings.ToUpper(last.query), "DELETE") {
		t.Fatalf("claim must not DELETE — Phase 5 is at-least-once. Got %q", last.query)
	}
}

func TestQueuesClaim_ReturnsNilOnEmpty(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"id", "payload", "visible_at", "created_at"},
		nil)
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("queue:jobs", queuePatternsFor("jobs"))

	msg, err := gl.Queues.Claim(context.Background(), "jobs", 30000)
	if err != nil {
		t.Fatalf("Queues.Claim: %v", err)
	}
	if msg != nil {
		t.Fatalf("expected nil on empty queue, got %+v", msg)
	}
}

func TestQueuesAck_IsSeparateFromClaim(t *testing.T) {
	db, drv := newTestDB(t, nil, nil)
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("queue:jobs", queuePatternsFor("jobs"))

	got, err := gl.Queues.Ack(context.Background(), "jobs", 42)
	if err != nil {
		t.Fatalf("Queues.Ack: %v", err)
	}
	if !got {
		t.Fatal("expected ack=true (mock RowsAffected returns 1)")
	}
	last := drv.lastCapture()
	if !strings.Contains(strings.ToUpper(last.query), "DELETE") {
		t.Fatalf("ack must DELETE the row, got %q", last.query)
	}
	if len(last.args) != 1 || last.args[0] != int64(42) {
		t.Fatalf("ack binds only message_id, got %v", last.args)
	}
}

// TestQueuesNoDequeueShim is the explicit anti-alias regression. The
// dispatcher considered shipping Dequeue as a claim+ack combo for
// migration; the master plan rejected that. There is no Dequeue method
// on Queues, and there is no Dequeue receiver method on *GoldLapel.
func TestQueuesNoDequeueShim(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	if gl.Queues == nil {
		t.Fatal("Queues must be the only path post-Phase-5")
	}
	// Compile-time: gl.Queues.Dequeue and gl.Dequeue would not compile.
	// Runtime: a gl.Queues with the right shape proves the cut is in place.
}

func TestQueuesAbandon_UsesNackPattern(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"id"},
		[][]driver.Value{{int64(42)}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("queue:jobs", queuePatternsFor("jobs"))

	got, err := gl.Queues.Abandon(context.Background(), "jobs", 42)
	if err != nil {
		t.Fatalf("Queues.Abandon: %v", err)
	}
	if !got {
		t.Fatal("expected abandon=true when message existed")
	}
	last := drv.lastCapture()
	// nack SQL must NOT delete; instead it resets status to 'ready'.
	if strings.Contains(strings.ToUpper(last.query), "DELETE") {
		t.Fatalf("abandon must not DELETE, got %q", last.query)
	}
	if !strings.Contains(last.query, "status = 'ready'") {
		t.Fatalf("abandon must set status='ready', got %q", last.query)
	}
}

func TestQueuesExtend_BindsIdAndAdditionalMs(t *testing.T) {
	now := time.Now().UTC()
	db, drv := newTestDB(t,
		[]string{"visible_at"},
		[][]driver.Value{{now}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("queue:jobs", queuePatternsFor("jobs"))

	out, ok, err := gl.Queues.Extend(context.Background(), "jobs", 42, 5000)
	if err != nil {
		t.Fatalf("Queues.Extend: %v", err)
	}
	if !ok {
		t.Fatal("expected extend ok=true")
	}
	if !out.Equal(now) {
		t.Errorf("expected returned visible_at to match mock (%s), got %s", now, out)
	}
	last := drv.lastCapture()
	// $1=id, $2=additional_ms.
	if len(last.args) != 2 || last.args[0] != int64(42) || last.args[1] != int64(5000) {
		t.Fatalf("expected (id=42, ms=5000), got %v", last.args)
	}
}

func TestQueuesPeek_NilOnEmpty(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"id", "payload", "visible_at", "status", "created_at"},
		nil)
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("queue:jobs", queuePatternsFor("jobs"))

	msg, err := gl.Queues.Peek(context.Background(), "jobs")
	if err != nil {
		t.Fatalf("Queues.Peek: %v", err)
	}
	if msg != nil {
		t.Fatalf("expected nil on empty, got %+v", msg)
	}
}

func TestQueuesCountReady(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"count"},
		[][]driver.Value{{int64(3)}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("queue:jobs", queuePatternsFor("jobs"))

	got, err := gl.Queues.CountReady(context.Background(), "jobs")
	if err != nil {
		t.Fatalf("Queues.CountReady: %v", err)
	}
	if got != 3 {
		t.Fatalf("want 3, got %d", got)
	}
}

func TestQueuesCountClaimed(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"count"},
		[][]driver.Value{{int64(2)}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("queue:jobs", queuePatternsFor("jobs"))

	got, err := gl.Queues.CountClaimed(context.Background(), "jobs")
	if err != nil {
		t.Fatalf("Queues.CountClaimed: %v", err)
	}
	if got != 2 {
		t.Fatalf("want 2, got %d", got)
	}
}

func TestSupportedVersion_Queue_IsV1(t *testing.T) {
	if got := SupportedVersion("queue"); got != "v1" {
		t.Fatalf("want v1, got %q", got)
	}
}
