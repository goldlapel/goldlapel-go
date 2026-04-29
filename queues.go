package goldlapel

// Queues namespace API — gl.Queues.<Verb>(ctx, name, ...).
//
// Phase 5 of schema-to-core. The proxy's v1 queue schema is at-least-once
// with visibility-timeout — NOT the legacy fire-and-forget shape. The
// breaking change:
//
//	Before: gl.Dequeue(ctx, "jobs")              // delete-on-fetch (loses work on crash)
//	After:  msg, err := gl.Queues.Claim(ctx, "jobs", 30000)  // lease the row
//	        // ... handle work using msg.ID, msg.Payload ...
//	        gl.Queues.Ack(ctx, "jobs", msg.ID)   // commit; missing Ack → redelivery
//
// Claim returns a `*ClaimedMsg` (nil when the queue is empty) so callers
// distinguish "no message" from "got message". The caller MUST Ack(id) to
// commit, or Abandon(id) to release the lease immediately. A consumer that
// crashes leaves the lease standing; the message becomes ready again after
// `visibilityTimeoutMs` and is redelivered to the next claim.
//
// Hard cut: there is no `Dequeue` shim — `Queues` exposes only the explicit
// claim/ack lifecycle.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Queues is the queues sub-API — accessible as gl.Queues.
type Queues struct {
	gl *GoldLapel
}

// ClaimedMsg is the lease handed back by Claim. Caller MUST follow up with
// Ack(ID) on success or Abandon(ID) on transient failure; otherwise the
// message becomes visible again after the visibility-timeout and is
// redelivered to the next claim.
type ClaimedMsg struct {
	ID      int64
	Payload json.RawMessage
}

// PeekedMsg is the look-only shape returned by Peek. No lease is taken.
type PeekedMsg struct {
	ID        int64
	Payload   json.RawMessage
	VisibleAt time.Time
	Status    string
	CreatedAt time.Time
}

func (qs *Queues) patterns(ctx context.Context, name string) (*DdlEntry, error) {
	if err := validateIdentifier(name); err != nil {
		return nil, err
	}
	return qs.gl.FetchPatterns(ctx, "queue", name)
}

func (qs *Queues) requirePattern(entry *DdlEntry, key, verb string) (string, error) {
	if entry == nil || entry.QueryPatterns == nil {
		return "", fmt.Errorf("queue/%s: proxy returned no query_patterns", verb)
	}
	sqlStr, ok := entry.QueryPatterns[key]
	if !ok {
		return "", fmt.Errorf("queue/%s: missing pattern %q", verb, key)
	}
	return sqlStr, nil
}

// Create eagerly materializes the queue table on the proxy.
func (qs *Queues) Create(ctx context.Context, name string) error {
	_, err := qs.patterns(ctx, name)
	return err
}

// Enqueue appends a message; returns the assigned id.
func (qs *Queues) Enqueue(ctx context.Context, name string, payload interface{}, opts ...Option) (int64, error) {
	entry, err := qs.patterns(ctx, name)
	if err != nil {
		return 0, err
	}
	sqlStr, err := qs.requirePattern(entry, "enqueue", "enqueue")
	if err != nil {
		return 0, err
	}
	q, err := qs.gl.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("queue enqueue marshal: %w", err)
	}
	// Proxy contract: `enqueue` pattern returns (id, created_at). We only
	// need id but must scan the full row shape — database/sql rejects an
	// under-scan with "expected N destination args, got M".
	var id int64
	var createdAt sql.NullTime
	row := q.QueryRowContext(ctx, sqlStr, string(encoded))
	if err := row.Scan(&id, &createdAt); err != nil {
		return 0, fmt.Errorf("queue enqueue %s: %w", name, err)
	}
	return id, nil
}

// Claim leases the next ready message. Returns nil (no message) when the
// queue is empty. Caller MUST call Ack(id) on success or Abandon(id) to
// release the lease early; otherwise redelivery fires after
// visibilityTimeoutMs.
func (qs *Queues) Claim(ctx context.Context, name string, visibilityTimeoutMs int64, opts ...Option) (*ClaimedMsg, error) {
	entry, err := qs.patterns(ctx, name)
	if err != nil {
		return nil, err
	}
	sqlStr, err := qs.requirePattern(entry, "claim", "claim")
	if err != nil {
		return nil, err
	}
	q, err := qs.gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	// Proxy contract: `claim` pattern returns (id, payload, visible_at,
	// created_at). Scan the full row shape; database/sql rejects an under-
	// scan with "expected N destination args, got M".
	//
	// Use []byte (not sql.RawBytes) — the latter isn't allowed on Row.Scan.
	var id int64
	var raw []byte
	var visAt, createdAt sql.NullTime
	row := q.QueryRowContext(ctx, sqlStr, visibilityTimeoutMs)
	if err := row.Scan(&id, &raw, &visAt, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("queue claim %s: %w", name, err)
	}
	return &ClaimedMsg{ID: id, Payload: json.RawMessage(raw)}, nil
}

// Ack marks a claimed message done (DELETEs the row). Returns true if the
// message existed and was removed.
func (qs *Queues) Ack(ctx context.Context, name string, messageID int64, opts ...Option) (bool, error) {
	entry, err := qs.patterns(ctx, name)
	if err != nil {
		return false, err
	}
	sqlStr, err := qs.requirePattern(entry, "ack", "ack")
	if err != nil {
		return false, err
	}
	q, err := qs.gl.resolveExec(opts)
	if err != nil {
		return false, err
	}
	res, err := q.ExecContext(ctx, sqlStr, messageID)
	if err != nil {
		return false, fmt.Errorf("queue ack %s/%d: %w", name, messageID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// Abandon releases a claim immediately so the message is redelivered without
// waiting for the visibility timeout. Returns true if the message existed and
// was a claim (NACK semantics).
func (qs *Queues) Abandon(ctx context.Context, name string, messageID int64, opts ...Option) (bool, error) {
	entry, err := qs.patterns(ctx, name)
	if err != nil {
		return false, err
	}
	sqlStr, err := qs.requirePattern(entry, "nack", "abandon")
	if err != nil {
		return false, err
	}
	q, err := qs.gl.resolveExec(opts)
	if err != nil {
		return false, err
	}
	// nack pattern returns id only if the row was a claim; sql.ErrNoRows
	// means the id didn't match a claimed message (already acked, never
	// claimed, or wrong id). We discard the returned id — its presence is
	// the signal.
	row := q.QueryRowContext(ctx, sqlStr, messageID)
	var returnedID int64
	if err := row.Scan(&returnedID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("queue abandon %s/%d: %w", name, messageID, err)
	}
	return true, nil
}

// Extend pushes the visibility deadline forward by additionalMs. Returns the
// new visible_at timestamp; the bool is false when the id wasn't a claimed
// message.
func (qs *Queues) Extend(ctx context.Context, name string, messageID, additionalMs int64, opts ...Option) (time.Time, bool, error) {
	entry, err := qs.patterns(ctx, name)
	if err != nil {
		return time.Time{}, false, err
	}
	sqlStr, err := qs.requirePattern(entry, "extend", "extend")
	if err != nil {
		return time.Time{}, false, err
	}
	q, err := qs.gl.resolveExec(opts)
	if err != nil {
		return time.Time{}, false, err
	}
	row := q.QueryRowContext(ctx, sqlStr, messageID, additionalMs)
	var t time.Time
	if err := row.Scan(&t); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, fmt.Errorf("queue extend %s/%d: %w", name, messageID, err)
	}
	return t, true, nil
}

// Peek looks at the next-ready message without claiming. Returns nil when
// nothing is ready.
func (qs *Queues) Peek(ctx context.Context, name string, opts ...Option) (*PeekedMsg, error) {
	entry, err := qs.patterns(ctx, name)
	if err != nil {
		return nil, err
	}
	sqlStr, err := qs.requirePattern(entry, "peek", "peek")
	if err != nil {
		return nil, err
	}
	q, err := qs.gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	var msg PeekedMsg
	var raw []byte
	row := q.QueryRowContext(ctx, sqlStr)
	if err := row.Scan(&msg.ID, &raw, &msg.VisibleAt, &msg.Status, &msg.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("queue peek %s: %w", name, err)
	}
	msg.Payload = json.RawMessage(raw)
	return &msg, nil
}

// CountReady returns the number of messages currently ready (status='ready'
// and visible).
func (qs *Queues) CountReady(ctx context.Context, name string, opts ...Option) (int64, error) {
	entry, err := qs.patterns(ctx, name)
	if err != nil {
		return 0, err
	}
	sqlStr, err := qs.requirePattern(entry, "count_ready", "count_ready")
	if err != nil {
		return 0, err
	}
	q, err := qs.gl.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	var out int64
	if err := q.QueryRowContext(ctx, sqlStr).Scan(&out); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("queue count_ready %s: %w", name, err)
	}
	return out, nil
}

// CountClaimed returns the number of currently-claimed messages (in flight).
func (qs *Queues) CountClaimed(ctx context.Context, name string, opts ...Option) (int64, error) {
	entry, err := qs.patterns(ctx, name)
	if err != nil {
		return 0, err
	}
	sqlStr, err := qs.requirePattern(entry, "count_claimed", "count_claimed")
	if err != nil {
		return 0, err
	}
	q, err := qs.gl.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	var out int64
	if err := q.QueryRowContext(ctx, sqlStr).Scan(&out); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("queue count_claimed %s: %w", name, err)
	}
	return out, nil
}
