package goldlapel

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
)

// Publish sends a message to a channel. Like redis.publish().
func Publish(ctx context.Context, q execQuerier, channel, message string) error {
	if err := validateIdentifier(channel); err != nil {
		return err
	}
	_, err := q.ExecContext(ctx, "SELECT pg_notify($1, $2)", channel, message)
	return err
}

// Subscribe listens for messages on a channel. Like redis.subscribe().
// This blocks forever, calling the callback for each received message.
// Pass a connection string (DSN), not a *sql.DB, because LISTEN requires
// a dedicated connection. The context cancels the loop.
func Subscribe(ctx context.Context, conn string, channel string, callback func(channel, payload string)) error {
	if err := validateIdentifier(channel); err != nil {
		return err
	}
	minReconn := 10 * time.Second
	maxReconn := time.Minute
	listener := pq.NewListener(conn, minReconn, maxReconn, nil)
	defer listener.Close()

	if err := listener.Listen(channel); err != nil {
		return fmt.Errorf("listen on channel %q: %w", channel, err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case n := <-listener.Notify:
			if n == nil {
				continue
			}
			callback(n.Channel, n.Extra)
		}
	}
}

// SubscribeAsync listens for messages on a channel in a background goroutine.
// Returns a channel that receives an error on initial LISTEN failure or ctx
// cancellation, otherwise stays open until the context is cancelled.
func SubscribeAsync(ctx context.Context, conn string, channel string, callback func(channel, payload string)) chan error {
	errCh := make(chan error, 1)
	go func() {
		if err := Subscribe(ctx, conn, channel, callback); err != nil {
			errCh <- err
		}
	}()
	return errCh
}

// CountDistinct counts the number of distinct values in a column.
func CountDistinct(ctx context.Context, q execQuerier, table, column string) (int64, error) {
	if err := validateIdentifier(table); err != nil {
		return 0, err
	}
	if err := validateIdentifier(column); err != nil {
		return 0, err
	}
	var count int64
	err := q.QueryRowContext(ctx, "SELECT COUNT(DISTINCT "+column+") FROM "+table).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// StreamMessage represents a message from a stream.
type StreamMessage struct {
	ID        int64
	Payload   string
	CreatedAt string
}

// StreamAdd adds a message to a stream. Like redis.xadd().
//
// The Stream* helpers need a *GoldLapel (not a bare execQuerier) because they
// fetch canonical DDL + query patterns from the proxy's /api/ddl/* endpoint
// on first call. Patterns are cached per-instance; subsequent calls reuse
// the cache. Queries run against the GoldLapel's execQuerier (either its
// internal *sql.DB or, inside InTx, the scoped *sql.Tx). Callers who opened
// their own *sql.DB can register it via gl.UseDB(db) before calling.
func StreamAdd(ctx context.Context, gl *GoldLapel, stream string, payload string) (int64, error) {
	if err := validateIdentifier(stream); err != nil {
		return 0, err
	}
	qp, err := gl.streamPatterns(ctx, stream)
	if err != nil {
		return 0, err
	}
	sql, err := requireStreamPattern(qp, "insert", "StreamAdd")
	if err != nil {
		return 0, err
	}
	// JSONB binding: cast at SQL site.
	sql = strings.Replace(sql, "VALUES ($1)", "VALUES ($1::jsonb)", 1)
	q, err := gl.requireExec()
	if err != nil {
		return 0, err
	}
	var id int64
	var createdAt time.Time
	if err := q.QueryRowContext(ctx, sql, payload).Scan(&id, &createdAt); err != nil {
		return 0, err
	}
	_ = createdAt // matches canonical RETURNING; returned for symmetry
	return id, nil
}

// StreamCreateGroup creates a consumer group for a stream. Like redis.xgroup_create().
func StreamCreateGroup(ctx context.Context, gl *GoldLapel, stream, group string) error {
	if err := validateIdentifier(stream); err != nil {
		return err
	}
	qp, err := gl.streamPatterns(ctx, stream)
	if err != nil {
		return err
	}
	sql, err := requireStreamPattern(qp, "create_group", "StreamCreateGroup")
	if err != nil {
		return err
	}
	q, eqErr := gl.requireExec()
	if eqErr != nil {
		return eqErr
	}
	_, err = q.ExecContext(ctx, sql, group)
	return err
}

// StreamRead reads messages from a stream for a consumer group. Like redis.xreadgroup().
//
// StreamRead runs the cursor read → advance → pending insert sequence inside
// an explicit transaction so that the FOR UPDATE lock from the cursor SELECT
// is held across the whole claim flow. Under autocommit the lock would be
// released as soon as the SELECT returns, letting concurrent consumers read
// the same cursor and claim duplicate messages.
//
// If the instance is already scoped to a caller-owned transaction (via
// InTx/WithTx), we run inside that transaction and let the caller manage
// commit/rollback. Otherwise we begin our own tx on the pool for the
// duration of this call.
func StreamRead(ctx context.Context, gl *GoldLapel, stream, group, consumer string, count int) ([]StreamMessage, error) {
	if err := validateIdentifier(stream); err != nil {
		return nil, err
	}
	qp, err := gl.streamPatterns(ctx, stream)
	if err != nil {
		return nil, err
	}
	cursorSQL, err := requireStreamPattern(qp, "group_get_cursor", "StreamRead")
	if err != nil {
		return nil, err
	}
	readSQL, err := requireStreamPattern(qp, "read_since", "StreamRead")
	if err != nil {
		return nil, err
	}
	advanceSQL, err := requireStreamPattern(qp, "group_advance_cursor", "StreamRead")
	if err != nil {
		return nil, err
	}
	pendingSQL, err := requireStreamPattern(qp, "pending_insert", "StreamRead")
	if err != nil {
		return nil, err
	}

	// If the caller is already inside a tx (InTx/WithTx) we run in it.
	// Otherwise we open a private tx on the pool just for this call.
	gl.mu.Lock()
	scopedTx := gl.tx
	db := gl.db
	gl.mu.Unlock()

	var q execQuerier
	var ownTx *sql.Tx
	if scopedTx != nil {
		q = scopedTx
	} else {
		if db == nil {
			return nil, ErrNotConnected
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		ownTx = tx
		q = tx
	}

	// commitOrRollback is a no-op when the caller owns the tx.
	commit := func() error {
		if ownTx != nil {
			return ownTx.Commit()
		}
		return nil
	}
	rollback := func() {
		if ownTx != nil {
			_ = ownTx.Rollback()
		}
	}

	var lastID int64
	if err := q.QueryRowContext(ctx, cursorSQL, group).Scan(&lastID); err != nil {
		if err == sql.ErrNoRows {
			if cerr := commit(); cerr != nil {
				return nil, cerr
			}
			return nil, nil
		}
		rollback()
		return nil, err
	}

	rows, err := q.QueryContext(ctx, readSQL, lastID, count)
	if err != nil {
		rollback()
		return nil, err
	}
	var msgs []StreamMessage
	for rows.Next() {
		var m StreamMessage
		if err := rows.Scan(&m.ID, &m.Payload, &m.CreatedAt); err != nil {
			rows.Close()
			rollback()
			return nil, err
		}
		msgs = append(msgs, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		rollback()
		return nil, err
	}

	if len(msgs) > 0 {
		maxID := msgs[len(msgs)-1].ID
		if _, err := q.ExecContext(ctx, advanceSQL, maxID, group); err != nil {
			rollback()
			return nil, err
		}
		for _, m := range msgs {
			if _, err := q.ExecContext(ctx, pendingSQL, m.ID, group, consumer); err != nil {
				rollback()
				return nil, err
			}
		}
	}
	if err := commit(); err != nil {
		return nil, err
	}
	return msgs, nil
}

// StreamAck acknowledges a message in a consumer group. Like redis.xack().
func StreamAck(ctx context.Context, gl *GoldLapel, stream, group string, messageID int64) (bool, error) {
	if err := validateIdentifier(stream); err != nil {
		return false, err
	}
	qp, err := gl.streamPatterns(ctx, stream)
	if err != nil {
		return false, err
	}
	sqlStr, err := requireStreamPattern(qp, "ack", "StreamAck")
	if err != nil {
		return false, err
	}
	q, eqErr := gl.requireExec()
	if eqErr != nil {
		return false, eqErr
	}
	result, err := q.ExecContext(ctx, sqlStr, group, messageID)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// StreamClaim claims idle messages from other consumers. Like redis.xclaim().
func StreamClaim(ctx context.Context, gl *GoldLapel, stream, group, consumer string, minIdleMs int64) ([]StreamMessage, error) {
	if err := validateIdentifier(stream); err != nil {
		return nil, err
	}
	qp, err := gl.streamPatterns(ctx, stream)
	if err != nil {
		return nil, err
	}
	claimSQL, err := requireStreamPattern(qp, "claim", "StreamClaim")
	if err != nil {
		return nil, err
	}
	readByIDSQL, err := requireStreamPattern(qp, "read_by_id", "StreamClaim")
	if err != nil {
		return nil, err
	}

	q, eqErr := gl.requireExec()
	if eqErr != nil {
		return nil, eqErr
	}
	rows, err := q.QueryContext(ctx, claimSQL, consumer, group, minIdleMs)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var msgs []StreamMessage
	for _, id := range ids {
		var m StreamMessage
		if err := q.QueryRowContext(ctx, readByIDSQL, id).Scan(&m.ID, &m.Payload, &m.CreatedAt); err != nil {
			if err == sql.ErrNoRows {
				continue
			}
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, nil
}

// Script executes Lua code on PostgreSQL via the pllua extension.
func Script(ctx context.Context, q execQuerier, luaCode string, args ...string) (*string, error) {
	_, err := q.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS pllua")
	if err != nil {
		return nil, err
	}

	// crypto/rand for SQL identifier collision resistance: 8 bytes hex = 16
	// chars of entropy, well above the birthday bound for concurrent Script
	// calls. math/rand would share an unseeded PRNG across goroutines.
	var nameBytes [8]byte
	if _, err := rand.Read(nameBytes[:]); err != nil {
		return nil, fmt.Errorf("generate function name: %w", err)
	}
	funcName := "_gl_lua_" + hex.EncodeToString(nameBytes[:])

	var params []string
	for i := range args {
		params = append(params, fmt.Sprintf("p%d text", i+1))
	}
	paramStr := strings.Join(params, ", ")

	_, err = q.ExecContext(ctx, fmt.Sprintf(
		`CREATE OR REPLACE FUNCTION pg_temp.%s(%s) RETURNS text LANGUAGE pllua AS $pllua$ %s $pllua$`,
		funcName, paramStr, luaCode))
	if err != nil {
		return nil, err
	}

	var placeholders []string
	var queryArgs []interface{}
	for i, arg := range args {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		queryArgs = append(queryArgs, arg)
	}
	phStr := strings.Join(placeholders, ", ")

	query := fmt.Sprintf("SELECT pg_temp.%s(%s)", funcName, phStr)

	var result sql.NullString
	err = q.QueryRowContext(ctx, query, queryArgs...).Scan(&result)
	if err != nil {
		return nil, err
	}

	if result.Valid {
		return &result.String, nil
	}
	return nil, nil
}
