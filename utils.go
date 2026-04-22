package goldlapel

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
)

// ZMember represents a member and its score in a sorted set.
type ZMember struct {
	Member string
	Score  float64
}

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

// Enqueue adds a job to a queue table. Like redis.lpush().
func Enqueue(ctx context.Context, q execQuerier, queueTable string, payload interface{}) error {
	if err := validateIdentifier(queueTable); err != nil {
		return err
	}
	_, err := q.ExecContext(ctx,
		"CREATE TABLE IF NOT EXISTS "+queueTable+" ("+
			"id BIGSERIAL PRIMARY KEY, "+
			"payload JSONB NOT NULL, "+
			"created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())")
	if err != nil {
		return fmt.Errorf("create queue table: %w", err)
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	_, err = q.ExecContext(ctx, "INSERT INTO "+queueTable+" (payload) VALUES ($1::jsonb)", string(data))
	return err
}

// Dequeue pops the next job from a queue table. Like redis.brpop() (non-blocking).
// Returns the payload as raw JSON, or nil if the queue is empty.
func Dequeue(ctx context.Context, q execQuerier, queueTable string) (json.RawMessage, error) {
	if err := validateIdentifier(queueTable); err != nil {
		return nil, err
	}
	var payload string
	err := q.QueryRowContext(ctx,
		"DELETE FROM "+queueTable+
			" WHERE id = ("+
			"SELECT id FROM "+queueTable+
			" ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1"+
			") RETURNING payload").Scan(&payload)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return json.RawMessage(payload), nil
}

// Incr increments a counter. Like redis.incr().
func Incr(ctx context.Context, q execQuerier, table, key string, amount int64) (int64, error) {
	if err := validateIdentifier(table); err != nil {
		return 0, err
	}
	_, err := q.ExecContext(ctx,
		"CREATE TABLE IF NOT EXISTS "+table+" ("+
			"key TEXT PRIMARY KEY, "+
			"value BIGINT NOT NULL DEFAULT 0)")
	if err != nil {
		return 0, fmt.Errorf("create counter table: %w", err)
	}

	var value int64
	err = q.QueryRowContext(ctx,
		"INSERT INTO "+table+" (key, value) VALUES ($1, $2) "+
			"ON CONFLICT (key) DO UPDATE SET value = "+table+".value + $3 "+
			"RETURNING value",
		key, amount, amount).Scan(&value)
	if err != nil {
		return 0, err
	}
	return value, nil
}

// GetCounter reads a counter value. Returns 0 if the key doesn't exist.
func GetCounter(ctx context.Context, q execQuerier, table, key string) (int64, error) {
	if err := validateIdentifier(table); err != nil {
		return 0, err
	}
	var value int64
	err := q.QueryRowContext(ctx,
		"SELECT value FROM "+table+" WHERE key = $1",
		key).Scan(&value)

	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return value, nil
}

// Zadd adds a member with a score to a sorted set. Like redis.zadd().
func Zadd(ctx context.Context, q execQuerier, table, member string, score float64) error {
	if err := validateIdentifier(table); err != nil {
		return err
	}
	_, err := q.ExecContext(ctx,
		"CREATE TABLE IF NOT EXISTS "+table+" ("+
			"member TEXT PRIMARY KEY, "+
			"score DOUBLE PRECISION NOT NULL)")
	if err != nil {
		return fmt.Errorf("create sorted set table: %w", err)
	}

	_, err = q.ExecContext(ctx,
		"INSERT INTO "+table+" (member, score) VALUES ($1, $2) "+
			"ON CONFLICT (member) DO UPDATE SET score = EXCLUDED.score",
		member, score)
	return err
}

// Zrange gets members by score rank. Like redis.zrange().
func Zrange(ctx context.Context, q execQuerier, table string, start, stop int, desc bool) ([]ZMember, error) {
	if err := validateIdentifier(table); err != nil {
		return nil, err
	}
	order := "ASC"
	if desc {
		order = "DESC"
	}
	limit := stop - start

	rows, err := q.QueryContext(ctx,
		"SELECT member, score FROM "+table+
			" ORDER BY score "+order+
			" LIMIT $1 OFFSET $2",
		limit, start)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []ZMember
	for rows.Next() {
		var m ZMember
		if err := rows.Scan(&m.Member, &m.Score); err != nil {
			return nil, err
		}
		results = append(results, m)
	}
	return results, rows.Err()
}

// Zincrby increments a member's score in a sorted set. Like redis.zincrby().
func Zincrby(ctx context.Context, q execQuerier, table, member string, amount float64) (float64, error) {
	if err := validateIdentifier(table); err != nil {
		return 0, err
	}
	_, err := q.ExecContext(ctx,
		"CREATE TABLE IF NOT EXISTS "+table+" ("+
			"member TEXT PRIMARY KEY, "+
			"score DOUBLE PRECISION NOT NULL)")
	if err != nil {
		return 0, fmt.Errorf("create sorted set table: %w", err)
	}

	var score float64
	err = q.QueryRowContext(ctx,
		"INSERT INTO "+table+" (member, score) VALUES ($1, $2) "+
			"ON CONFLICT (member) DO UPDATE SET score = "+table+".score + $3 "+
			"RETURNING score",
		member, amount, amount).Scan(&score)
	if err != nil {
		return 0, err
	}
	return score, nil
}

// Zrank gets the rank of a member in a sorted set. Like redis.zrank().
func Zrank(ctx context.Context, q execQuerier, table, member string, desc bool) (*int, error) {
	if err := validateIdentifier(table); err != nil {
		return nil, err
	}
	order := "ASC"
	if desc {
		order = "DESC"
	}

	var rank int
	err := q.QueryRowContext(ctx,
		"SELECT rank FROM ("+
			"SELECT member, ROW_NUMBER() OVER (ORDER BY score "+order+") - 1 AS rank "+
			"FROM "+table+
			") ranked WHERE member = $1",
		member).Scan(&rank)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &rank, nil
}

// Zscore gets the score of a member in a sorted set. Like redis.zscore().
func Zscore(ctx context.Context, q execQuerier, table, member string) (*float64, error) {
	if err := validateIdentifier(table); err != nil {
		return nil, err
	}
	var score float64
	err := q.QueryRowContext(ctx,
		"SELECT score FROM "+table+" WHERE member = $1",
		member).Scan(&score)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &score, nil
}

// Zrem removes a member from a sorted set. Like redis.zrem().
func Zrem(ctx context.Context, q execQuerier, table, member string) (bool, error) {
	if err := validateIdentifier(table); err != nil {
		return false, err
	}
	result, err := q.ExecContext(ctx,
		"DELETE FROM "+table+" WHERE member = $1",
		member)
	if err != nil {
		return false, err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

// Hset sets a field in a hash. Like redis.hset().
func Hset(ctx context.Context, q execQuerier, table, key, field string, value interface{}) error {
	if err := validateIdentifier(table); err != nil {
		return err
	}
	_, err := q.ExecContext(ctx,
		"CREATE TABLE IF NOT EXISTS "+table+" ("+
			"key TEXT PRIMARY KEY, "+
			"data JSONB NOT NULL DEFAULT '{}'::jsonb)")
	if err != nil {
		return fmt.Errorf("create hash table: %w", err)
	}

	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal value: %w", err)
	}

	_, err = q.ExecContext(ctx,
		"INSERT INTO "+table+" (key, data) VALUES ($1, jsonb_build_object($2, $3::jsonb)) "+
			"ON CONFLICT (key) DO UPDATE SET data = "+table+".data || jsonb_build_object($4, $5::jsonb)",
		key, field, string(data), field, string(data))
	return err
}

// Hget gets a field from a hash. Like redis.hget().
func Hget(ctx context.Context, q execQuerier, table, key, field string) (json.RawMessage, error) {
	if err := validateIdentifier(table); err != nil {
		return nil, err
	}
	var val *string
	err := q.QueryRowContext(ctx,
		"SELECT data->>$1 FROM "+table+" WHERE key = $2",
		field, key).Scan(&val)

	if err == sql.ErrNoRows || val == nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return json.RawMessage(*val), nil
}

// Hgetall gets all fields from a hash. Like redis.hgetall().
func Hgetall(ctx context.Context, q execQuerier, table, key string) (json.RawMessage, error) {
	if err := validateIdentifier(table); err != nil {
		return nil, err
	}
	var data string
	err := q.QueryRowContext(ctx,
		"SELECT data FROM "+table+" WHERE key = $1",
		key).Scan(&data)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return json.RawMessage(data), nil
}

// Hdel removes a field from a hash. Like redis.hdel().
func Hdel(ctx context.Context, q execQuerier, table, key, field string) (bool, error) {
	if err := validateIdentifier(table); err != nil {
		return false, err
	}
	var existed bool
	err := q.QueryRowContext(ctx,
		"SELECT data ? $1 FROM "+table+" WHERE key = $2",
		field, key).Scan(&existed)

	if err == sql.ErrNoRows || !existed {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	_, err = q.ExecContext(ctx,
		"UPDATE "+table+" SET data = data - $1 WHERE key = $2",
		field, key)
	if err != nil {
		return false, err
	}
	return true, nil
}

// Geoadd adds a location to a geo table. Like redis.geoadd().
func Geoadd(ctx context.Context, q execQuerier, table, nameColumn, geomColumn, name string, lon, lat float64) error {
	if err := validateIdentifier(table); err != nil {
		return err
	}
	if err := validateIdentifier(nameColumn); err != nil {
		return err
	}
	if err := validateIdentifier(geomColumn); err != nil {
		return err
	}
	_, err := q.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS postgis")
	if err != nil {
		return fmt.Errorf("create postgis extension: %w", err)
	}

	_, err = q.ExecContext(ctx,
		"CREATE TABLE IF NOT EXISTS "+table+" ("+
			"id BIGSERIAL PRIMARY KEY, "+
			nameColumn+" TEXT NOT NULL, "+
			geomColumn+" GEOMETRY(Point, 4326) NOT NULL)")
	if err != nil {
		return fmt.Errorf("create geo table: %w", err)
	}

	_, err = q.ExecContext(ctx,
		"INSERT INTO "+table+" ("+nameColumn+", "+geomColumn+") "+
			"VALUES ($1, ST_SetSRID(ST_MakePoint($2, $3), 4326))",
		name, lon, lat)
	return err
}

// Georadius finds rows within a radius of a point. Like redis.georadius().
func Georadius(ctx context.Context, q execQuerier, table, geomColumn string, lon, lat, radiusMeters float64, limit int) ([]map[string]interface{}, error) {
	if err := validateIdentifier(table); err != nil {
		return nil, err
	}
	if err := validateIdentifier(geomColumn); err != nil {
		return nil, err
	}
	rows, err := q.QueryContext(ctx,
		"SELECT *, ST_Distance("+
			geomColumn+"::geography, "+
			"ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography"+
			") AS distance_m "+
			"FROM "+table+" "+
			"WHERE ST_DWithin("+
			geomColumn+"::geography, "+
			"ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography, "+
			"$5) "+
			"ORDER BY distance_m "+
			"LIMIT $6",
		lon, lat, lon, lat, radiusMeters, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(columns))
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
	}
	return results, rows.Err()
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

// Geodist gets the distance between two members in meters. Like redis.geodist().
func Geodist(ctx context.Context, q execQuerier, table, geomColumn, nameColumn, nameA, nameB string) (*float64, error) {
	if err := validateIdentifier(table); err != nil {
		return nil, err
	}
	if err := validateIdentifier(geomColumn); err != nil {
		return nil, err
	}
	if err := validateIdentifier(nameColumn); err != nil {
		return nil, err
	}
	var dist float64
	err := q.QueryRowContext(ctx,
		"SELECT ST_Distance(a."+geomColumn+"::geography, b."+geomColumn+"::geography) "+
			"FROM "+table+" a, "+table+" b "+
			"WHERE a."+nameColumn+" = $1 AND b."+nameColumn+" = $2",
		nameA, nameB).Scan(&dist)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &dist, nil
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

	q, eqErr := gl.requireExec()
	if eqErr != nil {
		return nil, eqErr
	}
	var lastID int64
	if err := q.QueryRowContext(ctx, cursorSQL, group).Scan(&lastID); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	rows, err := q.QueryContext(ctx, readSQL, lastID, count)
	if err != nil {
		return nil, err
	}
	var msgs []StreamMessage
	for rows.Next() {
		var m StreamMessage
		if err := rows.Scan(&m.ID, &m.Payload, &m.CreatedAt); err != nil {
			rows.Close()
			return nil, err
		}
		msgs = append(msgs, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(msgs) > 0 {
		maxID := msgs[len(msgs)-1].ID
		if _, err := q.ExecContext(ctx, advanceSQL, maxID, group); err != nil {
			return nil, err
		}
		for _, m := range msgs {
			if _, err := q.ExecContext(ctx, pendingSQL, m.ID, group, consumer); err != nil {
				return nil, err
			}
		}
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
