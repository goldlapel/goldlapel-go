package goldlapel

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
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
// Uses PostgreSQL NOTIFY under the hood.
func Publish(db *sql.DB, channel, message string) error {
	_, err := db.Exec("SELECT pg_notify($1, $2)", channel, message)
	return err
}

// Subscribe listens for messages on a channel. Like redis.subscribe().
// Uses PostgreSQL LISTEN/NOTIFY under the hood. This blocks forever,
// calling the callback for each received message. Pass a connection
// string (DSN), not a *sql.DB, because LISTEN requires a dedicated connection.
func Subscribe(conn string, channel string, callback func(channel, payload string)) error {
	minReconn := 10 * time.Second
	maxReconn := time.Minute
	listener := pq.NewListener(conn, minReconn, maxReconn, nil)
	defer listener.Close()

	if err := listener.Listen(channel); err != nil {
		return fmt.Errorf("listen on channel %q: %w", channel, err)
	}

	for {
		n := <-listener.Notify
		if n == nil {
			// Connection lost, lib/pq will reconnect automatically.
			// The next receive will block until reconnected.
			continue
		}
		callback(n.Channel, n.Extra)
	}
}

// SubscribeAsync listens for messages on a channel in a background goroutine.
// Like redis.subscribe() but non-blocking. Returns immediately.
// The returned channel receives an error if the initial LISTEN fails;
// otherwise it is closed when the goroutine starts listening.
func SubscribeAsync(conn string, channel string, callback func(channel, payload string)) chan error {
	errCh := make(chan error, 1)
	go func() {
		if err := Subscribe(conn, channel, callback); err != nil {
			errCh <- err
		}
	}()
	return errCh
}

// Enqueue adds a job to a queue table. Like redis.lpush().
// Creates the queue table if it doesn't exist. Payload is stored as JSONB.
func Enqueue(db *sql.DB, queueTable string, payload interface{}) error {
	_, err := db.Exec(
		"CREATE TABLE IF NOT EXISTS " + queueTable + " (" +
			"id BIGSERIAL PRIMARY KEY, " +
			"payload JSONB NOT NULL, " +
			"created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())")
	if err != nil {
		return fmt.Errorf("create queue table: %w", err)
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	_, err = db.Exec("INSERT INTO "+queueTable+" (payload) VALUES ($1::jsonb)", string(data))
	return err
}

// Dequeue pops the next job from a queue table. Like redis.brpop() (non-blocking).
// Uses FOR UPDATE SKIP LOCKED for safe concurrent access.
// Returns the payload as raw JSON, or nil if the queue is empty.
func Dequeue(db *sql.DB, queueTable string) (json.RawMessage, error) {
	var payload string
	err := db.QueryRow(
		"DELETE FROM " + queueTable +
			" WHERE id = (" +
			"SELECT id FROM " + queueTable +
			" ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1" +
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
// Creates the counter table if it doesn't exist. Returns the new value.
func Incr(db *sql.DB, table, key string, amount int64) (int64, error) {
	_, err := db.Exec(
		"CREATE TABLE IF NOT EXISTS " + table + " (" +
			"key TEXT PRIMARY KEY, " +
			"value BIGINT NOT NULL DEFAULT 0)")
	if err != nil {
		return 0, fmt.Errorf("create counter table: %w", err)
	}

	var value int64
	err = db.QueryRow(
		"INSERT INTO "+table+" (key, value) VALUES ($1, $2) "+
			"ON CONFLICT (key) DO UPDATE SET value = "+table+".value + $3 "+
			"RETURNING value",
		key, amount, amount).Scan(&value)
	if err != nil {
		return 0, err
	}
	return value, nil
}

// GetCounter reads a counter value. Like redis.get() for a counter key.
// Returns the current value, or 0 if the key doesn't exist.
func GetCounter(db *sql.DB, table, key string) (int64, error) {
	var value int64
	err := db.QueryRow(
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
// Creates the sorted set table if it doesn't exist.
// If the member already exists, updates the score.
func Zadd(db *sql.DB, table, member string, score float64) error {
	_, err := db.Exec(
		"CREATE TABLE IF NOT EXISTS " + table + " (" +
			"member TEXT PRIMARY KEY, " +
			"score DOUBLE PRECISION NOT NULL)")
	if err != nil {
		return fmt.Errorf("create sorted set table: %w", err)
	}

	_, err = db.Exec(
		"INSERT INTO "+table+" (member, score) VALUES ($1, $2) "+
			"ON CONFLICT (member) DO UPDATE SET score = EXCLUDED.score",
		member, score)
	return err
}

// Zrange gets members by score rank. Like redis.zrange().
// Returns a slice of ZMember structs.
// desc=true returns highest scores first (leaderboard order).
func Zrange(db *sql.DB, table string, start, stop int, desc bool) ([]ZMember, error) {
	order := "ASC"
	if desc {
		order = "DESC"
	}
	limit := stop - start

	rows, err := db.Query(
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
// Creates the sorted set table if it doesn't exist.
// If the member doesn't exist, it is created with the given amount as its score.
// Returns the new score.
func Zincrby(db *sql.DB, table, member string, amount float64) (float64, error) {
	_, err := db.Exec(
		"CREATE TABLE IF NOT EXISTS " + table + " (" +
			"member TEXT PRIMARY KEY, " +
			"score DOUBLE PRECISION NOT NULL)")
	if err != nil {
		return 0, fmt.Errorf("create sorted set table: %w", err)
	}

	var score float64
	err = db.QueryRow(
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
// Rank is 0-based. desc=true ranks by highest score first (leaderboard order).
// Returns nil if the member doesn't exist.
func Zrank(db *sql.DB, table, member string, desc bool) (*int, error) {
	order := "ASC"
	if desc {
		order = "DESC"
	}

	var rank int
	err := db.QueryRow(
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
// Returns nil if the member doesn't exist.
func Zscore(db *sql.DB, table, member string) (*float64, error) {
	var score float64
	err := db.QueryRow(
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
// Returns true if the member was removed, false if it didn't exist.
func Zrem(db *sql.DB, table, member string) (bool, error) {
	result, err := db.Exec(
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
// Creates the hash table if it doesn't exist. Uses JSONB for storage.
func Hset(db *sql.DB, table, key, field string, value interface{}) error {
	_, err := db.Exec(
		"CREATE TABLE IF NOT EXISTS " + table + " (" +
			"key TEXT PRIMARY KEY, " +
			"data JSONB NOT NULL DEFAULT '{}'::jsonb)")
	if err != nil {
		return fmt.Errorf("create hash table: %w", err)
	}

	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal value: %w", err)
	}

	_, err = db.Exec(
		"INSERT INTO "+table+" (key, data) VALUES ($1, jsonb_build_object($2, $3::jsonb)) "+
			"ON CONFLICT (key) DO UPDATE SET data = "+table+".data || jsonb_build_object($4, $5::jsonb)",
		key, field, string(data), field, string(data))
	return err
}

// Hget gets a field from a hash. Like redis.hget().
// Returns the value as raw JSON, or nil if key or field doesn't exist.
func Hget(db *sql.DB, table, key, field string) (json.RawMessage, error) {
	var val *string
	err := db.QueryRow(
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
// Returns the full JSONB object as raw JSON, or nil if key doesn't exist.
func Hgetall(db *sql.DB, table, key string) (json.RawMessage, error) {
	var data string
	err := db.QueryRow(
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
// Returns true if the field existed, false otherwise.
func Hdel(db *sql.DB, table, key, field string) (bool, error) {
	var existed bool
	err := db.QueryRow(
		"SELECT data ? $1 FROM "+table+" WHERE key = $2",
		field, key).Scan(&existed)

	if err == sql.ErrNoRows || !existed {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	_, err = db.Exec(
		"UPDATE "+table+" SET data = data - $1 WHERE key = $2",
		field, key)
	if err != nil {
		return false, err
	}
	return true, nil
}

// Geoadd adds a location to a geo table. Like redis.geoadd().
// Creates the table with PostGIS geometry column if it doesn't exist.
// Requires PostGIS extension.
func Geoadd(db *sql.DB, table, nameColumn, geomColumn, name string, lon, lat float64) error {
	_, err := db.Exec("CREATE EXTENSION IF NOT EXISTS postgis")
	if err != nil {
		return fmt.Errorf("create postgis extension: %w", err)
	}

	_, err = db.Exec(
		"CREATE TABLE IF NOT EXISTS " + table + " (" +
			"id BIGSERIAL PRIMARY KEY, " +
			nameColumn + " TEXT NOT NULL, " +
			geomColumn + " GEOMETRY(Point, 4326) NOT NULL)")
	if err != nil {
		return fmt.Errorf("create geo table: %w", err)
	}

	_, err = db.Exec(
		"INSERT INTO "+table+" ("+nameColumn+", "+geomColumn+") "+
			"VALUES ($1, ST_SetSRID(ST_MakePoint($2, $3), 4326))",
		name, lon, lat)
	return err
}

// Georadius finds rows within a radius of a point. Like redis.georadius().
// Requires PostGIS extension. Uses ST_DWithin with geography type
// for accurate distance on the Earth's surface.
// Returns a slice of maps with all columns plus a "distance_m" field.
func Georadius(db *sql.DB, table, geomColumn string, lon, lat, radiusMeters float64, limit int) ([]map[string]interface{}, error) {
	rows, err := db.Query(
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

// CountDistinct counts the number of distinct values in a column. Like redis.pfcount().
// This is an exact count, unlike HyperLogLog which is approximate.
func CountDistinct(db *sql.DB, table, column string) (int64, error) {
	var count int64
	err := db.QueryRow("SELECT COUNT(DISTINCT " + column + ") FROM " + table).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// Geodist gets the distance between two members in meters. Like redis.geodist().
// Returns a pointer to the distance, or nil if either member doesn't exist.
func Geodist(db *sql.DB, table, geomColumn, nameColumn, nameA, nameB string) (*float64, error) {
	var dist float64
	err := db.QueryRow(
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
// Creates the stream table if it doesn't exist. Returns the message ID.
func StreamAdd(db *sql.DB, stream string, payload string) (int64, error) {
	_, err := db.Exec(
		"CREATE TABLE IF NOT EXISTS " + stream + " (" +
			"id BIGSERIAL PRIMARY KEY, " +
			"payload JSONB NOT NULL, " +
			"created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())")
	if err != nil {
		return 0, fmt.Errorf("create stream table: %w", err)
	}

	var id int64
	err = db.QueryRow(
		"INSERT INTO "+stream+" (payload) VALUES ($1::jsonb) RETURNING id",
		payload).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

// StreamCreateGroup creates a consumer group for a stream. Like redis.xgroup_create().
// Creates the consumer group tracking table and initializes the group at position 0.
func StreamCreateGroup(db *sql.DB, stream, group string) error {
	_, err := db.Exec(
		"CREATE TABLE IF NOT EXISTS " + stream + "_groups (" +
			"group_name TEXT NOT NULL, " +
			"consumer TEXT NOT NULL DEFAULT '', " +
			"message_id BIGINT NOT NULL, " +
			"acked BOOLEAN NOT NULL DEFAULT FALSE, " +
			"claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), " +
			"PRIMARY KEY (group_name, message_id))")
	if err != nil {
		return fmt.Errorf("create consumer group table: %w", err)
	}

	_, err = db.Exec(
		"CREATE TABLE IF NOT EXISTS " + stream + "_cursors (" +
			"group_name TEXT PRIMARY KEY, " +
			"last_id BIGINT NOT NULL DEFAULT 0)")
	if err != nil {
		return fmt.Errorf("create cursor table: %w", err)
	}

	_, err = db.Exec(
		"INSERT INTO "+stream+"_cursors (group_name, last_id) VALUES ($1, 0) "+
			"ON CONFLICT (group_name) DO NOTHING",
		group)
	return err
}

// StreamRead reads messages from a stream for a consumer group. Like redis.xreadgroup().
// Returns up to count undelivered messages and assigns them to the consumer.
func StreamRead(db *sql.DB, stream, group, consumer string, count int) ([]StreamMessage, error) {
	rows, err := db.Query(
		"WITH cursor AS ("+
			"SELECT last_id FROM "+stream+"_cursors WHERE group_name = $1 FOR UPDATE"+
			"), new_msgs AS ("+
			"SELECT id, payload, created_at FROM "+stream+
			" WHERE id > (SELECT last_id FROM cursor)"+
			" ORDER BY id LIMIT $2"+
			"), updated_cursor AS ("+
			"UPDATE "+stream+"_cursors SET last_id = COALESCE((SELECT MAX(id) FROM new_msgs), last_id)"+
			" WHERE group_name = $3"+
			"), inserted AS ("+
			"INSERT INTO "+stream+"_groups (group_name, consumer, message_id)"+
			" SELECT $4, $5, id FROM new_msgs"+
			" ON CONFLICT (group_name, message_id) DO NOTHING"+
			") SELECT id, payload, created_at FROM new_msgs ORDER BY id",
		group, count, group, group, consumer)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []StreamMessage
	for rows.Next() {
		var m StreamMessage
		if err := rows.Scan(&m.ID, &m.Payload, &m.CreatedAt); err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

// StreamAck acknowledges a message in a consumer group. Like redis.xack().
// Returns true if the message was acknowledged, false if it didn't exist.
func StreamAck(db *sql.DB, stream, group string, messageID int64) (bool, error) {
	result, err := db.Exec(
		"UPDATE "+stream+"_groups SET acked = TRUE "+
			"WHERE group_name = $1 AND message_id = $2 AND acked = FALSE",
		group, messageID)
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
// Returns messages that have been pending (unacked) longer than minIdleMs milliseconds
// and reassigns them to the specified consumer.
func StreamClaim(db *sql.DB, stream, group, consumer string, minIdleMs int64) ([]StreamMessage, error) {
	rows, err := db.Query(
		"WITH claimed AS ("+
			"UPDATE "+stream+"_groups SET consumer = $1, claimed_at = NOW()"+
			" WHERE group_name = $2 AND acked = FALSE"+
			" AND claimed_at < NOW() - ($3 || ' milliseconds')::interval"+
			" RETURNING message_id"+
			") SELECT s.id, s.payload, s.created_at FROM "+stream+" s"+
			" INNER JOIN claimed c ON c.message_id = s.id ORDER BY s.id",
		consumer, group, fmt.Sprintf("%d", minIdleMs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []StreamMessage
	for rows.Next() {
		var m StreamMessage
		if err := rows.Scan(&m.ID, &m.Payload, &m.CreatedAt); err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

// Script executes Lua code on PostgreSQL via the pllua extension.
// Creates a temporary function from the Lua code, executes it with the
// provided arguments, and returns the text result. Returns nil if the
// function returns NULL. Requires the pllua extension.
func Script(db *sql.DB, luaCode string, args ...string) (*string, error) {
	_, err := db.Exec("CREATE EXTENSION IF NOT EXISTS pllua")
	if err != nil {
		return nil, err
	}

	funcName := fmt.Sprintf("_gl_lua_%016x", rand.Int63())[:24]

	var params []string
	for i := range args {
		params = append(params, fmt.Sprintf("p%d text", i+1))
	}
	paramStr := strings.Join(params, ", ")

	_, err = db.Exec(fmt.Sprintf(
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
	err = db.QueryRow(query, queryArgs...).Scan(&result)
	if err != nil {
		return nil, err
	}

	if result.Valid {
		return &result.String, nil
	}
	return nil, nil
}
