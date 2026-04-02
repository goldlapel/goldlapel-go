package goldlapel

import (
	"database/sql"
	"encoding/json"
	"fmt"
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
