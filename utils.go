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
