package goldlapel

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// integrationUpstream and integrationBinary control whether the live tests
// below try to spawn the Gold Lapel binary. If either is missing, tests are
// skipped. Set GOLDLAPEL_TEST_UPSTREAM to enable, e.g.
//
//	GOLDLAPEL_TEST_UPSTREAM=postgresql://sgibson@localhost/postgres \
//	GOLDLAPEL_BINARY=/home/sgibson/bin/goldlapel \
//	go test -run TestIntegration ./...
func integrationEnv(t *testing.T) string {
	t.Helper()
	upstream := os.Getenv("GOLDLAPEL_TEST_UPSTREAM")
	if upstream == "" {
		t.Skip("set GOLDLAPEL_TEST_UPSTREAM to run integration tests")
	}
	return upstream
}

// openIntegrationDB ignores gl.DB() (which may be nil if the auto-opener
// couldn't connect, e.g. lib/pq demanding SSL on a dev server) and opens a
// fresh pool with sslmode=disable appended. This is a test convenience —
// production apps pick the driver-specific URL form they want.
func openIntegrationDB(t *testing.T, gl *GoldLapel) *sql.DB {
	t.Helper()
	url := gl.URL()
	sep := "?"
	for _, ch := range url {
		if ch == '?' {
			sep = "&"
			break
		}
	}
	url += sep + "sslmode=disable"
	db, err := sql.Open("postgres", url)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		t.Fatalf("Ping: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// testPort gives each live-proxy test a different port so they don't collide
// on parallel runs or back-to-back invocations.
var testPortCounter int = 17932

func nextTestPort() int {
	testPortCounter++
	return testPortCounter
}

// startForIntegration boots a proxy against the configured upstream and
// registers cleanup.
func startForIntegration(t *testing.T) *GoldLapel {
	t.Helper()
	upstream := integrationEnv(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	port := nextTestPort()
	gl, err := Start(ctx, upstream,
		WithPort(port),
		WithConfig(map[string]interface{}{"dashboard_port": 0, "invalidation_port": 0}),
	)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		gl.Stop(context.Background())
	})

	if gl == nil {
		t.Fatal("Start returned nil *GoldLapel")
	}
	if !gl.Running() {
		t.Fatal("expected gl.Running() == true after Start")
	}
	if gl.URL() == "" {
		t.Fatal("expected non-empty URL after Start")
	}

	return gl
}

func TestIntegration_StartReturnsReadyInstance(t *testing.T) {
	gl := startForIntegration(t)
	db := openIntegrationDB(t, gl)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var one int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("SELECT 1: %v", err)
	}
	if one != 1 {
		t.Fatalf("expected 1, got %d", one)
	}
}

// TestIntegration_InTxCommits writes a row inside InTx and verifies it is
// visible after commit.
func TestIntegration_InTxCommits(t *testing.T) {
	gl := startForIntegration(t)
	db := openIntegrationDB(t, gl)

	ctx := context.Background()
	collection := fmt.Sprintf("gltest_intx_commit_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+collection)
	})

	// First seed the table outside the tx so the commit assertion doesn't
	// rely on CREATE TABLE inside a transaction (mock driver would disagree,
	// but real Postgres handles it fine).
	err := gl.InTx(ctx, db, func(scoped *GoldLapel) error {
		_, err := scoped.DocInsert(ctx, collection, map[string]interface{}{"name": "alice"})
		return err
	})
	if err != nil {
		t.Fatalf("InTx commit: %v", err)
	}

	// After commit, the row should be visible — open a non-transactional
	// DocCount against the pool.
	var count int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+collection).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row after commit, got %d", count)
	}
}

// TestIntegration_InTxRollsBack writes a row inside InTx, returns an error,
// and verifies the row is NOT visible.
func TestIntegration_InTxRollsBack(t *testing.T) {
	gl := startForIntegration(t)
	db := openIntegrationDB(t, gl)

	ctx := context.Background()
	collection := fmt.Sprintf("gltest_intx_rollback_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+collection)
	})

	// Pre-create the table and seed one row outside the tx. The proxy does
	// not like CREATE TABLE IF NOT EXISTS running inside a transaction, so
	// we avoid that path for the InTx smoke test by using a raw INSERT via
	// the transaction's ExecContext instead of DocInsert (which would
	// redundantly try to CREATE TABLE IF NOT EXISTS).
	if _, err := DocInsert(ctx, db, collection, map[string]interface{}{"name": "seed"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	sentinel := errors.New("rollback please")
	err := gl.InTx(ctx, db, func(scoped *GoldLapel) error {
		// Use a bare INSERT on the scoped tx so ensureCollection doesn't
		// re-run CREATE TABLE IF NOT EXISTS inside the transaction.
		_, err := scoped.tx.ExecContext(ctx,
			"INSERT INTO "+collection+" (data) VALUES ($1::jsonb)",
			`{"name":"bob"}`)
		if err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}

	var count int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+collection).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row after rollback (only the seed), got %d", count)
	}
}

// TestIntegration_WithTxOverride exercises the per-call WithTx option: we
// start a transaction, do a DocInsert against it via WithTx, then roll back
// and confirm nothing landed.
func TestIntegration_WithTxOverride(t *testing.T) {
	gl := startForIntegration(t)
	db := openIntegrationDB(t, gl)

	ctx := context.Background()
	collection := fmt.Sprintf("gltest_withtx_%d", time.Now().UnixNano())

	// First, create the collection outside the tx so the rollback doesn't
	// also wipe the DDL.
	if _, err := DocInsert(ctx, db, collection, map[string]interface{}{"seed": 1}); err != nil {
		t.Fatalf("seed DocInsert: %v", err)
	}
	t.Cleanup(func() {
		db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+collection)
	})

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}

	// Use a bare INSERT via tx so ensureCollection doesn't re-run inside the
	// transaction (which interacts badly with the proxy under lib/pq).
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO "+collection+" (data) VALUES ($1::jsonb)",
		`{"name":"carol"}`); err != nil {
		tx.Rollback()
		t.Fatalf("insert in tx: %v", err)
	}

	// Now exercise WithTx on a goldlapel method: DocCount with WithTx(tx)
	// must see the in-tx row count of 2.
	countInTx, err := gl.DocCount(ctx, collection, nil, WithTx(tx))
	if err != nil {
		tx.Rollback()
		t.Fatalf("DocCount WithTx: %v", err)
	}
	if countInTx != 2 {
		tx.Rollback()
		t.Fatalf("expected 2 rows inside tx, got %d", countInTx)
	}

	// Roll back and verify only the seed row remains.
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	count, err := DocCount(ctx, db, collection, nil)
	if err != nil {
		t.Fatalf("DocCount post-rollback: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row post-rollback, got %d", count)
	}
}

