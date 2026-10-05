package goldlapel

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// integrationEnv gates integration tests on the standardized Gold Lapel
// convention shared across all 7 wrapper repos:
//
//   - GOLDLAPEL_INTEGRATION=1  — explicit opt-in gate
//   - GOLDLAPEL_TEST_UPSTREAM  — Postgres URL for the test upstream
//
// Both must be set. If GOLDLAPEL_INTEGRATION=1 is set but
// GOLDLAPEL_TEST_UPSTREAM is missing, the gate calls t.Fatal — this
// prevents a half-configured CI from silently skipping integration tests
// and producing a false-green unit-only run.
//
// If GOLDLAPEL_INTEGRATION is unset, tests skip silently (unit-only run).
//
// Also set GOLDLAPEL_BINARY to point at the goldlapel binary, e.g.
//
//	GOLDLAPEL_INTEGRATION=1 \
//	GOLDLAPEL_TEST_UPSTREAM=postgresql://sgibson@localhost/postgres \
//	GOLDLAPEL_BINARY=/home/sgibson/bin/goldlapel \
//	go test -run TestIntegration ./...
func integrationEnv(t *testing.T) string {
	t.Helper()
	integration := os.Getenv("GOLDLAPEL_INTEGRATION") == "1"
	upstream := os.Getenv("GOLDLAPEL_TEST_UPSTREAM")

	if integration && upstream == "" {
		// Half-configured CI — fail loudly to prevent false-green.
		t.Fatal("GOLDLAPEL_INTEGRATION=1 is set but GOLDLAPEL_TEST_UPSTREAM " +
			"is missing. Set GOLDLAPEL_TEST_UPSTREAM to a Postgres URL " +
			"(e.g. postgresql://postgres@localhost/postgres) or unset " +
			"GOLDLAPEL_INTEGRATION to skip integration tests.")
	}

	if !integration {
		t.Skip("set GOLDLAPEL_INTEGRATION=1 and GOLDLAPEL_TEST_UPSTREAM to run integration tests")
	}

	return upstream
}

// withSSLDisabled appends sslmode=disable: lib/pq demands SSL by default,
// and neither the test proxy nor a dev server speaks it. This is a test
// convenience — production apps pick the driver-specific URL form they want.
func withSSLDisabled(url string) string {
	if strings.Contains(url, "?") {
		return url + "&sslmode=disable"
	}
	return url + "?sslmode=disable"
}

// openIntegrationDB opens a fresh pool of its own, separate from gl.DB().
// gl.URL() already carries sslmode=disable, which lib/pq needs.
func openIntegrationDB(t *testing.T, gl *GoldLapel) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", gl.URL())
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

// testPort gives each live-proxy test a different pair of ports (proxy, and
// dashboard on proxy + 1) so they don't collide on parallel runs or
// back-to-back invocations.
var testPortCounter int = 17932

func nextTestPort() int {
	testPortCounter += 2
	return testPortCounter
}

// startForIntegration boots a proxy against the configured upstream and
// registers cleanup. The dashboard stays on: the document-store helpers go
// through it.
func startForIntegration(t *testing.T) *GoldLapel {
	t.Helper()
	// The test Postgres doesn't speak TLS, so the proxy's upstream hop
	// mustn't ask for it.
	upstream := withSSLDisabled(integrationEnv(t))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	port := nextTestPort()
	gl, err := Start(ctx, upstream,
		WithProxyPort(port),
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

// dropCollectionOnCleanup drops a doc-store collection the proxy created
// (its _goldlapel.doc_<name> table and its schema_meta row) when the test
// ends, so runs don't pile up timestamped tables.
func dropCollectionOnCleanup(t *testing.T, gl *GoldLapel, db *sql.DB, collection string) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		table, err := gl.Documents.resolveTable(ctx, collection)
		if err != nil {
			t.Errorf("cleanup: resolve %s: %v", collection, err)
			return
		}
		meta := "_goldlapel_schema_meta"
		if strings.HasPrefix(table, "_goldlapel.") {
			meta = "_goldlapel.schema_meta"
		}
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			t.Errorf("cleanup: drop %s: %v", table, err)
		}
		if _, err := db.ExecContext(ctx, "DELETE FROM "+meta+" WHERE family = 'doc_store' AND name = $1", collection); err != nil {
			t.Errorf("cleanup: forget %s: %v", collection, err)
		}
	})
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
	dropCollectionOnCleanup(t, gl, db, collection)

	// Create the collection outside the tx: the proxy creates collections
	// through its own connection, which can't see an uncommitted one.
	if _, err := gl.Documents.Insert(ctx, collection, map[string]interface{}{"name": "seed"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	err := gl.InTx(ctx, db, func(scoped *GoldLapel) error {
		_, err := scoped.Documents.Insert(ctx, collection, map[string]interface{}{"name": "alice"})
		return err
	})
	if err != nil {
		t.Fatalf("InTx commit: %v", err)
	}

	// After commit, the row should be visible outside the transaction.
	count, err := gl.Documents.Count(ctx, collection, nil)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 rows after commit (seed + alice), got %d", count)
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
	dropCollectionOnCleanup(t, gl, db, collection)

	// First, create the collection outside the tx so the rollback doesn't
	// also wipe the DDL.
	if _, err := gl.Documents.Insert(ctx, collection, map[string]interface{}{"seed": 1}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}

	if _, err := gl.Documents.Insert(ctx, collection, map[string]interface{}{"name": "carol"}, WithTx(tx)); err != nil {
		tx.Rollback()
		t.Fatalf("insert in tx: %v", err)
	}

	// Now exercise WithTx on a goldlapel method: Documents.Count with
	// WithTx(tx) must see the in-tx row count of 2.
	countInTx, err := gl.Documents.Count(ctx, collection, nil, WithTx(tx))
	if err != nil {
		tx.Rollback()
		t.Fatalf("Documents.Count WithTx: %v", err)
	}
	if countInTx != 2 {
		tx.Rollback()
		t.Fatalf("expected 2 rows inside tx, got %d", countInTx)
	}

	// Roll back and verify only the seed row remains.
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	count, err := gl.Documents.Count(ctx, collection, nil)
	if err != nil {
		t.Fatalf("Documents.Count post-rollback: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row post-rollback, got %d", count)
	}
}

// TestIntegration_DocFilter_ElemMatch asserts the $elemMatch operator against
// a real Postgres instance — the SQL shape is identical to the cross-wrapper
// reference (jsonb_array_elements + EXISTS) so passing here confirms the Go
// translator's output actually executes against Postgres.
func TestIntegration_DocFilter_ElemMatch(t *testing.T) {
	gl := startForIntegration(t)
	db := openIntegrationDB(t, gl)

	ctx := context.Background()
	collection := fmt.Sprintf("gltest_elemmatch_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+collection)
	})

	// Seed: three docs with a "scores" array.
	docs := []interface{}{
		map[string]interface{}{"name": "alice", "scores": []interface{}{70, 85, 92}, "tags": []interface{}{"python", "sql"}},
		map[string]interface{}{"name": "bob", "scores": []interface{}{50, 60, 65}, "tags": []interface{}{"java", "go"}},
		map[string]interface{}{"name": "carol", "scores": []interface{}{88, 95}, "tags": []interface{}{"pytest", "ruby"}},
	}
	if _, err := DocInsertMany(ctx, db, collection, docs); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Numeric range: at least one score strictly between 80 and 90 →
	// alice (85) and carol (88) qualify; bob (max 65) doesn't.
	hits, err := DocFind(ctx, db, collection, map[string]interface{}{
		"scores": map[string]interface{}{
			"$elemMatch": map[string]interface{}{"$gt": 80, "$lt": 90},
		},
	})
	if err != nil {
		t.Fatalf("DocFind $elemMatch numeric: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("expected 2 hits for 80<score<90 (alice+carol), got %d", len(hits))
	}
	// Verify bob isn't in the results.
	for _, h := range hits {
		if h["data"].(map[string]interface{})["name"] == "bob" {
			t.Fatal("bob should not match — scores all <70")
		}
	}

	// Regex on string array: tags starting with "py" → alice ("python"), carol ("pytest").
	hits, err = DocFind(ctx, db, collection, map[string]interface{}{
		"tags": map[string]interface{}{
			"$elemMatch": map[string]interface{}{"$regex": "^py"},
		},
	})
	if err != nil {
		t.Fatalf("DocFind $elemMatch regex: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("expected 2 hits for tags^=py, got %d", len(hits))
	}

	// No match: scores > 100.
	hits, err = DocFind(ctx, db, collection, map[string]interface{}{
		"scores": map[string]interface{}{
			"$elemMatch": map[string]interface{}{"$gt": 100},
		},
	})
	if err != nil {
		t.Fatalf("DocFind $elemMatch no-match: %v", err)
	}
	if len(hits) != 0 {
		t.Fatalf("expected 0 hits for score>100, got %d", len(hits))
	}
}

// TestIntegration_DocFilter_Text asserts the $text operator against a real
// Postgres — uses the default english full-text config. No extensions
// required: to_tsvector/plainto_tsquery are built in.
func TestIntegration_DocFilter_Text(t *testing.T) {
	gl := startForIntegration(t)
	db := openIntegrationDB(t, gl)

	ctx := context.Background()
	collection := fmt.Sprintf("gltest_text_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+collection)
	})

	// Seed three articles.
	docs := []interface{}{
		map[string]interface{}{"title": "A guide to coffee", "body": "Brewing the perfect cup of coffee requires fresh beans."},
		map[string]interface{}{"title": "Tea ceremonies", "body": "Traditional tea brewing varies by region."},
		map[string]interface{}{"title": "Roasting techniques", "body": "Dark roasted coffee has a smoky character."},
	}
	if _, err := DocInsertMany(ctx, db, collection, docs); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Top-level $text: search anywhere in the document for "coffee" → 2 hits.
	hits, err := DocFind(ctx, db, collection, map[string]interface{}{
		"$text": map[string]interface{}{"$search": "coffee"},
	})
	if err != nil {
		t.Fatalf("DocFind top-level $text: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("expected 2 hits for 'coffee', got %d", len(hits))
	}

	// Field-level $text on body: "brewing" should hit the first two.
	hits, err = DocFind(ctx, db, collection, map[string]interface{}{
		"body": map[string]interface{}{
			"$text": map[string]interface{}{"$search": "brewing"},
		},
	})
	if err != nil {
		t.Fatalf("DocFind field-level $text: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("expected 2 hits for body contains 'brewing', got %d", len(hits))
	}

	// No match.
	hits, err = DocFind(ctx, db, collection, map[string]interface{}{
		"$text": map[string]interface{}{"$search": "unobtanium"},
	})
	if err != nil {
		t.Fatalf("DocFind $text no-match: %v", err)
	}
	if len(hits) != 0 {
		t.Fatalf("expected 0 hits for 'unobtanium', got %d", len(hits))
	}
}

// --- Several proxies in one process ---

// queryOne runs SELECT 1 through url.
func queryOne(t *testing.T, url string) {
	t.Helper()
	db, err := sql.Open("postgres", url)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var one int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("SELECT 1 via %s: %v", redactPassword(url), err)
	}
}

// TestIntegration_TwoUpstreamsGetTheirOwnPorts starts two upstreams without
// WithProxyPort. Before port allocation both landed on 7932 and the second
// proxy's queries could run against the first one's upstream.
func TestIntegration_TwoUpstreamsGetTheirOwnPorts(t *testing.T) {
	first := withSSLDisabled(integrationEnv(t))
	second := first + "&application_name=gltest_second"
	ctx := context.Background()

	a, err := Start(ctx, first, WithSilent(true))
	if err != nil {
		t.Fatalf("Start first: %v", err)
	}
	defer a.Stop(ctx)
	b, err := Start(ctx, second, WithSilent(true))
	if err != nil {
		t.Fatalf("Start second: %v", err)
	}
	defer b.Stop(ctx)

	ports := map[int]bool{a.ProxyPort(): true, a.DashboardPort(): true}
	if ports[b.ProxyPort()] || ports[b.DashboardPort()] {
		t.Fatalf("second proxy %d/%d overlaps the first %d/%d",
			b.ProxyPort(), b.DashboardPort(), a.ProxyPort(), a.DashboardPort())
	}
	queryOne(t, a.URL())
	queryOne(t, b.URL())
}

// TestIntegration_SameUpstreamSharesProxy starts the same upstream twice:
// one proxy, stopped by the last Stop.
func TestIntegration_SameUpstreamSharesProxy(t *testing.T) {
	upstream := withSSLDisabled(integrationEnv(t))
	ctx := context.Background()
	port := nextTestPort()

	a, err := Start(ctx, upstream, WithProxyPort(port), WithSilent(true))
	if err != nil {
		t.Fatalf("Start first: %v", err)
	}
	b, err := Start(ctx, upstream, WithSilent(true))
	if err != nil {
		a.Stop(ctx)
		t.Fatalf("Start second: %v", err)
	}
	if b.ProxyPort() != port {
		a.Stop(ctx)
		b.Stop(ctx)
		t.Fatalf("second Start should reuse the proxy on %d, got %d", port, b.ProxyPort())
	}

	if err := a.Stop(ctx); err != nil {
		t.Fatalf("Stop first: %v", err)
	}
	queryOne(t, b.URL())

	if err := b.Stop(ctx); err != nil {
		t.Fatalf("Stop second: %v", err)
	}
	if !portBindable(port) {
		t.Fatalf("port %d still held after the last Stop", port)
	}
}

// TestIntegration_ExplicitPortBusyElsewhere holds a port outside the
// wrapper's knowledge: the proxy refuses it and Start surfaces why.
func TestIntegration_ExplicitPortBusyElsewhere(t *testing.T) {
	upstream := withSSLDisabled(integrationEnv(t))
	port := nextTestPort()
	ln, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	gl, err := Start(context.Background(), upstream, WithProxyPort(port), WithSilent(true))
	if err == nil {
		gl.Stop(context.Background())
		t.Fatal("expected Start to fail on a port another process holds")
	}
	if !strings.Contains(err.Error(), "already in use") {
		t.Fatalf("expected the proxy's refusal in the error, got %q", err)
	}
}

// TestIntegration_UpstreamTLSParamsStayUpstream gives the upstream URL the
// TLS parameters a hosted Postgres URL carries; the app's URL must not, or
// the app's connection to the (plaintext) proxy fails.
func TestIntegration_UpstreamTLSParamsStayUpstream(t *testing.T) {
	base := integrationEnv(t)
	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	upstream := base + sep + "sslmode=prefer&channel_binding=prefer&connect_timeout=10"

	gl, err := Start(context.Background(), upstream, WithProxyPort(nextTestPort()), WithSilent(true))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer gl.Stop(context.Background())

	url := gl.URL()
	if strings.Contains(url, "sslmode=prefer") || strings.Contains(url, "channel_binding") {
		t.Fatalf("app URL kept upstream TLS params: %s", redactPassword(url))
	}
	if !strings.Contains(url, "connect_timeout=10") {
		t.Fatalf("app URL dropped a non-TLS param: %s", redactPassword(url))
	}
	queryOne(t, url)
}
