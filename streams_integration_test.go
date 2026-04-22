package goldlapel

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// startForStreams boots a proxy with the dashboard enabled (streams need the
// DDL API). Mirrors startForIntegration but sets dashboard_port to
// proxy_port+1 instead of 0.
func startForStreams(t *testing.T) *GoldLapel {
	t.Helper()
	upstream := integrationEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	port := nextTestPort()
	gl, err := Start(ctx, upstream,
		WithPort(port),
		WithConfig(map[string]interface{}{
			"invalidation_port":   0,
			"disable_result_cache": true,
			"disable_consolidation": true,
		}),
	)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { gl.Stop(context.Background()) })
	return gl
}

func TestIntegrationStreams_PrefixedTable(t *testing.T) {
	gl := startForStreams(t)
	db := openIntegrationDB(t, gl)
	gl.UseDB(db) // wire the pool into gl so Stream* can reach it
	ctx := context.Background()
	name := fmt.Sprintf("gl_go_int_stream_%d", time.Now().UnixNano())

	if _, err := gl.StreamAdd(ctx, name, `{"type":"click"}`); err != nil {
		t.Fatalf("StreamAdd: %v", err)
	}

	// Verify _goldlapel.stream_<name> exists.
	var count int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.tables "+
			"WHERE table_schema = '_goldlapel' AND table_name = $1",
		"stream_"+name,
	).Scan(&count)
	if err != nil {
		t.Fatalf("info_schema query: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected _goldlapel.stream_%s to exist, got %d rows", name, count)
	}

	// No public.<name> table — proxy owns DDL.
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.tables "+
			"WHERE table_schema = 'public' AND table_name = $1",
		name,
	).Scan(&count)
	if err != nil {
		t.Fatalf("info_schema public query: %v", err)
	}
	if count != 0 {
		t.Fatalf("public.%s should not exist — proxy owns DDL", name)
	}
}

func TestIntegrationStreams_SchemaMetaRow(t *testing.T) {
	gl := startForStreams(t)
	db := openIntegrationDB(t, gl)
	gl.UseDB(db)
	ctx := context.Background()
	name := fmt.Sprintf("gl_go_int_meta_%d", time.Now().UnixNano())

	if _, err := gl.StreamAdd(ctx, name, `{"type":"click"}`); err != nil {
		t.Fatalf("StreamAdd: %v", err)
	}

	var family, storedName, version string
	err := db.QueryRowContext(ctx,
		"SELECT family, name, schema_version FROM _goldlapel.schema_meta "+
			"WHERE family = 'stream' AND name = $1",
		name,
	).Scan(&family, &storedName, &version)
	if err != nil {
		t.Fatalf("schema_meta query: %v", err)
	}
	if family != "stream" || storedName != name || version != "v1" {
		t.Fatalf("unexpected schema_meta row: family=%s name=%s version=%s",
			family, storedName, version)
	}
}

func TestIntegrationStreams_DDL_HTTPCallHappensOnce(t *testing.T) {
	gl := startForStreams(t)
	gl.UseDB(openIntegrationDB(t, gl))
	ctx := context.Background()
	name := fmt.Sprintf("gl_go_int_once_%d", time.Now().UnixNano())

	// Swap the post function for a counting spy.
	origPost := ddlPost
	var calls int32
	ddlPost = func(c context.Context, url, token string, body []byte) (int, []byte, error) {
		atomic.AddInt32(&calls, 1)
		return origPost(c, url, token, body)
	}
	defer func() { ddlPost = origPost }()

	if _, err := gl.StreamAdd(ctx, name, `{"i":1}`); err != nil {
		t.Fatalf("StreamAdd 1: %v", err)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("want 1 DDL POST, got %d", calls)
	}

	if _, err := gl.StreamAdd(ctx, name, `{"i":2}`); err != nil {
		t.Fatalf("StreamAdd 2: %v", err)
	}
	if _, err := gl.StreamAdd(ctx, name, `{"i":3}`); err != nil {
		t.Fatalf("StreamAdd 3: %v", err)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("subsequent calls must use cache — want 1, got %d", calls)
	}
}

func TestIntegrationStreams_RoundTrip(t *testing.T) {
	gl := startForStreams(t)
	gl.UseDB(openIntegrationDB(t, gl))
	ctx := context.Background()
	name := fmt.Sprintf("gl_go_int_rt_%d", time.Now().UnixNano())

	if err := gl.StreamCreateGroup(ctx, name, "workers"); err != nil {
		t.Fatalf("StreamCreateGroup: %v", err)
	}
	id1, err := gl.StreamAdd(ctx, name, `{"i":1}`)
	if err != nil {
		t.Fatalf("StreamAdd 1: %v", err)
	}
	id2, err := gl.StreamAdd(ctx, name, `{"i":2}`)
	if err != nil {
		t.Fatalf("StreamAdd 2: %v", err)
	}
	if id2 <= id1 {
		t.Errorf("id2 (%d) should be > id1 (%d)", id2, id1)
	}

	msgs, err := gl.StreamRead(ctx, name, "workers", "c", 10)
	if err != nil {
		t.Fatalf("StreamRead: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("want 2 messages, got %d", len(msgs))
	}

	// Ack the first; second ack should return false.
	ok, err := gl.StreamAck(ctx, name, "workers", id1)
	if err != nil || !ok {
		t.Fatalf("first ack: ok=%v err=%v", ok, err)
	}
	ok, err = gl.StreamAck(ctx, name, "workers", id1)
	if err != nil {
		t.Fatalf("second ack err: %v", err)
	}
	if ok {
		t.Errorf("second ack should return false (already removed)")
	}
}

// TestIntegrationStreams_ConcurrentReadNoDoubleClaim proves StreamRead's
// transaction wrapping is load-bearing: two consumers racing on the same
// consumer group must divide the pending messages between them, never
// claiming the same message twice.
//
// Without BEGIN/COMMIT around the cursor read + advance + pending insert,
// the SELECT ... FOR UPDATE lock is released immediately under autocommit
// and the two consumers read the same cursor.
func TestIntegrationStreams_ConcurrentReadNoDoubleClaim(t *testing.T) {
	gl := startForStreams(t)
	gl.UseDB(openIntegrationDB(t, gl))
	ctx := context.Background()
	name := fmt.Sprintf("gl_go_int_concurrent_%d", time.Now().UnixNano())

	if err := gl.StreamCreateGroup(ctx, name, "workers"); err != nil {
		t.Fatalf("StreamCreateGroup: %v", err)
	}
	const N = 40
	for i := 0; i < N; i++ {
		if _, err := gl.StreamAdd(ctx, name, fmt.Sprintf(`{"i":%d}`, i)); err != nil {
			t.Fatalf("StreamAdd %d: %v", i, err)
		}
	}

	type result struct {
		ids []int64
		err error
	}
	ch := make(chan result, 2)
	start := make(chan struct{})
	for _, consumer := range []string{"ca", "cb"} {
		consumer := consumer
		go func() {
			<-start
			var out []int64
			for {
				batch, err := gl.StreamRead(ctx, name, "workers", consumer, 4)
				if err != nil {
					ch <- result{nil, err}
					return
				}
				if len(batch) == 0 {
					break
				}
				for _, m := range batch {
					out = append(out, m.ID)
				}
			}
			ch <- result{out, nil}
		}()
	}
	close(start)

	seen := make(map[int64]int) // id -> consumer count
	for i := 0; i < 2; i++ {
		r := <-ch
		if r.err != nil {
			t.Fatalf("StreamRead goroutine: %v", r.err)
		}
		for _, id := range r.ids {
			seen[id]++
		}
	}
	if len(seen) != N {
		t.Errorf("want %d distinct messages, got %d (seen=%v)", N, len(seen), seen)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("message %d claimed %d times (want 1)", id, n)
		}
	}
}

// Suppress unused-import warning on database/sql when integration env isn't
// set (skip ⇒ none of these functions run).
var _ = sql.ErrNoRows
var _ os.Signal
