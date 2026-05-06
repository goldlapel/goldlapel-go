package goldlapel

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// --- PoolReleaseDiscarder lifecycle ---

func TestPoolReleaseDiscarder_DefaultsToClean(t *testing.T) {
	p := NewPoolReleaseDiscarder()
	if p.ShouldDiscard() {
		t.Fatal("fresh discarder should not be dirty")
	}
}

func TestPoolReleaseDiscarder_MarkDirtyAndClean(t *testing.T) {
	p := NewPoolReleaseDiscarder()
	p.MarkDirty()
	if !p.ShouldDiscard() {
		t.Fatal("MarkDirty should flip ShouldDiscard")
	}
	p.MarkClean()
	if p.ShouldDiscard() {
		t.Fatal("MarkClean should reset ShouldDiscard")
	}
}

func TestPoolReleaseDiscarder_RaceCleanUnderConcurrentMarks(t *testing.T) {
	// Multiple goroutines flipping the flag must be race-free under -race.
	p := NewPoolReleaseDiscarder()
	const goroutines, iters = 8, 200
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				if (id+i)%2 == 0 {
					p.MarkDirty()
				} else {
					p.MarkClean()
				}
				_ = p.ShouldDiscard()
			}
		}(g)
	}
	wg.Wait()
}

// --- OnAfterRelease ---

func TestOnAfterRelease_NilDiscarderSkipsDiscard(t *testing.T) {
	called := false
	exec := func(ctx context.Context, sql string) error {
		called = true
		return nil
	}
	if !OnAfterRelease(context.Background(), nil, exec) {
		t.Error("nil discarder should keep the connection (return true)")
	}
	if called {
		t.Error("nil discarder should not invoke exec")
	}
}

func TestOnAfterRelease_CleanDiscarderSkipsExec(t *testing.T) {
	p := NewPoolReleaseDiscarder()
	called := false
	exec := func(ctx context.Context, sql string) error {
		called = true
		return nil
	}
	if !OnAfterRelease(context.Background(), p, exec) {
		t.Error("clean discarder should keep the connection")
	}
	if called {
		t.Error("clean discarder should not run exec")
	}
}

func TestOnAfterRelease_DirtyDiscarderRunsExecAndClears(t *testing.T) {
	p := NewPoolReleaseDiscarder()
	p.MarkDirty()

	var execCalls atomic.Int32
	var seenSQL string
	exec := func(ctx context.Context, sql string) error {
		execCalls.Add(1)
		seenSQL = sql
		return nil
	}
	if !OnAfterRelease(context.Background(), p, exec) {
		t.Error("successful discard should keep the connection")
	}
	if execCalls.Load() != 1 {
		t.Fatalf("expected exec to fire once, got %d", execCalls.Load())
	}
	if seenSQL != "DISCARD ALL" {
		t.Errorf("expected DISCARD ALL, got %q", seenSQL)
	}
	if p.ShouldDiscard() {
		t.Error("post-discard the discarder should be clean")
	}
}

func TestOnAfterRelease_ExecErrorReturnsFalseAndKeepsDirty(t *testing.T) {
	p := NewPoolReleaseDiscarder()
	p.MarkDirty()
	exec := func(ctx context.Context, sql string) error {
		return errors.New("server gone")
	}
	if OnAfterRelease(context.Background(), p, exec) {
		t.Error("exec error should return false (drop the connection)")
	}
	if !p.ShouldDiscard() {
		t.Error("exec error should leave dirty flag set so next attempt retries")
	}
}

func TestOnAfterRelease_NilExecReturnsFalseWhenDirty(t *testing.T) {
	p := NewPoolReleaseDiscarder()
	p.MarkDirty()
	if OnAfterRelease(context.Background(), p, nil) {
		t.Error("nil exec on a dirty discarder should drop the connection")
	}
}

func TestOnAfterRelease_NilExecOnCleanDiscarderIsBenign(t *testing.T) {
	// A nil exec callback isn't a bug if there's nothing to discard.
	p := NewPoolReleaseDiscarder()
	if !OnAfterRelease(context.Background(), p, nil) {
		t.Error("clean discarder + nil exec should keep the connection")
	}
}

// --- AttachDiscarderTo + automatic state propagation ---

func TestAttachDiscarderTo_NilCachedConnIsNoOp(t *testing.T) {
	p := NewPoolReleaseDiscarder()
	if got := AttachDiscarderTo(nil, p); got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestAttachDiscarderTo_NilDiscarderIsNoOp(t *testing.T) {
	cc := &CachedConn{}
	if got := AttachDiscarderTo(cc, nil); got != cc {
		t.Error("AttachDiscarderTo(cc, nil) should return cc unchanged")
	}
	if cc.poolDiscarder != nil {
		t.Error("cc.poolDiscarder should remain nil")
	}
}

func TestAttachDiscarderTo_PropagatesUnsafeSetToDiscarder(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	p := NewPoolReleaseDiscarder()
	AttachDiscarderTo(cc, p)
	ctx := context.Background()

	cc.Exec(ctx, "SET app.user_id = '42'")
	if !p.ShouldDiscard() {
		t.Error("unsafe SET should mark pool dirty")
	}
}

func TestAttachDiscarderTo_SafeSetDoesNotMarkDirty(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	p := NewPoolReleaseDiscarder()
	AttachDiscarderTo(cc, p)
	ctx := context.Background()

	cc.Exec(ctx, "SET work_mem = '64MB'")
	if p.ShouldDiscard() {
		t.Error("safe SET should not mark pool dirty")
	}
}

func TestAttachDiscarderTo_DiscardAllClearsPoolDirty(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	p := NewPoolReleaseDiscarder()
	AttachDiscarderTo(cc, p)
	ctx := context.Background()

	cc.Exec(ctx, "SET app.user_id = '42'")
	if !p.ShouldDiscard() {
		t.Fatal("setup: SET should have marked dirty")
	}
	cc.Exec(ctx, "DISCARD ALL")
	if p.ShouldDiscard() {
		t.Error("DISCARD ALL should clear pool dirty")
	}
}

func TestAttachDiscarderTo_ResetAllClearsPoolDirty(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	p := NewPoolReleaseDiscarder()
	AttachDiscarderTo(cc, p)
	ctx := context.Background()

	cc.Exec(ctx, "SET search_path TO 'tenant_a'")
	cc.Exec(ctx, "RESET ALL")
	if p.ShouldDiscard() {
		t.Error("RESET ALL should clear pool dirty")
	}
}

func TestAttachDiscarderTo_SetLocalDoesNotMarkDirty(t *testing.T) {
	// SET LOCAL is automatically dropped at COMMIT/ROLLBACK; it doesn't
	// leak across pool borrowers, so the discarder stays clean.
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	p := NewPoolReleaseDiscarder()
	AttachDiscarderTo(cc, p)
	ctx := context.Background()

	cc.Exec(ctx, "BEGIN")
	cc.Exec(ctx, "SET LOCAL app.user_id = '42'")
	cc.Exec(ctx, "COMMIT")
	if p.ShouldDiscard() {
		t.Error("SET LOCAL should not mark pool dirty")
	}
}

func TestAttachDiscarderTo_SetConfigPropagates(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	p := NewPoolReleaseDiscarder()
	AttachDiscarderTo(cc, p)
	ctx := context.Background()

	// set_config(false) is the canonical Supabase shape — equivalent
	// to SET. Pool dirty must propagate.
	cc.Exec(ctx, "SELECT set_config('app.user_id', '42', false)")
	if !p.ShouldDiscard() {
		t.Error("set_config should mark pool dirty")
	}

	// set_config(true) is SET LOCAL — does NOT mark dirty.
	p.MarkClean()
	cc.Exec(ctx, "SELECT set_config('app.user_id', '99', true)")
	if p.ShouldDiscard() {
		t.Error("set_config(true) should not mark pool dirty")
	}
}

func TestAttachDiscarderTo_MultiStatementBodyIsHandled(t *testing.T) {
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	p := NewPoolReleaseDiscarder()
	AttachDiscarderTo(cc, p)
	ctx := context.Background()

	cc.Query(ctx, "SET app.user_id = '42'; SELECT 1")
	if !p.ShouldDiscard() {
		t.Error("multi-statement SET should mark pool dirty")
	}
}

// --- End-to-end: the documented pgxpool integration shape ---

func TestEndToEnd_PgxpoolStyleIntegration(t *testing.T) {
	// This mirrors what a pgxpool user would write:
	//
	//   discarder := goldlapel.NewPoolReleaseDiscarder()
	//   cfg.AfterRelease = func(c *pgx.Conn) bool {
	//       return goldlapel.OnAfterRelease(ctx, discarder, ...)
	//   }
	//   cc := goldlapel.AttachDiscarderTo(goldlapel.Wrap(c, port), discarder)
	//
	// Without importing pgxpool we exercise the same control flow.
	cc, _ := setupWrapped(t, [][]interface{}{{1}}, []FieldDescription{{Name: "id"}})
	p := NewPoolReleaseDiscarder()
	AttachDiscarderTo(cc, p)
	ctx := context.Background()

	// User does a series of SETs.
	cc.Exec(ctx, "SET app.user_id = '42'")
	cc.Exec(ctx, "SET search_path TO 'tenant_a'")

	// Pool would call AfterRelease at this point. Simulate.
	var execSeen []string
	exec := func(ctx context.Context, sql string) error {
		execSeen = append(execSeen, sql)
		return nil
	}
	keep := OnAfterRelease(ctx, p, exec)
	if !keep {
		t.Error("AfterRelease should keep the connection on success")
	}
	if len(execSeen) != 1 || execSeen[0] != "DISCARD ALL" {
		t.Errorf("expected DISCARD ALL exactly once, got %v", execSeen)
	}
	if p.ShouldDiscard() {
		t.Error("post-DISCARD the discarder should be clean")
	}

	// A second AfterRelease (e.g. the same conn checked out + released
	// again with no SETs) is now a no-op.
	execSeen = nil
	OnAfterRelease(ctx, p, exec)
	if len(execSeen) != 0 {
		t.Errorf("clean discarder should skip exec, got %v", execSeen)
	}
}
