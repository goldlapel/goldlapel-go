//go:build !windows

package goldlapel

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestSubprocessCleanupOnConnectFailure is the Go parity counterpart to the
// Python (test_v02_subprocess_cleanup.py) and Ruby
// (test_v02_subprocess_cleanup.rb) regressions. It guards the same bug class:
// the wrapper has spawned the goldlapel binary via exec.Command, but the
// wrapper's own readiness check subsequently fails (in Go, the port-poll
// timeout) — at that point the wrapper MUST tear the subprocess down before
// returning the error, otherwise the child outlives the parent Go process as
// an orphaned goldlapel.
//
// Python/Ruby frame the failure as "driver.connect() raised after Popen
// succeeded"; Go's Start does not return driver-connect errors (PingContext
// failure is swallowed, gl.db stays nil and URL() is still returned — see
// openDB + spawn), so the equivalent code path in Go is the port-readiness
// timeout branch in spawn(): cmd.Start() succeeded, waitForPortCtx returned
// false, cmd.Process.Kill() must fire. The test forces that branch with a
// fake "goldlapel" shell script that starts successfully but never binds a
// port, then confirms the recorded child PID is no longer alive after Start
// returns its error.
//
// The sh-based fake binary is POSIX-only, matching the existing
// Stop()-family tests in goldlapel_test.go.
func TestSubprocessCleanupOnConnectFailure(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child.pid")
	binPath := filepath.Join(dir, "goldlapel-fake")

	// sh -c variant that:
	//   1. writes its own PID to pidFile
	//   2. exec's sleep 300, which inherits the PID (exec replaces the
	//      process image without forking), so the sh PID == the sleep PID
	//      == what cmd.Process.Pid sees in spawn().
	// It never listens on a TCP port, so waitForPortCtx will time out.
	script := `#!/bin/sh
echo $$ > ` + pidFile + `
exec sleep 300
`
	if err := os.WriteFile(binPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake binary: %v", err)
	}

	t.Setenv("GOLDLAPEL_BINARY", binPath)
	// Pick a port the fake never binds — anything unlikely to be in use.
	// waitForPortCtx uses startupTimeout (10s) unless ctx expires sooner,
	// so we cap the test with a short context deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	gl, err := Start(ctx, "postgresql://user:pass@localhost:5432/db",
		WithPort(17739),
		WithSilent(),
		// Turn off the auto-derived dashboard + invalidation sockets so the
		// fake doesn't have to claim them either; 0 means "don't bind".
		WithConfig(map[string]interface{}{
			"dashboard_port":    0,
			"invalidation_port": 0,
		}),
	)
	if err == nil {
		if gl != nil {
			gl.Stop(context.Background())
		}
		t.Fatal("expected Start to fail when the binary never binds its port")
	}
	if gl != nil {
		// Start must return a nil instance on failure so callers can't
		// accidentally Stop() a half-constructed proxy.
		t.Fatalf("expected nil *GoldLapel on failure, got %+v", gl)
	}

	pidBytes, readErr := os.ReadFile(pidFile)
	if readErr != nil {
		t.Fatalf("fake binary never wrote its PID (%v) — did it run at all?", readErr)
	}
	pid, parseErr := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
	if parseErr != nil {
		t.Fatalf("parse PID %q: %v", pidBytes, parseErr)
	}

	// Poll for up to 500ms for the process to disappear. spawn's error path
	// calls cmd.Process.Kill() followed by cmd.Wait(), so by the time Start
	// returns the child is already reaped — but allow a tiny grace window
	// for OS-level propagation on slow CI runners.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if !isProcessAliveByPID(pid) {
			return // PASS: subprocess was cleaned up.
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Bug present: subprocess leaked. Kill it so the test doesn't leave
	// orphans behind for the rest of the suite / the developer's laptop.
	if proc, err := os.FindProcess(pid); err == nil {
		_ = proc.Signal(syscall.SIGKILL)
	}
	t.Fatalf("subprocess PID %d still alive after Start failed — cleanup bug", pid)
}

// isProcessAliveByPID uses the canonical Unix "signal 0" liveness check:
// kill(pid, 0) returns nil iff the process exists and the caller has
// permission to signal it. os.FindProcess on Unix never fails (it just
// wraps the PID in a *Process), so Signal is what actually probes.
func isProcessAliveByPID(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}
