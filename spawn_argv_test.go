//go:build !windows

package goldlapel

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestSpawnArgv_EnableProxyCacheForWrappers verifies that
// WithEnableProxyCacheForWrappers(true) causes spawn() to pass
// --enable-proxy-cache-for-wrappers in the subprocess argv. We can't introspect
// cmd.Args after Start returns (Start nils gl on failure and teardownLocked
// nils gl.cmd on the success path), so we use the same fake-binary pattern as
// TestSubprocessCleanupOnConnectFailure: point GOLDLAPEL_BINARY at a shell
// script that records "$@" to a file and never binds a port. Start fails at
// the port-readiness poll; we then read the captured argv off disk.
func TestSpawnArgv_EnableProxyCacheForWrappers(t *testing.T) {
	dir := t.TempDir()
	argvFile := filepath.Join(dir, "argv.txt")
	binPath := filepath.Join(dir, "goldlapel-fake")

	// Record argv (one arg per line so flag values can't be confused with
	// flag names), then sleep so Start hits its port-poll timeout instead
	// of an immediate exit (the immediate-exit path is harder to wait on).
	script := `#!/bin/sh
for a in "$@"; do echo "$a" >> ` + argvFile + `; done
exec sleep 60
`
	if err := os.WriteFile(binPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake binary: %v", err)
	}

	t.Setenv("GOLDLAPEL_BINARY", binPath)

	// Cap the test at well under startupTimeout (10s).
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	gl, err := Start(ctx, "postgresql://user:pass@localhost:5432/db",
		WithProxyPort(17742),
		WithEnableProxyCacheForWrappers(true),
		WithSilent(true),
		WithDashboardPort(0),
		WithInvalidationPort(0),
	)
	if err == nil {
		if gl != nil {
			gl.Stop(context.Background())
		}
		t.Fatal("expected Start to fail when fake binary never binds its port")
	}

	data, readErr := os.ReadFile(argvFile)
	if readErr != nil {
		t.Fatalf("fake binary never wrote its argv (%v) — did it run at all?", readErr)
	}
	args := strings.Split(strings.TrimSpace(string(data)), "\n")

	if !containsArg(args, "--enable-proxy-cache-for-wrappers") {
		t.Fatalf("expected --enable-proxy-cache-for-wrappers in spawned argv, got %v", args)
	}
}

// TestSpawnArgv_EnableProxyCacheForWrappers_DefaultOmitted verifies that when
// the option is not provided, the flag does NOT appear in the argv (so the
// proxy applies its own default of wrapper-skip).
func TestSpawnArgv_EnableProxyCacheForWrappers_DefaultOmitted(t *testing.T) {
	dir := t.TempDir()
	argvFile := filepath.Join(dir, "argv.txt")
	binPath := filepath.Join(dir, "goldlapel-fake")

	script := `#!/bin/sh
for a in "$@"; do echo "$a" >> ` + argvFile + `; done
exec sleep 60
`
	if err := os.WriteFile(binPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake binary: %v", err)
	}
	t.Setenv("GOLDLAPEL_BINARY", binPath)

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	gl, err := Start(ctx, "postgresql://user:pass@localhost:5432/db",
		WithProxyPort(17743),
		WithSilent(true),
		WithDashboardPort(0),
		WithInvalidationPort(0),
	)
	if err == nil {
		if gl != nil {
			gl.Stop(context.Background())
		}
		t.Fatal("expected Start to fail when fake binary never binds its port")
	}

	data, readErr := os.ReadFile(argvFile)
	if readErr != nil {
		t.Fatalf("fake binary never wrote its argv (%v) — did it run at all?", readErr)
	}
	args := strings.Split(strings.TrimSpace(string(data)), "\n")

	if containsArg(args, "--enable-proxy-cache-for-wrappers") {
		t.Fatalf("did not expect --enable-proxy-cache-for-wrappers in spawned argv when option not provided, got %v", args)
	}
}

func containsArg(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}
