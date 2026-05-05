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

// fakeBinaryArgvCapture spawns Start against a fake shell-script binary that
// records its argv to disk and never binds the proxy port, then returns the
// captured argv after Start times out at the port-readiness poll. Centralised
// helper for every spawn-argv assertion in this file — same pattern as
// TestSubprocessCleanupOnConnectFailure but reused across multiple cases.
func fakeBinaryArgvCapture(t *testing.T, port int, opts ...Option) []string {
	t.Helper()
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

	combined := append([]Option{
		WithProxyPort(port),
		WithSilent(true),
		WithDashboardPort(0),
		WithInvalidationPort(0),
	}, opts...)

	gl, err := Start(ctx, "postgresql://user:pass@localhost:5432/db", combined...)
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
	return strings.Split(strings.TrimSpace(string(data)), "\n")
}

func containsArg(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

// --- WithDisableProxyCache argv emission ---

func TestSpawnArgv_DisableProxyCache(t *testing.T) {
	args := fakeBinaryArgvCapture(t, 17742, WithDisableProxyCache(true))
	if !containsArg(args, "--disable-proxy-cache") {
		t.Fatalf("expected --disable-proxy-cache in spawned argv, got %v", args)
	}
}

func TestSpawnArgv_DisableProxyCache_DefaultOmitted(t *testing.T) {
	args := fakeBinaryArgvCapture(t, 17743)
	if containsArg(args, "--disable-proxy-cache") {
		t.Fatalf("did not expect --disable-proxy-cache without WithDisableProxyCache(true), got %v", args)
	}
}

func TestSpawnArgv_DisableProxyCache_FalseOmitted(t *testing.T) {
	// WithDisableProxyCache(false) must not emit the flag — there is no
	// "enable" CLI counterpart and we don't want to double-imply intent.
	args := fakeBinaryArgvCapture(t, 17744, WithDisableProxyCache(false))
	if containsArg(args, "--disable-proxy-cache") {
		t.Fatalf("did not expect --disable-proxy-cache for WithDisableProxyCache(false), got %v", args)
	}
}

// --- WithDisableMatviews argv emission ---

func TestSpawnArgv_DisableMatviews(t *testing.T) {
	args := fakeBinaryArgvCapture(t, 17745, WithDisableMatviews(true))
	if !containsArg(args, "--disable-matviews") {
		t.Fatalf("expected --disable-matviews in spawned argv, got %v", args)
	}
}

func TestSpawnArgv_DisableMatviews_DefaultOmitted(t *testing.T) {
	args := fakeBinaryArgvCapture(t, 17746)
	if containsArg(args, "--disable-matviews") {
		t.Fatalf("did not expect --disable-matviews without WithDisableMatviews(true), got %v", args)
	}
}

// --- WithDisableSqloptimize argv emission ---

func TestSpawnArgv_DisableSqloptimize(t *testing.T) {
	args := fakeBinaryArgvCapture(t, 17747, WithDisableSqloptimize(true))
	if !containsArg(args, "--disable-sqloptimize") {
		t.Fatalf("expected --disable-sqloptimize in spawned argv, got %v", args)
	}
}

func TestSpawnArgv_DisableSqloptimize_DefaultOmitted(t *testing.T) {
	args := fakeBinaryArgvCapture(t, 17748)
	if containsArg(args, "--disable-sqloptimize") {
		t.Fatalf("did not expect --disable-sqloptimize without WithDisableSqloptimize(true), got %v", args)
	}
}

// --- WithDisableAutoIndexes argv emission ---

func TestSpawnArgv_DisableAutoIndexes(t *testing.T) {
	args := fakeBinaryArgvCapture(t, 17749, WithDisableAutoIndexes(true))
	if !containsArg(args, "--disable-auto-indexes") {
		t.Fatalf("expected --disable-auto-indexes in spawned argv, got %v", args)
	}
}

func TestSpawnArgv_DisableAutoIndexes_DefaultOmitted(t *testing.T) {
	args := fakeBinaryArgvCapture(t, 17750)
	if containsArg(args, "--disable-auto-indexes") {
		t.Fatalf("did not expect --disable-auto-indexes without WithDisableAutoIndexes(true), got %v", args)
	}
}

// --- Combined: multiple disable options stack ---

func TestSpawnArgv_AllFourDisableOptionsTogether(t *testing.T) {
	args := fakeBinaryArgvCapture(t, 17751,
		WithDisableProxyCache(true),
		WithDisableMatviews(true),
		WithDisableSqloptimize(true),
		WithDisableAutoIndexes(true),
	)
	for _, want := range []string{
		"--disable-proxy-cache",
		"--disable-matviews",
		"--disable-sqloptimize",
		"--disable-auto-indexes",
	} {
		if !containsArg(args, want) {
			t.Errorf("expected %s in argv, got %v", want, args)
		}
	}
}

// --- Negative: the dropped option must not exist anywhere on the argv ---

func TestSpawnArgv_NoEnableProxyCacheForWrappersFlagEverEmitted(t *testing.T) {
	// Defensive: the previous --enable-proxy-cache-for-wrappers flag was
	// removed in the Model B pivot. Confirm it never lands in argv even
	// when every other surface option is exercised. (No WithEnableProxyCacheForWrappers
	// option exists; this test guards against a future re-introduction
	// via the structured config map.)
	args := fakeBinaryArgvCapture(t, 17752,
		WithDisableProxyCache(true),
		WithDisableMatviews(true),
	)
	if containsArg(args, "--enable-proxy-cache-for-wrappers") {
		t.Fatalf("--enable-proxy-cache-for-wrappers must not be emitted; got %v", args)
	}
}
