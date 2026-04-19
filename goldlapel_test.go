package goldlapel

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// --- FindBinary tests ---

func TestFindBinaryEnvVarOverride(t *testing.T) {
	// Create a temp file to act as the binary
	tmp := filepath.Join(t.TempDir(), "goldlapel-fake")
	if err := os.WriteFile(tmp, []byte("fake"), 0755); err != nil {
		t.Fatal(err)
	}

	t.Setenv("GOLDLAPEL_BINARY", tmp)

	path, err := FindBinary()
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if path != tmp {
		t.Fatalf("expected %s, got %s", tmp, path)
	}
}

func TestFindBinaryEnvVarMissingFile(t *testing.T) {
	t.Setenv("GOLDLAPEL_BINARY", "/nonexistent/path/goldlapel")

	_, err := FindBinary()
	if err == nil {
		t.Fatal("expected error for missing file")
	}
	expected := "GOLDLAPEL_BINARY points to /nonexistent/path/goldlapel but file not found"
	if err.Error() != expected {
		t.Fatalf("expected error %q, got %q", expected, err.Error())
	}
}

func TestIsExecutable(t *testing.T) {
	dir := t.TempDir()

	// File with execute permission
	execFile := filepath.Join(dir, "exec")
	if err := os.WriteFile(execFile, []byte("test"), 0755); err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(execFile)
	if !isExecutable(info) {
		t.Fatal("expected file with 0755 to be executable")
	}

	// File without execute permission
	noExecFile := filepath.Join(dir, "noexec")
	if err := os.WriteFile(noExecFile, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}
	info, _ = os.Stat(noExecFile)
	if isExecutable(info) {
		t.Fatal("expected file with 0644 to not be executable")
	}

	// Read-only file (simulates Go module cache)
	readOnlyFile := filepath.Join(dir, "readonly")
	if err := os.WriteFile(readOnlyFile, []byte("test"), 0444); err != nil {
		t.Fatal(err)
	}
	info, _ = os.Stat(readOnlyFile)
	if isExecutable(info) {
		t.Fatal("expected file with 0444 to not be executable")
	}
}

func TestCopyToExecutableTemp(t *testing.T) {
	dir := t.TempDir()

	// Create a non-executable source file
	src := filepath.Join(dir, "goldlapel-test-binary")
	content := []byte("fake binary content")
	if err := os.WriteFile(src, content, 0444); err != nil {
		t.Fatal(err)
	}

	dst, err := copyToExecutableTemp(src, "goldlapel-test-binary")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Verify the path includes a hash suffix
	if !strings.Contains(dst, "goldlapel-test-binary-") {
		t.Fatalf("expected hashed filename, got %q", dst)
	}

	// Verify the copy exists and is executable
	info, err := os.Stat(dst)
	if err != nil {
		t.Fatalf("expected temp copy to exist, got: %v", err)
	}
	if !isExecutable(info) {
		t.Fatal("expected temp copy to be executable")
	}

	// Verify content matches
	copied, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("failed to read temp copy: %v", err)
	}
	if string(copied) != string(content) {
		t.Fatalf("content mismatch: got %q, want %q", string(copied), string(content))
	}

	// Clean up
	os.RemoveAll(filepath.Dir(dst))
}

func TestCopyToExecutableTempReusesExisting(t *testing.T) {
	dir := t.TempDir()

	src := filepath.Join(dir, "goldlapel-test-reuse")
	content := []byte("fake binary content")
	if err := os.WriteFile(src, content, 0444); err != nil {
		t.Fatal(err)
	}

	// First call creates the copy
	dst1, err := copyToExecutableTemp(src, "goldlapel-test-reuse")
	if err != nil {
		t.Fatalf("first call: unexpected error: %v", err)
	}

	// Second call should reuse it (same content → same hash → same path)
	dst2, err := copyToExecutableTemp(src, "goldlapel-test-reuse")
	if err != nil {
		t.Fatalf("second call: unexpected error: %v", err)
	}

	if dst1 != dst2 {
		t.Fatalf("expected same path, got %q and %q", dst1, dst2)
	}

	// Clean up
	os.RemoveAll(filepath.Dir(dst1))
}

func TestCopyToExecutableTempDifferentContentGetsDifferentPath(t *testing.T) {
	dir := t.TempDir()

	src := filepath.Join(dir, "goldlapel-test-upgrade")

	// First version
	if err := os.WriteFile(src, []byte("version 1 binary"), 0444); err != nil {
		t.Fatal(err)
	}
	dst1, err := copyToExecutableTemp(src, "goldlapel-test-upgrade")
	if err != nil {
		t.Fatalf("first version: unexpected error: %v", err)
	}

	// Second version (different content, could even be same size)
	// chmod first since previous WriteFile created it read-only
	os.Chmod(src, 0644)
	if err := os.WriteFile(src, []byte("version 2 binary"), 0444); err != nil {
		t.Fatal(err)
	}
	dst2, err := copyToExecutableTemp(src, "goldlapel-test-upgrade")
	if err != nil {
		t.Fatalf("second version: unexpected error: %v", err)
	}

	if dst1 == dst2 {
		t.Fatalf("expected different paths for different content, both got %q", dst1)
	}

	// Old version file should have been cleaned up
	if _, err := os.Stat(dst1); !os.IsNotExist(err) {
		t.Fatalf("expected old version %q to be cleaned up", dst1)
	}

	// New version file should exist
	if _, err := os.Stat(dst2); err != nil {
		t.Fatalf("expected new version %q to exist, got: %v", dst2, err)
	}

	// Clean up
	os.RemoveAll(filepath.Dir(dst2))
}

func TestCopyToExecutableTempSourceNotFound(t *testing.T) {
	_, err := copyToExecutableTemp("/nonexistent/path/binary", "test-binary")
	if err == nil {
		t.Fatal("expected error for missing source file")
	}
	if !strings.Contains(err.Error(), "failed to read bundled binary") {
		t.Fatalf("expected 'failed to read bundled binary' in error, got: %v", err)
	}
}

func TestFindBinaryNotFoundError(t *testing.T) {
	t.Setenv("GOLDLAPEL_BINARY", "")
	t.Setenv("PATH", t.TempDir())

	// Skip if bundled binary exists (FindBinary finds it via runtime.Caller)
	_, err := FindBinary()
	if err == nil {
		t.Skip("bundled binary present — cannot test not-found path")
	}
	expected := "Gold Lapel binary not found. Set GOLDLAPEL_BINARY env var, install the platform-specific package, or ensure 'goldlapel' is on PATH."
	if err.Error() != expected {
		t.Fatalf("expected error %q, got %q", expected, err.Error())
	}
}

// --- MakeProxyURL tests ---

func TestMakeProxyURLPostgresqlWithPort(t *testing.T) {
	got := MakeProxyURL("postgresql://user:pass@dbhost:5432/mydb", 7932)
	want := "postgresql://user:pass@localhost:7932/mydb"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestMakeProxyURLPostgresWithPort(t *testing.T) {
	got := MakeProxyURL("postgres://user:pass@remote.aws.com:5432/mydb", 7932)
	want := "postgres://user:pass@localhost:7932/mydb"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestMakeProxyURLWithoutPort(t *testing.T) {
	got := MakeProxyURL("postgresql://user:pass@host.aws.com/mydb", 7932)
	want := "postgresql://user:pass@localhost:7932/mydb"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestMakeProxyURLWithoutPortOrPath(t *testing.T) {
	got := MakeProxyURL("postgresql://user:pass@host.aws.com", 7932)
	want := "postgresql://user:pass@localhost:7932"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestMakeProxyURLBareHostPort(t *testing.T) {
	got := MakeProxyURL("dbhost:5432", 7932)
	want := "localhost:7932"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestMakeProxyURLBareHost(t *testing.T) {
	got := MakeProxyURL("dbhost", 7932)
	want := "localhost:7932"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestMakeProxyURLPreservesQueryParams(t *testing.T) {
	got := MakeProxyURL("postgresql://user:pass@remote:5432/mydb?sslmode=require", 7932)
	want := "postgresql://user:pass@localhost:7932/mydb?sslmode=require"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestMakeProxyURLPreservesPercentEncoded(t *testing.T) {
	got := MakeProxyURL("postgresql://user:p%40ss@remote:5432/mydb", 7932)
	want := "postgresql://user:p%40ss@localhost:7932/mydb"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestMakeProxyURLWithoutUserinfo(t *testing.T) {
	got := MakeProxyURL("postgresql://dbhost:5432/mydb", 7932)
	want := "postgresql://localhost:7932/mydb"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestMakeProxyURLLiteralAtInPasswordWithPort(t *testing.T) {
	got := MakeProxyURL("postgresql://user:p@ss@host:5432/mydb", 7932)
	want := "postgresql://user:p@ss@localhost:7932/mydb"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestMakeProxyURLLiteralAtInPasswordWithoutPort(t *testing.T) {
	got := MakeProxyURL("postgresql://user:p@ss@host/mydb", 7932)
	want := "postgresql://user:p@ss@localhost:7932/mydb"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestMakeProxyURLLiteralAtWithQueryParams(t *testing.T) {
	got := MakeProxyURL("postgresql://user:p@ss@host:5432/mydb?sslmode=require&param=val@ue", 7932)
	want := "postgresql://user:p@ss@localhost:7932/mydb?sslmode=require&param=val@ue"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestMakeProxyURLLocalhostStaysLocalhost(t *testing.T) {
	got := MakeProxyURL("postgresql://user:pass@localhost:5432/mydb", 7932)
	want := "postgresql://user:pass@localhost:7932/mydb"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestMakeProxyURLCustomPort(t *testing.T) {
	got := MakeProxyURL("postgresql://user:pass@dbhost:5432/mydb", 9000)
	want := "postgresql://user:pass@localhost:9000/mydb"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// --- WaitForPort tests ---

func TestWaitForPortOpenPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	port := ln.Addr().(*net.TCPAddr).Port
	if !WaitForPort("127.0.0.1", port, 2*time.Second) {
		t.Fatal("expected WaitForPort to return true for open port")
	}
}

func TestWaitForPortClosedPort(t *testing.T) {
	// Use a port that's almost certainly not listening
	if WaitForPort("127.0.0.1", 19999, 200*time.Millisecond) {
		t.Fatal("expected WaitForPort to return false for closed port")
	}
}

// --- GoldLapel struct tests ---

// buildForTest constructs a *GoldLapel without spawning the binary. Used by
// unit tests that exercise field-level behaviour (Port, DashboardURL parsing,
// Stop idempotency on an unstarted instance, etc.).
func buildForTest(upstream string, opts ...Option) *GoldLapel {
	gl := &GoldLapel{
		upstream: upstream,
		port:     DefaultPort,
	}
	for _, opt := range opts {
		opt.applyStart(gl)
	}
	// Mirrors Start: default dashboard port to proxy port + 1 so custom
	// WithPort values propagate to the dashboard. Explicit dashboard_port
	// in config overrides.
	gl.dashboardPort = gl.port + 1
	if gl.config != nil {
		if dp, ok := gl.config["dashboard_port"]; ok {
			gl.dashboardPort = toInt(dp)
		}
	}
	return gl
}

func TestDefaultPort(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	if gl.Port() != 7932 {
		t.Fatalf("expected default port 7932, got %d", gl.Port())
	}
}

func TestCustomPort(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb", WithPort(9000))
	if gl.Port() != 9000 {
		t.Fatalf("expected port 9000, got %d", gl.Port())
	}
}

func TestNotRunningBeforeStart(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	if gl.Running() {
		t.Fatal("expected Running() to be false before Start()")
	}
}

func TestStopNoOp(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	if err := gl.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() on unstarted instance should not error, got: %v", err)
	}
}

func TestStopIdempotent(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	if err := gl.Stop(context.Background()); err != nil {
		t.Fatalf("first Stop() failed: %v", err)
	}
	if err := gl.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop() failed: %v", err)
	}
}

func TestCloseMirrorsStop(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	if err := gl.Close(); err != nil {
		t.Fatalf("Close() on unstarted instance should not error, got: %v", err)
	}
}

// --- ConfigToArgs tests ---

func TestConfigToArgs_StringValue(t *testing.T) {
	args, err := ConfigToArgs(map[string]interface{}{"mode": "waiter"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"--mode", "waiter"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("got %v, want %v", args, want)
	}
}

func TestConfigToArgs_NumericValue(t *testing.T) {
	args, err := ConfigToArgs(map[string]interface{}{"pool_size": 20})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"--pool-size", "20"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("got %v, want %v", args, want)
	}
}

func TestConfigToArgs_BooleanTrue(t *testing.T) {
	args, err := ConfigToArgs(map[string]interface{}{"disable_pool": true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"--disable-pool"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("got %v, want %v", args, want)
	}
}

func TestConfigToArgs_BooleanFalse(t *testing.T) {
	args, err := ConfigToArgs(map[string]interface{}{"disable_pool": false})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(args) != 0 {
		t.Fatalf("expected empty args for false boolean, got %v", args)
	}
}

func TestConfigToArgs_ListValue(t *testing.T) {
	args, err := ConfigToArgs(map[string]interface{}{
		"replica": []interface{}{"postgres://r1:5432/db", "postgres://r2:5432/db"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"--replica", "postgres://r1:5432/db", "--replica", "postgres://r2:5432/db"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("got %v, want %v", args, want)
	}
}

func TestConfigToArgs_ExcludeTablesList(t *testing.T) {
	args, err := ConfigToArgs(map[string]interface{}{
		"exclude_tables": []string{"sessions", "logs"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"--exclude-tables", "sessions", "--exclude-tables", "logs"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("got %v, want %v", args, want)
	}
}

func TestConfigToArgs_UnknownKey(t *testing.T) {
	_, err := ConfigToArgs(map[string]interface{}{"bogus_key": "value"})
	if err == nil {
		t.Fatal("expected error for unknown key")
	}
	if !strings.Contains(err.Error(), "unknown config key") {
		t.Fatalf("expected 'unknown config key' in error, got: %v", err)
	}
}

func TestConfigToArgs_MultipleKeys(t *testing.T) {
	args, err := ConfigToArgs(map[string]interface{}{
		"mode":         "waiter",
		"pool_size":    10,
		"disable_pool": true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Keys are sorted: disable_pool, mode, pool_size
	want := []string{"--disable-pool", "--mode", "waiter", "--pool-size", "10"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("got %v, want %v", args, want)
	}
}

func TestConfigToArgs_EmptyConfig(t *testing.T) {
	args, err := ConfigToArgs(map[string]interface{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if args != nil {
		t.Fatalf("expected nil for empty config, got %v", args)
	}
}

func TestConfigToArgs_NilConfig(t *testing.T) {
	args, err := ConfigToArgs(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if args != nil {
		t.Fatalf("expected nil for nil config, got %v", args)
	}
}

func TestConfigToArgs_BooleanNonBool(t *testing.T) {
	_, err := ConfigToArgs(map[string]interface{}{"disable_pool": "yes"})
	if err == nil {
		t.Fatal("expected error for non-bool value on boolean key")
	}
	if !strings.Contains(err.Error(), "expects a bool value") {
		t.Fatalf("expected 'expects a bool value' in error, got: %v", err)
	}
}

// --- log_level translation tests ---
//
// The proxy binary doesn't accept --log-level; verbosity is count-based
// (-v/-vv/-vvv). ConfigToArgs translates the ergonomic log_level string
// into the corresponding -v flag (or omits it for warn/error default).

func TestConfigToArgs_LogLevelDebug(t *testing.T) {
	args, err := ConfigToArgs(map[string]interface{}{"log_level": "debug"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"-vv"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("expected %v, got %v", want, args)
	}
}

func TestConfigToArgs_LogLevelTrace(t *testing.T) {
	args, err := ConfigToArgs(map[string]interface{}{"log_level": "trace"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(args, []string{"-vvv"}) {
		t.Fatalf("expected [-vvv], got %v", args)
	}
}

func TestConfigToArgs_LogLevelInfo(t *testing.T) {
	args, err := ConfigToArgs(map[string]interface{}{"log_level": "info"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(args, []string{"-v"}) {
		t.Fatalf("expected [-v], got %v", args)
	}
}

func TestConfigToArgs_LogLevelWarnEmitsNothing(t *testing.T) {
	for _, lvl := range []string{"warn", "warning", "error"} {
		args, err := ConfigToArgs(map[string]interface{}{"log_level": lvl})
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", lvl, err)
		}
		if len(args) != 0 {
			t.Fatalf("expected no args for log_level=%q, got %v", lvl, args)
		}
	}
}

func TestConfigToArgs_LogLevelCaseInsensitive(t *testing.T) {
	args, err := ConfigToArgs(map[string]interface{}{"log_level": "DEBUG"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(args, []string{"-vv"}) {
		t.Fatalf("expected [-vv], got %v", args)
	}
}

func TestConfigToArgs_LogLevelInvalid(t *testing.T) {
	_, err := ConfigToArgs(map[string]interface{}{"log_level": "loud"})
	if err == nil {
		t.Fatal("expected error for invalid log_level")
	}
	if !strings.Contains(err.Error(), "log_level must be one of") {
		t.Fatalf("expected 'log_level must be one of' in error, got: %v", err)
	}
}

func TestConfigToArgs_LogLevelNonString(t *testing.T) {
	_, err := ConfigToArgs(map[string]interface{}{"log_level": 42})
	if err == nil {
		t.Fatal("expected error for non-string log_level")
	}
}

func TestConfigToArgs_LogLevelNeverEmitsLongFlag(t *testing.T) {
	// Regression guard: the proxy binary rejects --log-level. Make sure we
	// never emit it under any circumstances.
	for _, lvl := range []string{"trace", "debug", "info", "warn", "error"} {
		args, err := ConfigToArgs(map[string]interface{}{"log_level": lvl})
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", lvl, err)
		}
		for _, a := range args {
			if a == "--log-level" {
				t.Fatalf("emitted --log-level for level=%q (proxy does not support it)", lvl)
			}
		}
	}
}

func TestWithLogLevelTranslatesToVerboseFlag(t *testing.T) {
	// End-to-end sanity: WithLogLevel → ConfigToArgs via the stored config map.
	gl := &GoldLapel{}
	WithLogLevel("debug").applyStart(gl)
	args, err := ConfigToArgs(gl.config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found := false
	for _, a := range args {
		if a == "-vv" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected -vv in args, got %v", args)
	}
}

// --- ConfigKeys tests ---

func TestConfigKeys_ReturnsSlice(t *testing.T) {
	keys := ConfigKeys()
	if keys == nil {
		t.Fatal("expected non-nil slice")
	}
	if len(keys) == 0 {
		t.Fatal("expected non-empty slice")
	}
}

func TestConfigKeys_ContainsKnownKeys(t *testing.T) {
	keys := ConfigKeys()
	keySet := make(map[string]bool, len(keys))
	for _, k := range keys {
		keySet[k] = true
	}
	for _, expected := range []string{"mode", "pool_size", "disable_matviews", "replica"} {
		if !keySet[expected] {
			t.Fatalf("expected ConfigKeys() to contain %q", expected)
		}
	}
}

func TestConfigKeys_MatchesValidConfigKeysCount(t *testing.T) {
	keys := ConfigKeys()
	// Must match the number of entries in validConfigKeys
	if len(keys) != len(validConfigKeys) {
		t.Fatalf("expected %d keys, got %d", len(validConfigKeys), len(keys))
	}
}

func TestConfigKeys_IsSorted(t *testing.T) {
	keys := ConfigKeys()
	for i := 1; i < len(keys); i++ {
		if keys[i] < keys[i-1] {
			t.Fatalf("keys not sorted: %q came after %q", keys[i], keys[i-1])
		}
	}
}

// --- DashboardURL tests ---

func TestDashboardURLDefaultPort(t *testing.T) {
	gl := buildForTest("postgresql://localhost:5432/mydb")
	if gl.dashboardPort != 7933 {
		t.Fatalf("expected default dashboard port 7933, got %d", gl.dashboardPort)
	}
}

func TestDashboardURLCustomPort(t *testing.T) {
	gl := buildForTest("postgresql://localhost:5432/mydb", WithConfig(map[string]interface{}{
		"dashboard_port": 8080,
	}))
	if gl.dashboardPort != 8080 {
		t.Fatalf("expected dashboard port 8080, got %d", gl.dashboardPort)
	}
}

func TestDashboardURLDisabled(t *testing.T) {
	gl := buildForTest("postgresql://localhost:5432/mydb", WithConfig(map[string]interface{}{
		"dashboard_port": 0,
	}))
	if gl.dashboardPort != 0 {
		t.Fatalf("expected dashboard port 0, got %d", gl.dashboardPort)
	}
	if url := gl.DashboardURL(); url != "" {
		t.Fatalf("expected empty DashboardURL when disabled, got %q", url)
	}
}

func TestDashboardURLNotRunning(t *testing.T) {
	gl := buildForTest("postgresql://localhost:5432/mydb")
	if url := gl.DashboardURL(); url != "" {
		t.Fatalf("expected empty DashboardURL when not running, got %q", url)
	}
}

func TestDashboardPortFromConfigString(t *testing.T) {
	gl := buildForTest("postgresql://localhost:5432/mydb", WithConfig(map[string]interface{}{
		"dashboard_port": "9090",
	}))
	if gl.dashboardPort != 9090 {
		t.Fatalf("expected dashboard port 9090, got %d", gl.dashboardPort)
	}
}

func TestDashboardPortDerivesFromCustomProxyPort(t *testing.T) {
	gl := buildForTest("postgresql://localhost:5432/mydb", WithPort(17932))
	if gl.dashboardPort != 17933 {
		t.Fatalf("expected dashboard port 17933 (proxy+1), got %d", gl.dashboardPort)
	}
}

func TestExplicitDashboardPortOverridesDerivation(t *testing.T) {
	gl := buildForTest("postgresql://localhost:5432/mydb",
		WithPort(17932),
		WithConfig(map[string]interface{}{"dashboard_port": 9999}),
	)
	if gl.dashboardPort != 9999 {
		t.Fatalf("expected explicit dashboard port 9999, got %d", gl.dashboardPort)
	}
}

func TestWithConfig_Integration(t *testing.T) {
	config := map[string]interface{}{
		"mode":      "waiter",
		"pool_size": 20,
	}
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb", WithConfig(config))
	if gl.config == nil {
		t.Fatal("expected config to be set")
	}
	if gl.config["mode"] != "waiter" {
		t.Fatalf("expected mode=waiter, got %v", gl.config["mode"])
	}
	if gl.config["pool_size"] != 20 {
		t.Fatalf("expected pool_size=20, got %v", gl.config["pool_size"])
	}
}

// --- Banner routing tests ---
//
// The startup banner must never go to stdout (library code that pollutes
// stdout breaks callers who capture it — CI tools, test runners, CLIs
// piping the wrapper's own output). WithSilent() must suppress it on both
// streams. These tests swap os.Stdout/os.Stderr for pipes, invoke the
// banner helper the same way Start does, and inspect what landed where —
// all without spawning the real binary.

// captureStdStreams redirects os.Stdout and os.Stderr to pipes, runs fn,
// restores the originals, and returns whatever fn wrote to each. Any I/O
// error (pipe creation, restore) fails the test — the banner contract
// isn't worth asserting against a leaky harness.
func captureStdStreams(t *testing.T, fn func()) (stdout, stderr string) {
	t.Helper()

	origStdout := os.Stdout
	origStderr := os.Stderr

	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe stdout: %v", err)
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		stdoutR.Close()
		stdoutW.Close()
		t.Fatalf("os.Pipe stderr: %v", err)
	}

	os.Stdout = stdoutW
	os.Stderr = stderrW

	// Drain concurrently — a full pipe buffer would deadlock fn.
	stdoutCh := make(chan string, 1)
	stderrCh := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(stdoutR)
		stdoutCh <- string(b)
	}()
	go func() {
		b, _ := io.ReadAll(stderrR)
		stderrCh <- string(b)
	}()

	defer func() {
		os.Stdout = origStdout
		os.Stderr = origStderr
	}()

	fn()

	// Close the writer ends so the readers see EOF.
	stdoutW.Close()
	stderrW.Close()

	return <-stdoutCh, <-stderrCh
}

func TestBannerWritesToStderrNotStdout(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")

	stdout, stderr := captureStdStreams(t, func() {
		// Mirror what spawn does at the print site.
		gl.printBanner(os.Stderr)
	})

	if !strings.Contains(stderr, "goldlapel →") {
		t.Fatalf("expected banner on stderr, got stderr=%q", stderr)
	}
	if strings.Contains(stdout, "goldlapel") {
		t.Fatalf("banner leaked to stdout: %q", stdout)
	}
	// Confirm the full shape so a future formatting change trips the test.
	if !strings.Contains(stderr, ":7932 (proxy)") || !strings.Contains(stderr, "127.0.0.1:7933 (dashboard)") {
		t.Fatalf("banner shape changed: %q", stderr)
	}
}

func TestWithSilentSuppressesBanner(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb", WithSilent())

	stdout, stderr := captureStdStreams(t, func() {
		gl.printBanner(os.Stderr)
	})

	if strings.Contains(stdout, "goldlapel") {
		t.Fatalf("WithSilent should have suppressed stdout banner, got %q", stdout)
	}
	if strings.Contains(stderr, "goldlapel") {
		t.Fatalf("WithSilent should have suppressed stderr banner, got %q", stderr)
	}
}

func TestWithSilentDefaultsFalse(t *testing.T) {
	// No WithSilent → silent field defaults to false → banner prints.
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	if gl.silent {
		t.Fatal("expected silent to default to false")
	}
}

func TestWithSilentSetsField(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb", WithSilent())
	if !gl.silent {
		t.Fatal("expected WithSilent() to set silent=true")
	}
}
