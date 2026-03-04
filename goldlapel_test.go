package goldlapel

import (
	"net"
	"os"
	"path/filepath"
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

func TestFindBinaryNotFoundError(t *testing.T) {
	t.Setenv("GOLDLAPEL_BINARY", "")
	// Ensure goldlapel is not on PATH
	t.Setenv("PATH", t.TempDir())

	_, err := FindBinary()
	if err == nil {
		t.Fatal("expected error when binary not found")
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

func TestDefaultPort(t *testing.T) {
	gl := New("postgresql://user:pass@localhost:5432/mydb")
	if gl.Port() != 7932 {
		t.Fatalf("expected default port 7932, got %d", gl.Port())
	}
}

func TestCustomPort(t *testing.T) {
	gl := New("postgresql://user:pass@localhost:5432/mydb", WithPort(9000))
	if gl.Port() != 9000 {
		t.Fatalf("expected port 9000, got %d", gl.Port())
	}
}

func TestNotRunningBeforeStart(t *testing.T) {
	gl := New("postgresql://user:pass@localhost:5432/mydb")
	if gl.Running() {
		t.Fatal("expected Running() to be false before Start()")
	}
}

func TestStopNoOp(t *testing.T) {
	gl := New("postgresql://user:pass@localhost:5432/mydb")
	if err := gl.Stop(); err != nil {
		t.Fatalf("Stop() on unstarted instance should not error, got: %v", err)
	}
}

func TestStopIdempotent(t *testing.T) {
	gl := New("postgresql://user:pass@localhost:5432/mydb")
	if err := gl.Stop(); err != nil {
		t.Fatalf("first Stop() failed: %v", err)
	}
	if err := gl.Stop(); err != nil {
		t.Fatalf("second Stop() failed: %v", err)
	}
}

// --- Singleton tests ---

func TestProxyURLEmptyWhenNotStarted(t *testing.T) {
	if url := ProxyURL(); url != "" {
		t.Fatalf("expected empty ProxyURL(), got %q", url)
	}
}
