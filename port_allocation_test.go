//go:build !windows

package goldlapel

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Fake binaries for the port-allocation tests. "goldlapel" here is a shell
// script; a listener the test holds plays the proxy's port where one has to
// answer.

// fakeLongRunningBinary installs a fake binary that appends its PID to a
// file and then sleeps, and returns that file's path.
func fakeLongRunningBinary(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "pids")
	binPath := filepath.Join(dir, "goldlapel-fake")
	script := "#!/bin/sh\necho $$ >> " + pidFile + "\nexec sleep 60\n"
	if err := os.WriteFile(binPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake binary: %v", err)
	}
	t.Setenv("GOLDLAPEL_BINARY", binPath)
	return pidFile
}

func readPIDs(t *testing.T, pidFile string) []int {
	t.Helper()
	data, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("fake binary never wrote its PID (%v) — did it run at all?", err)
	}
	var pids []int
	for _, line := range strings.Fields(string(data)) {
		pid, err := strconv.Atoi(line)
		if err != nil {
			t.Fatalf("parse PID %q: %v", line, err)
		}
		pids = append(pids, pid)
	}
	return pids
}

// holdPort listens on 0.0.0.0:port for the rest of the test, closing each
// connection at once so the pool Start opens fails its ping fast.
func holdPort(t *testing.T, port int) {
	t.Helper()
	ln, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		t.Fatalf("listen on %d: %v", port, err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()
}

// registerProxies adds hand-built proxies to the registry for the rest of
// the test.
func registerProxies(t *testing.T, procs ...*proxyProcess) {
	t.Helper()
	proxiesMu.Lock()
	for _, p := range procs {
		proxies[p] = struct{}{}
	}
	proxiesMu.Unlock()
	t.Cleanup(func() {
		proxiesMu.Lock()
		for _, p := range procs {
			delete(proxies, p)
		}
		proxiesMu.Unlock()
	})
}

// liveProxy is a registered, started, still-running proxy for the registry.
func liveProxy(upstream string, proxyPort, dashboardPort int) *proxyProcess {
	p := &proxyProcess{upstream: upstream, proxyPort: proxyPort, dashboardPort: dashboardPort,
		ready: make(chan struct{}), done: make(chan struct{}), refs: 1}
	close(p.ready)
	return p
}

func registrySize() int {
	proxiesMu.Lock()
	defer proxiesMu.Unlock()
	return len(proxies)
}

func claimFor(t *testing.T, opts ...Option) (*GoldLapel, error) {
	t.Helper()
	gl := &GoldLapel{upstream: "postgresql://new@db/app"}
	for _, opt := range opts {
		opt.applyStart(gl)
	}
	proxiesMu.Lock()
	defer proxiesMu.Unlock()
	_, err := gl.claimPortsLocked()
	return gl, err
}

// --- R2: readiness requires the child to be alive ---

func TestStart_FailsWhenChildExitsWhileSomethingElseAnswers(t *testing.T) {
	// Another process already listens on the port. The proxy refuses it and
	// exits — the answering port must not be mistaken for our proxy.
	const port = 17801
	holdPort(t, port)
	dir := t.TempDir()
	binPath := filepath.Join(dir, "goldlapel-fake")
	script := "#!/bin/sh\necho \"I'm afraid port " + strconv.Itoa(port) +
		", for the proxy, is already in use — perhaps another Gold Lapel.\" >&2\nexit 1\n"
	if err := os.WriteFile(binPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake binary: %v", err)
	}
	t.Setenv("GOLDLAPEL_BINARY", binPath)

	gl, err := Start(context.Background(), "postgresql://user:pass@localhost:5432/db",
		WithProxyPort(port), WithDashboardPort(0), WithSilent(true))
	if err == nil {
		gl.Stop(context.Background())
		t.Fatal("expected Start to fail when the proxy exits during startup")
	}
	for _, want := range []string{"already in use", "exit status 1"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected error to contain %q, got %q", want, err)
		}
	}
	if n := registrySize(); n != 0 {
		t.Fatalf("failed start must release its claim; registry holds %d", n)
	}
}

// --- R1: one proxy per upstream, reference counted ---

func TestStart_SameUpstreamSharesOneProxy(t *testing.T) {
	const port = 17803
	holdPort(t, port) // answers for the fake proxy
	pidFile := fakeLongRunningBinary(t)
	ctx := context.Background()
	upstream := "postgresql://user:pass@localhost:5432/db"

	a, err := Start(ctx, upstream, WithProxyPort(port), WithDashboardPort(0), WithSilent(true))
	if err != nil {
		t.Fatalf("first Start: %v", err)
	}
	b, err := Start(ctx, upstream, WithSilent(true))
	if err != nil {
		a.Stop(ctx)
		t.Fatalf("second Start: %v", err)
	}
	if b.ProxyPort() != port || b.URL() != a.URL() {
		t.Fatalf("second Start should share the first proxy: port %d url %q, want %d %q",
			b.ProxyPort(), b.URL(), port, a.URL())
	}
	pids := readPIDs(t, pidFile)
	if len(pids) != 1 {
		t.Fatalf("expected one spawned proxy, got %d", len(pids))
	}

	if err := a.Stop(ctx); err != nil {
		t.Fatalf("first Stop: %v", err)
	}
	if !b.Running() || !isProcessAliveByPID(pids[0]) {
		t.Fatal("stopping one handle must leave the proxy running for the other")
	}
	if a.Running() || a.URL() != "" {
		t.Fatal("a stopped handle must report not running and no URL")
	}
	if err := a.Stop(ctx); err != nil {
		t.Fatalf("repeat Stop: %v", err)
	}
	if !b.Running() {
		t.Fatal("a repeated Stop on a released handle must not release the proxy again")
	}

	if err := b.Stop(ctx); err != nil {
		t.Fatalf("last Stop: %v", err)
	}
	if isProcessAliveByPID(pids[0]) {
		t.Fatal("the last Stop must terminate the proxy")
	}
	if n := registrySize(); n != 0 {
		t.Fatalf("registry should be empty after the last Stop, holds %d", n)
	}
}

func TestStart_ExplicitPortHeldByAnotherUpstreamErrors(t *testing.T) {
	const port = 17805
	holdPort(t, port)
	fakeLongRunningBinary(t)
	ctx := context.Background()

	a, err := Start(ctx, "postgresql://alice:hunter2@db1:5432/app", WithProxyPort(port), WithSilent(true))
	if err != nil {
		t.Fatalf("first Start: %v", err)
	}
	defer a.Stop(ctx)

	for _, tc := range []struct {
		name string
		opts []Option
		role string
	}{
		{"proxy port", []Option{WithProxyPort(port)}, "proxy"},
		{"dashboard port", []Option{WithProxyPort(17900), WithDashboardPort(port + 1)}, "dashboard"},
		{"derived dashboard port", []Option{WithProxyPort(port - 1)}, "dashboard"},
		{"auto proxy port, explicit dashboard port", []Option{WithDashboardPort(port)}, "dashboard"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gl, err := Start(ctx, "postgresql://bob:s3cret@db2:5432/app", append(tc.opts, WithSilent(true))...)
			if err == nil {
				gl.Stop(ctx)
				t.Fatal("expected a collision error")
			}
			msg := err.Error()
			if !strings.Contains(msg, "as the "+tc.role+" port") ||
				!strings.Contains(msg, "postgresql://alice:***@db1:5432/app") {
				t.Fatalf("error should name the port role and the other (redacted) upstream, got %q", msg)
			}
			if strings.Contains(msg, "hunter2") {
				t.Fatalf("error leaks the other upstream's password: %q", msg)
			}
		})
	}
	if n := registrySize(); n != 1 {
		t.Fatalf("rejected Starts must not register anything; registry holds %d", n)
	}
}

func TestStart_InvalidOptionsClaimNothing(t *testing.T) {
	_, err := Start(context.Background(), "postgresql://localhost:5432/mydb",
		WithConfig(map[string]interface{}{"no_such_key": 1}))
	if err == nil {
		t.Fatal("expected an unknown config key to fail Start")
	}
	if n := registrySize(); n != 0 {
		t.Fatalf("a Start rejected at option validation must not register; registry holds %d", n)
	}
}

// --- R1: port choice ---

func TestClaimPorts_SkipsPortsOtherProxiesHold(t *testing.T) {
	registerProxies(t,
		liveProxy("postgresql://a@db/x", 7932, 7933),
		liveProxy("postgresql://b@db/x", 7936, 7935), // explicit dashboard below its proxy
	)
	gl, err := claimFor(t)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	held := map[int]bool{7932: true, 7933: true, 7935: true, 7936: true}
	if gl.proxyPort <= 7932 || held[gl.proxyPort] || held[gl.dashboardPort] || gl.dashboardPort != gl.proxyPort+1 {
		t.Fatalf("picked %d/%d, which overlaps a held port", gl.proxyPort, gl.dashboardPort)
	}
	if gl.proxyPort == 7934 {
		t.Fatal("7934 must be skipped: its dashboard port 7935 is held")
	}
}

func TestClaimPorts_SkipsPortsBusyAtOSLevel(t *testing.T) {
	// Something outside this process holds 7933 — the default pair's
	// dashboard port.
	if ln, err := net.Listen("tcp", "0.0.0.0:7933"); err == nil {
		t.Cleanup(func() { ln.Close() })
	}
	gl, err := claimFor(t)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if gl.proxyPort == 7932 || gl.proxyPort == 7933 || gl.dashboardPort == 7933 {
		t.Fatalf("picked %d/%d although 7933 is busy", gl.proxyPort, gl.dashboardPort)
	}
}

func TestClaimPorts_ExitedProxyHoldsNothing(t *testing.T) {
	dead := liveProxy("postgresql://a@db/x", 17811, 17812)
	close(dead.done)
	registerProxies(t, dead)
	if _, err := claimFor(t, WithProxyPort(17811)); err != nil {
		t.Fatalf("an exited proxy's ports must be free to claim, got %v", err)
	}
}

func TestClaimPorts_StartingProxyHoldsItsPorts(t *testing.T) {
	starting := liveProxy("postgresql://a@db/x", 17813, 17814)
	starting.ready = make(chan struct{}) // start still in flight
	registerProxies(t, starting)
	if _, err := claimFor(t, WithProxyPort(17814)); err == nil {
		t.Fatal("a proxy still starting must hold its ports")
	}
}

func TestClaimPorts_DisabledDashboardHoldsNoSecondPort(t *testing.T) {
	registerProxies(t, liveProxy("postgresql://a@db/x", 17815, 0))
	if _, err := claimFor(t, WithProxyPort(17816), WithDashboardPort(0)); err != nil {
		t.Fatalf("a proxy with the dashboard off holds only its proxy port, got %v", err)
	}
}

func TestStart_WaitsForInFlightStartOfSameUpstream(t *testing.T) {
	// A Start for an upstream whose proxy is still starting waits for it
	// instead of spawning a second one; if ctx ends first it gives up.
	starting := liveProxy("postgresql://a@db/x", 17817, 17818)
	starting.ready = make(chan struct{})
	registerProxies(t, starting)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := Start(ctx, "postgresql://a@db/x", WithSilent(true)); err != context.DeadlineExceeded {
		t.Fatalf("expected the wait to end with the context, got %v", err)
	}
}

// --- R3: the app's URL carries no upstream TLS parameters ---

func TestMakeProxyURL_StripsUpstreamTLSParams(t *testing.T) {
	withClearedPGAppName(t, func() {
		got := MakeProxyURL("postgresql://u:p@db.neon.tech:5432/app?sslmode=require&channel_binding=require&application_name=app&SSLRootCert=/ca.pem&connect_timeout=5&gssencmode=disable", 7932)
		want := "postgresql://u:p@localhost:7932/app?application_name=app&connect_timeout=5&sslmode=disable"
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
}

func TestMakeProxyURL_KeepsTLSParamsWhenProxyHasCertificate(t *testing.T) {
	withClearedPGAppName(t, func() {
		got := makeProxyURL("postgresql://u:p@db:5432/app?sslmode=require", 7932, true)
		want := "postgresql://u:p@localhost:7932/app?sslmode=require&" + appNameSuffix()
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
}

func TestClientTLS(t *testing.T) {
	cases := []struct {
		opts []Option
		want bool
	}{
		{nil, false},
		{[]Option{WithConfig(map[string]interface{}{"tls_cert": "c.pem"})}, false},
		{[]Option{WithConfig(map[string]interface{}{"tls_cert": "c.pem", "tls_key": "k.pem"})}, true},
		{[]Option{WithExtraArgs("--tls-cert", "c.pem", "--tls-key", "k.pem")}, true},
		{[]Option{WithExtraArgs("--tls-cert=c.pem")}, true},
	}
	for i, tc := range cases {
		if got := buildForTest("postgresql://db/app", tc.opts...).clientTLS(); got != tc.want {
			t.Errorf("case %d: clientTLS() = %v, want %v", i, got, tc.want)
		}
	}
}

// --- R4: removed config keys say why ---

func TestConfigToArgs_RemovedKeysSayWhy(t *testing.T) {
	_, err := ConfigToArgs(map[string]interface{}{"invalidation_port": 7934})
	if err == nil || !strings.Contains(err.Error(), "removed with the in-process cache") {
		t.Fatalf("expected the error to say the key went with the in-process cache, got %v", err)
	}
}
