package goldlapel

import (
	"bufio"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// makeTelemetryCache builds a connected cache with a captured-emissions
// sink. Mirrors `make_cache(connected=True)` from the Python suite + the
// `_send_line = lambda line: emissions.append(line)` test seam from
// TestEvictionRateStateChange. Tests get deterministic emission capture
// without needing a real socket. The returned slice is appended to
// behind appendMu.
func makeTelemetryCache(t *testing.T, maxEntries int) (*NativeCache, *[]string, *sync.Mutex) {
	t.Helper()
	cache := newNativeCache()
	cache.maxEntries = maxEntries
	cache.enabled = true
	cache.invConnected = true
	emissions := []string{}
	var mu sync.Mutex
	cache.sendFunc = func(line string) error {
		mu.Lock()
		emissions = append(emissions, line)
		mu.Unlock()
		return nil
	}
	return cache, &emissions, &mu
}

// --- Wrapper identity ---

func TestTelemetry_WrapperIDIsUUIDv4(t *testing.T) {
	cache := newNativeCache()
	id := cache.WrapperID()
	// 8-4-4-4-12 hex layout
	parts := strings.Split(id, "-")
	if len(parts) != 5 {
		t.Fatalf("expected UUID 8-4-4-4-12 layout, got %q", id)
	}
	if len(parts[0]) != 8 || len(parts[1]) != 4 || len(parts[2]) != 4 || len(parts[3]) != 4 || len(parts[4]) != 12 {
		t.Fatalf("unexpected UUID part lengths: %q", id)
	}
	// Version nibble: third group's first hex digit must be '4'.
	if parts[2][0] != '4' {
		t.Fatalf("expected v4 marker '4' at parts[2][0], got %q in %q", parts[2][0:1], id)
	}
	// Variant nibble: fourth group's first hex digit ∈ {8,9,a,b}.
	v := parts[3][0]
	if v != '8' && v != '9' && v != 'a' && v != 'b' {
		t.Fatalf("expected RFC 4122 variant marker (8|9|a|b) at parts[3][0], got %q in %q", parts[3][0:1], id)
	}
}

func TestTelemetry_WrapperIDStableAcrossCalls(t *testing.T) {
	cache := newNativeCache()
	a := cache.WrapperID()
	b := cache.WrapperID()
	if a != b {
		t.Fatalf("WrapperID changed between calls: %q vs %q", a, b)
	}
}

func TestTelemetry_WrapperIDDistinctAcrossInstances(t *testing.T) {
	a := newNativeCache().WrapperID()
	b := newNativeCache().WrapperID()
	if a == b {
		t.Fatalf("two cache instances produced identical WrapperID %q", a)
	}
}

// --- Snapshot shape ---

func TestTelemetry_SnapshotCarriesRequiredFields(t *testing.T) {
	cache, _, _ := makeTelemetryCache(t, 64)
	cache.Put("SELECT 1", nil, 0, []int{1}, nil)
	cache.Get("SELECT 1", nil, 0)
	cache.Get("SELECT MISS", nil, 0)
	snap := cache.buildSnapshot()
	if snap.WrapperID != cache.WrapperID() {
		t.Fatalf("wrapper_id mismatch: %q vs %q", snap.WrapperID, cache.WrapperID())
	}
	if snap.Lang != "go" {
		t.Fatalf("lang mismatch: got %q, want \"go\"", snap.Lang)
	}
	if snap.Version == "" {
		t.Fatal("version must be non-empty")
	}
	if snap.Hits != 1 {
		t.Fatalf("hits: got %d, want 1", snap.Hits)
	}
	if snap.Misses != 1 {
		t.Fatalf("misses: got %d, want 1", snap.Misses)
	}
	if snap.Evictions != 0 {
		t.Fatalf("evictions: got %d, want 0", snap.Evictions)
	}
	if snap.CurrentSizeEntries != 1 {
		t.Fatalf("current_size_entries: got %d, want 1", snap.CurrentSizeEntries)
	}
	if snap.CapacityEntries != 64 {
		t.Fatalf("capacity_entries: got %d, want 64", snap.CapacityEntries)
	}
}

func TestTelemetry_SnapshotJSONFieldNames(t *testing.T) {
	cache, _, _ := makeTelemetryCache(t, 16)
	snap := cache.buildSnapshot()
	snap.TsMs = 1714600000000
	body, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	js := string(body)
	// Field names + types match the canonical Python shape exactly. The
	// proxy parses these structurally so any drift is a wire break.
	for _, want := range []string{
		`"wrapper_id":`,
		`"lang":"go"`,
		`"version":`,
		`"hits":`,
		`"misses":`,
		`"evictions":`,
		`"invalidations":`,
		`"current_size_entries":`,
		`"capacity_entries":`,
		`"ts_ms":1714600000000`,
	} {
		if !strings.Contains(js, want) {
			t.Errorf("missing %q in snapshot JSON: %s", want, js)
		}
	}
	// State must be omitted when empty (R: replies don't carry state).
	if strings.Contains(js, `"state":`) {
		t.Errorf("state field leaked into R:-shaped snapshot: %s", js)
	}
}

// --- Evictions counter ---

func TestTelemetry_EvictionsCounterStartsZero(t *testing.T) {
	cache, _, _ := makeTelemetryCache(t, 4)
	if cache.StatsEvictions() != 0 {
		t.Fatalf("expected 0, got %d", cache.StatsEvictions())
	}
}

func TestTelemetry_EvictionsCounterBumpsOnOverflow(t *testing.T) {
	cache, _, _ := makeTelemetryCache(t, 4)
	for i := 0; i < 8; i++ {
		cache.Put(uniqueSQL(i), nil, 0, []int{i}, nil)
	}
	// 8 puts, capacity 4 → 4 evictions.
	if got := cache.StatsEvictions(); got != 4 {
		t.Fatalf("expected 4 evictions, got %d", got)
	}
}

func TestTelemetry_EvictionsCounterNoBumpWithinCapacity(t *testing.T) {
	cache, _, _ := makeTelemetryCache(t, 8)
	for i := 0; i < 4; i++ {
		cache.Put(uniqueSQL(i), nil, 0, []int{i}, nil)
	}
	if got := cache.StatsEvictions(); got != 0 {
		t.Fatalf("expected 0 evictions, got %d", got)
	}
}

// --- Eviction-rate state-change emission (unit, via sendFunc) ---

func TestTelemetry_CacheFullFiresWhenEvictionsDominate(t *testing.T) {
	// Capacity 4 — every put past the 4th evicts. Window = 200 puts.
	cache, emissions, mu := makeTelemetryCache(t, 4)
	for i := 0; i < evictRateWindow+10; i++ {
		cache.Put(uniqueSQL(i), nil, 0, []int{i}, nil)
	}
	mu.Lock()
	defer mu.Unlock()
	if !anyContains(*emissions, "cache_full") {
		t.Fatalf("expected cache_full emission, got %v", *emissions)
	}
}

func TestTelemetry_CacheFullDoesNotFireBelowWindow(t *testing.T) {
	// With fewer puts than the window, no state-change fires — warmup gate.
	cache, emissions, mu := makeTelemetryCache(t, 2)
	for i := 0; i < evictRateWindow-1; i++ {
		cache.Put(uniqueSQL(i), nil, 0, []int{i}, nil)
	}
	mu.Lock()
	defer mu.Unlock()
	if anyContains(*emissions, "cache_full") {
		t.Fatalf("did not expect cache_full before window full, got %v", *emissions)
	}
}

func TestTelemetry_CacheFullEmittedExactlyOnceUntilRecovered(t *testing.T) {
	// Hysteresis: latched flag means a sustained-bad rate emits cache_full
	// exactly once, not once per put.
	cache, emissions, mu := makeTelemetryCache(t, 4)
	for i := 0; i < evictRateWindow+50; i++ {
		cache.Put(uniqueSQL(i), nil, 0, []int{i}, nil)
	}
	mu.Lock()
	count := countContains(*emissions, "cache_full")
	mu.Unlock()
	if count != 1 {
		t.Fatalf("expected exactly 1 cache_full emission, got %d", count)
	}
}

// --- ?: handler ---

func TestTelemetry_ProcessRequestSnapshotEmitsResponse(t *testing.T) {
	cache, emissions, mu := makeTelemetryCache(t, 16)
	cache.processRequest("snapshot")
	mu.Lock()
	defer mu.Unlock()
	rLines := filterPrefix(*emissions, "R:")
	if len(rLines) != 1 {
		t.Fatalf("expected 1 R: line, got %d (%v)", len(rLines), *emissions)
	}
	var snap snapshot
	if err := json.Unmarshal([]byte(rLines[0][2:]), &snap); err != nil {
		t.Fatalf("unmarshal R: payload: %v", err)
	}
	if snap.WrapperID != cache.WrapperID() {
		t.Fatalf("wrapper_id mismatch in R: payload")
	}
}

func TestTelemetry_ProcessRequestEmptyBodyTreatedAsSnapshot(t *testing.T) {
	cache, emissions, mu := makeTelemetryCache(t, 16)
	cache.processRequest("")
	mu.Lock()
	defer mu.Unlock()
	if len(filterPrefix(*emissions, "R:")) != 1 {
		t.Fatalf("expected 1 R: line, got %v", *emissions)
	}
}

func TestTelemetry_ProcessRequestUnknownBodyDropped(t *testing.T) {
	cache, emissions, mu := makeTelemetryCache(t, 16)
	cache.processRequest("future_request_type")
	mu.Lock()
	defer mu.Unlock()
	if rLines := filterPrefix(*emissions, "R:"); len(rLines) != 0 {
		t.Fatalf("expected 0 R: lines for unknown body, got %v", rLines)
	}
}

// --- Signal routing through processSignal ---

func TestTelemetry_ProcessSignalRoutesQuestion(t *testing.T) {
	cache, emissions, mu := makeTelemetryCache(t, 16)
	cache.processSignal("?:snapshot")
	mu.Lock()
	defer mu.Unlock()
	if len(filterPrefix(*emissions, "R:")) != 1 {
		t.Fatalf("expected ?:snapshot to emit R:, got %v", *emissions)
	}
}

func TestTelemetry_ProcessSignalUnknownPrefixSilent(t *testing.T) {
	cache, emissions, mu := makeTelemetryCache(t, 16)
	cache.processSignal("Z:future-prefix")
	cache.processSignal("$:bogus")
	mu.Lock()
	defer mu.Unlock()
	if len(*emissions) != 0 {
		t.Fatalf("unknown prefixes must not produce emissions, got %v", *emissions)
	}
}

// --- Opt-out ---

func TestTelemetry_ReportStatsDisabledSuppressesEmissions(t *testing.T) {
	cache, emissions, mu := makeTelemetryCache(t, 4)
	cache.SetReportStats(false)
	cache.processRequest("snapshot")
	cache.emitStateChange("wrapper_connected")
	for i := 0; i < evictRateWindow+10; i++ {
		cache.Put(uniqueSQL(i), nil, 0, []int{i}, nil)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(*emissions) != 0 {
		t.Fatalf("expected no emissions when reportStats disabled, got %v", *emissions)
	}
}

func TestTelemetry_ReportStatsEnvVarDefault(t *testing.T) {
	t.Setenv("GOLDLAPEL_REPORT_STATS", "false")
	cache := newNativeCache()
	if cache.ReportStatsEnabled() {
		t.Fatal("expected reportStats=false from env var")
	}
}

func TestTelemetry_ReportStatsEnvVarTrueByDefault(t *testing.T) {
	t.Setenv("GOLDLAPEL_REPORT_STATS", "")
	cache := newNativeCache()
	if !cache.ReportStatsEnabled() {
		t.Fatal("expected reportStats=true by default")
	}
}

// --- Integration: real socket ---

// telemetryServer is a tiny harness that listens on a random local port,
// accepts one connection from the cache, and exposes the lines the cache
// writes via a channel-fed slice. Mirrors `_accept_with_buf` from the
// Python suite.
type telemetryServer struct {
	ln    net.Listener
	port  int
	conn  net.Conn
	lines []string
	mu    sync.Mutex
	done  chan struct{}
}

func newTelemetryServer(t *testing.T) *telemetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return &telemetryServer{
		ln:   ln,
		port: ln.Addr().(*net.TCPAddr).Port,
		done: make(chan struct{}),
	}
}

func (ts *telemetryServer) accept(t *testing.T) {
	t.Helper()
	conn, err := ts.ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	ts.conn = conn
	go func() {
		defer close(ts.done)
		r := bufio.NewReader(conn)
		for {
			line, err := r.ReadString('\n')
			if line != "" {
				ts.mu.Lock()
				ts.lines = append(ts.lines, strings.TrimRight(line, "\n"))
				ts.mu.Unlock()
			}
			if err != nil {
				return
			}
		}
	}()
}

func (ts *telemetryServer) close() {
	if ts.conn != nil {
		ts.conn.Close()
	}
	ts.ln.Close()
}

func (ts *telemetryServer) snapshot() []string {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	out := make([]string, len(ts.lines))
	copy(out, ts.lines)
	return out
}

func (ts *telemetryServer) waitFor(predicate func([]string) bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate(ts.snapshot()) {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

func TestTelemetry_WrapperConnectedEmittedOnSocketConnect(t *testing.T) {
	ResetNativeCache()
	defer ResetNativeCache()

	cache := newNativeCache()
	cache.maxEntries = 64
	cache.enabled = true

	srv := newTelemetryServer(t)
	defer srv.close()

	cache.ConnectInvalidation(srv.port)
	defer cache.StopInvalidation()

	srv.accept(t)

	if !srv.waitFor(func(lines []string) bool {
		return anyMatch(lines, func(l string) bool { return strings.HasPrefix(l, "S:") })
	}, 2*time.Second) {
		t.Fatalf("expected S: line on connect, got %v", srv.snapshot())
	}

	stateLines := filterPrefix(srv.snapshot(), "S:")
	if len(stateLines) == 0 {
		t.Fatalf("expected S: line, got %v", srv.snapshot())
	}
	var snap snapshot
	if err := json.Unmarshal([]byte(stateLines[0][2:]), &snap); err != nil {
		t.Fatalf("unmarshal: %v (line: %q)", err, stateLines[0])
	}
	if snap.State != "wrapper_connected" {
		t.Fatalf("expected state=wrapper_connected, got %q", snap.State)
	}
	if snap.WrapperID != cache.WrapperID() {
		t.Fatalf("wrapper_id mismatch: %q vs %q", snap.WrapperID, cache.WrapperID())
	}
	if snap.Lang != "go" {
		t.Fatalf("expected lang=go, got %q", snap.Lang)
	}
}

func TestTelemetry_SnapshotRequestReturnsResponse(t *testing.T) {
	ResetNativeCache()
	defer ResetNativeCache()

	cache := newNativeCache()
	cache.maxEntries = 64
	cache.enabled = true
	// Pre-populate the cache so the response carries non-zero counters,
	// matching the Python integration test.
	cache.invConnected = true
	cache.Put("SELECT 1", nil, 0, []int{1}, nil)
	cache.Get("SELECT 1", nil, 0)
	cache.invConnected = false

	srv := newTelemetryServer(t)
	defer srv.close()

	cache.ConnectInvalidation(srv.port)
	defer cache.StopInvalidation()

	srv.accept(t)

	if !srv.waitFor(func(lines []string) bool {
		return anyMatch(lines, func(l string) bool { return strings.HasPrefix(l, "S:") })
	}, 2*time.Second) {
		t.Fatalf("expected wrapper_connected first, got %v", srv.snapshot())
	}

	if _, err := srv.conn.Write([]byte("?:snapshot\n")); err != nil {
		t.Fatalf("write ?:snapshot: %v", err)
	}

	if !srv.waitFor(func(lines []string) bool {
		return anyMatch(lines, func(l string) bool { return strings.HasPrefix(l, "R:") })
	}, 2*time.Second) {
		t.Fatalf("expected R: response, got %v", srv.snapshot())
	}

	rLines := filterPrefix(srv.snapshot(), "R:")
	if len(rLines) == 0 {
		t.Fatalf("expected R: line, got %v", srv.snapshot())
	}
	var snap snapshot
	if err := json.Unmarshal([]byte(rLines[0][2:]), &snap); err != nil {
		t.Fatalf("unmarshal: %v (line: %q)", err, rLines[0])
	}
	if snap.WrapperID != cache.WrapperID() {
		t.Fatalf("wrapper_id mismatch")
	}
	if snap.Hits != 1 {
		t.Fatalf("expected hits=1, got %d", snap.Hits)
	}
	if snap.CurrentSizeEntries != 1 {
		t.Fatalf("expected current_size_entries=1, got %d", snap.CurrentSizeEntries)
	}
}

func TestTelemetry_ReportStatsDisabledSuppressesWireEmissions(t *testing.T) {
	ResetNativeCache()
	defer ResetNativeCache()

	cache := newNativeCache()
	cache.maxEntries = 64
	cache.enabled = true
	cache.SetReportStats(false)

	srv := newTelemetryServer(t)
	defer srv.close()

	cache.ConnectInvalidation(srv.port)
	defer cache.StopInvalidation()

	srv.accept(t)

	// Give the loop time to attempt an emit, and give a ?:snapshot a
	// chance to round-trip.
	time.Sleep(150 * time.Millisecond)
	srv.conn.Write([]byte("?:snapshot\n"))
	time.Sleep(150 * time.Millisecond)

	for _, l := range srv.snapshot() {
		if strings.HasPrefix(l, "S:") || strings.HasPrefix(l, "R:") {
			t.Fatalf("expected no telemetry emissions, got %q", l)
		}
	}
}

// --- Helpers ---

func uniqueSQL(i int) string {
	return "SELECT " + itoa(i)
}

func itoa(i int) string {
	// Avoid pulling strconv just to make test SQL strings unique.
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	digits := []byte{}
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	if neg {
		return "-" + string(digits)
	}
	return string(digits)
}

func anyContains(lines []string, sub string) bool {
	for _, l := range lines {
		if strings.Contains(l, sub) {
			return true
		}
	}
	return false
}

func countContains(lines []string, sub string) int {
	c := 0
	for _, l := range lines {
		if strings.Contains(l, sub) {
			c++
		}
	}
	return c
}

func filterPrefix(lines []string, prefix string) []string {
	out := []string{}
	for _, l := range lines {
		if strings.HasPrefix(l, prefix) {
			out = append(out, l)
		}
	}
	return out
}

func anyMatch(lines []string, pred func(string) bool) bool {
	for _, l := range lines {
		if pred(l) {
			return true
		}
	}
	return false
}

// --- Disable native cache ---

func TestTelemetry_SnapshotDisabledOmittedByDefault(t *testing.T) {
	// disabled is JSON-omitted when false so older proxies / HQ
	// consumers that don't know the field don't see noise on the wire.
	cache, _, _ := makeTelemetryCache(t, 16)
	snap := cache.buildSnapshot()
	body, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(body), `"disabled"`) {
		t.Errorf("disabled must be omitted when false, got %s", body)
	}
}

func TestTelemetry_SnapshotDisabledTrueWhenSet(t *testing.T) {
	cache, _, _ := makeTelemetryCache(t, 16)
	cache.SetDisableNativeCache(true)
	snap := cache.buildSnapshot()
	if !snap.Disabled {
		t.Fatal("expected snapshot.Disabled=true after SetDisableNativeCache(true)")
	}
	body, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(body), `"disabled":true`) {
		t.Errorf("expected disabled:true in snapshot JSON, got %s", body)
	}
}
