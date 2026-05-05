package goldlapel

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	ddlSentinel             = "__ddl__"
	DefaultInvalidationPort = 7934

	// --- L1 telemetry tuning ---
	//
	// Demand-driven model (2026-05-02): the wrapper has NO background timer.
	// Cache counters increment on cache ops (free); state-change events are
	// emitted synchronously when a relevant counter crosses a threshold;
	// snapshot replies are sent only when the proxy asks via ?:<request>.
	//
	// Eviction-rate sliding window. cache_full fires when ≥ evictRateHigh
	// of the last evictRateWindow cache writes (puts) caused an eviction;
	// cache_recovered fires when the rate falls back below evictRateLow.
	// With a 32k-entry default capacity, a steady-state high eviction rate
	// means the working set exceeds the cache — actionable signal for the
	// dashboard.
	evictRateWindow = 200
	evictRateHigh   = 0.5 // 50% of recent puts evicted → cache_full
	evictRateLow    = 0.1 // ≤ 10% → cache_recovered
)

var (
	txStartRe = regexp.MustCompile(`(?i)^\s*(BEGIN|START\s+TRANSACTION)\b`)
	txEndRe   = regexp.MustCompile(`(?i)^\s*(COMMIT|ROLLBACK|END)\b`)

	tablePattern = regexp.MustCompile(`(?i)\b(?:FROM|JOIN)\s+(?:ONLY\s+)?(?:(\w+)\.)?(\w+)`)

	sqlKeywords = map[string]bool{
		"select": true, "from": true, "where": true, "and": true, "or": true,
		"not": true, "in": true, "exists": true, "between": true, "like": true,
		"is": true, "null": true, "true": true, "false": true, "as": true,
		"on": true, "left": true, "right": true, "inner": true, "outer": true,
		"cross": true, "full": true, "natural": true, "group": true, "order": true,
		"having": true, "limit": true, "offset": true, "union": true, "intersect": true,
		"except": true, "all": true, "distinct": true, "lateral": true, "values": true,
	}
)

type cacheEntry struct {
	Rows   interface{}
	Fields interface{}
	Tables map[string]bool
}

type NativeCache struct {
	cache        map[string]*cacheEntry
	tableIndex   map[string]map[string]bool
	accessOrder  map[string]uint64
	counter      uint64
	maxEntries   int
	enabled      bool
	// disabled is the explicit WithDisableNativeCache toggle — orthogonal to
	// `enabled` (which is the GOLDLAPEL_NATIVE_CACHE env-var kill-switch)
	// and to maxEntries. When true, Get always returns nil (incrementing
	// misses, never hits, never evictions) and Put is a silent no-op.
	// The invalidation goroutine continues to run so telemetry signal
	// flow (wrapper_connected / snapshot replies) keeps working — Manor
	// and the dashboard still need to see the wrapper even when the
	// native cache is off.
	disabled     bool
	mu           sync.Mutex
	invConnected bool
	invStop      chan struct{}
	invRunning   bool
	invPort      int
	invConn      net.Conn
	reconnectN   int

	// Counters bumped on the cache hot path. statsHits / statsMisses /
	// statsInvalidations are read concurrently by external callers
	// (StatsHits()/etc.) and must use atomics to avoid a race with
	// ongoing puts/gets that read other fields under nc.mu. statsEvictions
	// is a peer counter — same access pattern, same atomic treatment.
	// The eviction-rate sliding window is a multi-field structure that
	// is read+mutated together, so it stays under nc.mu.
	statsHits          int64
	statsMisses        int64
	statsInvalidations int64
	statsEvictions     int64

	// --- L1 telemetry (2026-05-03) ---

	// Stable wrapper identity for the lifetime of the process. Lets the
	// proxy aggregate per-wrapper stats across reconnects.
	wrapperID      string
	wrapperLang    string
	wrapperVersion string

	// Opt-out switch: GOLDLAPEL_REPORT_STATS=false (or ReportStats=false
	// in goldlapel.Start options) suppresses every emit path. The cache
	// continues to function (invalidation thread still runs; only
	// telemetry output is suppressed).
	reportStats bool

	// Send-side serialization. The recv goroutine emits R: replies and
	// the cache-op caller goroutines emit S: events; without serialization
	// concurrent conn.Write() calls could interleave bytes. sendMu
	// guards every line write to invConn.
	sendMu sync.Mutex

	// sendFunc is a test seam: when non-nil, telemetry lines are routed
	// to it instead of the live socket. Production code leaves it nil
	// and writes go to invConn under sendMu. Tests inject an
	// appendingFunc to capture emissions deterministically.
	sendFunc func(string) error

	// Eviction-rate sliding window — bounded ring of length evictRateWindow,
	// 1 = put caused an eviction, 0 = put inserted without eviction.
	// O(1) amortised: append until full, then overwrite oldest.
	recentEvictions    []int
	recentEvictionsIdx int

	// Latched "cache is full" flag. Hysteresis: only flips on transition,
	// so a sustained-bad rate emits exactly one cache_full, not one per
	// put. cache_recovered flips it back when the rate falls below
	// evictRateLow.
	stateCacheFull bool

	// Signal-handler goroutine state. signalCancel terminates the goroutine
	// when the cache is reset / invalidation stops; the goroutine emits
	// wrapper_disconnected on SIGINT/SIGTERM. signalOnce ensures we install
	// the handler exactly once per cache instance, regardless of how many
	// times ConnectInvalidation is called.
	signalCancel context.CancelFunc
	signalOnce   sync.Once
}

var (
	cacheInstance   *NativeCache
	cacheInstanceMu sync.Mutex
)

func GetNativeCache() *NativeCache {
	cacheInstanceMu.Lock()
	defer cacheInstanceMu.Unlock()
	if cacheInstance == nil {
		cacheInstance = newNativeCache()
	}
	return cacheInstance
}

func ResetNativeCache() {
	cacheInstanceMu.Lock()
	defer cacheInstanceMu.Unlock()
	if cacheInstance != nil {
		cacheInstance.StopInvalidation()
		cacheInstance = nil
	}
}

func newNativeCache() *NativeCache {
	maxEntries := 32768
	if s := os.Getenv("GOLDLAPEL_NATIVE_CACHE_SIZE"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			maxEntries = n
		}
	}
	enabled := true
	if s := os.Getenv("GOLDLAPEL_NATIVE_CACHE"); strings.EqualFold(s, "false") {
		enabled = false
	}
	// Telemetry opt-out (env). The Options-layer ReportStats override is
	// applied after construction via SetReportStats; this default honours
	// the env var when no option was passed.
	reportStats := true
	if s := os.Getenv("GOLDLAPEL_REPORT_STATS"); strings.EqualFold(s, "false") {
		reportStats = false
	}
	return &NativeCache{
		cache:          make(map[string]*cacheEntry),
		tableIndex:     make(map[string]map[string]bool),
		accessOrder:    make(map[string]uint64),
		maxEntries:     maxEntries,
		enabled:        enabled,
		wrapperID:      newWrapperID(),
		wrapperLang:    "go",
		wrapperVersion: telemetryVersion(),
		reportStats:    reportStats,
	}
}

// newWrapperID returns a fresh UUID v4 string for use as the stable
// wrapper identity. Hand-rolled from crypto/rand to avoid pulling in
// google/uuid as a dependency (the wrapper's go.mod intentionally
// minimises external deps). RFC 4122 §4.4 layout: byte 6 high nibble
// 0x40 (version), byte 8 high two bits 0x80 (variant 1).
func newWrapperID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand should never fail on supported platforms; on
		// the off-chance it does, time-mix something so two wrappers
		// in the same process don't share an empty ID.
		ts := time.Now().UnixNano()
		for i := 0; i < 8; i++ {
			b[i] = byte(ts >> (i * 8))
		}
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 1 (RFC 4122)
	hexStr := hex.EncodeToString(b[:])
	return hexStr[0:8] + "-" + hexStr[8:12] + "-" + hexStr[12:16] + "-" + hexStr[16:20] + "-" + hexStr[20:32]
}

// telemetryVersion returns the wrapper version string for the snapshot
// `version` field. Mirrors WrapperVersion()'s debug.ReadBuildInfo() lookup
// but maps the dev fallback ("0.0.0" in WrapperVersion, used to keep the
// application_name marker non-empty) to the more honest "unknown" — the
// telemetry doc explicitly calls this out: "fall back to unknown".
//
// debug.ReadBuildInfo() yields a real version only when the wrapper is
// imported as a tagged module by a downstream consumer. In local dev
// (working tree, `go test ./...`) info.Main.Version is "(devel)" and
// info.Deps doesn't include this module, so we fall through to "unknown".
func telemetryVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		if info.Main.Path == "github.com/goldlapel/goldlapel-go" && info.Main.Version != "" && info.Main.Version != "(devel)" {
			return strings.TrimPrefix(info.Main.Version, "v")
		}
		for _, dep := range info.Deps {
			if dep == nil {
				continue
			}
			if dep.Path == "github.com/goldlapel/goldlapel-go" && dep.Version != "" && dep.Version != "(devel)" {
				return strings.TrimPrefix(dep.Version, "v")
			}
		}
	}
	return "unknown"
}

// SetReportStats overrides the GOLDLAPEL_REPORT_STATS env-derived default.
// Wired through goldlapel.Start's WithReportStats option so library users
// can disable telemetry programmatically without touching the environment.
// Safe to call before or after ConnectInvalidation; the new value takes
// effect on the next emit.
func (nc *NativeCache) SetReportStats(enabled bool) {
	nc.mu.Lock()
	nc.reportStats = enabled
	nc.mu.Unlock()
}

// ReportStatsEnabled reports whether telemetry emissions are currently
// enabled. Exposed for tests; callers shouldn't need to gate on this in
// production code.
func (nc *NativeCache) ReportStatsEnabled() bool {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	return nc.reportStats
}

// SetDisableNativeCache toggles the explicit native-cache no-op
// pass-through. Wired through goldlapel.Start's WithDisableNativeCache
// option so library users can disable the native cache without losing
// their tuned cache size (passing WithConfig("proxy_cache_size", 0)
// would force them to). Safe to call before or after
// ConnectInvalidation; the new value takes effect on the next Get/Put.
func (nc *NativeCache) SetDisableNativeCache(disable bool) {
	nc.mu.Lock()
	nc.disabled = disable
	nc.mu.Unlock()
}

// DisableNativeCache reports whether the native cache is currently in
// no-op pass-through mode. Exposed primarily for tests; production
// callers shouldn't need to gate on this — Get/Put already
// short-circuit internally.
func (nc *NativeCache) DisableNativeCache() bool {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	return nc.disabled
}

// WrapperID returns the stable UUID v4 identifier this cache uses in
// telemetry snapshots. Generated once at construction and stable for the
// process lifetime.
func (nc *NativeCache) WrapperID() string {
	return nc.wrapperID
}

func (nc *NativeCache) Connected() bool {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	return nc.invConnected
}

func (nc *NativeCache) Enabled() bool {
	return nc.enabled
}

// StatsHits returns the number of cache hits.
func (nc *NativeCache) StatsHits() int64 {
	return atomic.LoadInt64(&nc.statsHits)
}

// StatsMisses returns the number of cache misses.
func (nc *NativeCache) StatsMisses() int64 {
	return atomic.LoadInt64(&nc.statsMisses)
}

// StatsInvalidations returns the number of cache invalidations.
func (nc *NativeCache) StatsInvalidations() int64 {
	return atomic.LoadInt64(&nc.statsInvalidations)
}

// StatsEvictions returns the number of cache evictions (entries removed
// from the LRU because the cache reached its capacity).
func (nc *NativeCache) StatsEvictions() int64 {
	return atomic.LoadInt64(&nc.statsEvictions)
}

func (nc *NativeCache) Size() int {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	return len(nc.cache)
}

// Get looks up a cached entry for (sql, args, stateHash). The stateHash is
// the wrapper's per-connection unsafe-GUC fingerprint (see guc_state.go);
// pass 0 for the baseline state. Two connections with different unsafe
// GUC settings (e.g. SET app.user_id='42' vs '43') hash to different
// stateHash values and therefore never share a cache slot — this is the
// wrapper-side mirror of the proxy's Option Y GUC-RLS cache safety.
func (nc *NativeCache) Get(sql string, args []interface{}, stateHash uint64) *cacheEntry {
	if !nc.enabled {
		return nil
	}
	nc.mu.Lock()
	if !nc.invConnected {
		nc.mu.Unlock()
		return nil
	}
	// disabled native cache: tick misses (callers measure miss rate), never hit.
	// Skip the key-build + cache lookup entirely — no point. Hits and
	// evictions stay at zero by definition; only misses move.
	if nc.disabled {
		nc.mu.Unlock()
		atomic.AddInt64(&nc.statsMisses, 1)
		return nil
	}
	defer nc.mu.Unlock()
	key := makeKey(sql, args, stateHash)
	entry, ok := nc.cache[key]
	if ok {
		nc.counter++
		nc.accessOrder[key] = nc.counter
		atomic.AddInt64(&nc.statsHits, 1)
		return entry
	}
	atomic.AddInt64(&nc.statsMisses, 1)
	return nil
}

// Put inserts or refreshes a cache entry for (sql, args, stateHash). See
// Get for the meaning of stateHash; pass 0 for the baseline state.
func (nc *NativeCache) Put(sql string, args []interface{}, stateHash uint64, rows interface{}, fields interface{}) {
	if !nc.enabled {
		return
	}
	nc.mu.Lock()
	if !nc.invConnected {
		nc.mu.Unlock()
		return
	}
	// disabled native cache: silent no-op. Don't touch cache state, don't
	// touch the eviction-rate window, don't bump counters — the layer is off.
	if nc.disabled {
		nc.mu.Unlock()
		return
	}
	key := makeKey(sql, args, stateHash)
	tables := extractTables(sql)
	evicted := 0
	if _, exists := nc.cache[key]; !exists && len(nc.cache) >= nc.maxEntries {
		nc.evictOne()
		evicted = 1
	}
	nc.cache[key] = &cacheEntry{Rows: rows, Fields: fields, Tables: tables}
	nc.counter++
	nc.accessOrder[key] = nc.counter
	for table := range tables {
		if nc.tableIndex[table] == nil {
			nc.tableIndex[table] = make(map[string]bool)
		}
		nc.tableIndex[table][key] = true
	}
	nc.recordEvictionLocked(evicted)
	nc.mu.Unlock()
	// Eviction-rate threshold check happens outside the lock — emit may
	// take sendMu and we don't want to nest locks (the recv goroutine
	// can hold sendMu while attempting a write that blocks on a slow
	// peer; nesting it under nc.mu would freeze every cache op).
	nc.maybeEmitEvictionRateStateChange()
}

func (nc *NativeCache) InvalidateTable(table string) {
	table = strings.ToLower(table)
	nc.mu.Lock()
	defer nc.mu.Unlock()
	keys, ok := nc.tableIndex[table]
	if !ok {
		return
	}
	delete(nc.tableIndex, table)
	for key := range keys {
		entry, entryOk := nc.cache[key]
		delete(nc.cache, key)
		delete(nc.accessOrder, key)
		if entryOk && entry != nil {
			for otherTable := range entry.Tables {
				if otherTable != table {
					if otherKeys, ok2 := nc.tableIndex[otherTable]; ok2 {
						delete(otherKeys, key)
						if len(otherKeys) == 0 {
							delete(nc.tableIndex, otherTable)
						}
					}
				}
			}
		}
	}
	atomic.AddInt64(&nc.statsInvalidations, int64(len(keys)))
}

func (nc *NativeCache) InvalidateAll() {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	count := int64(len(nc.cache))
	nc.cache = make(map[string]*cacheEntry)
	nc.tableIndex = make(map[string]map[string]bool)
	nc.accessOrder = make(map[string]uint64)
	atomic.AddInt64(&nc.statsInvalidations, count)
}

func (nc *NativeCache) ConnectInvalidation(port int) {
	nc.mu.Lock()
	if nc.invRunning {
		nc.mu.Unlock()
		return
	}
	nc.invPort = port
	nc.invStop = make(chan struct{})
	nc.invRunning = true
	nc.reconnectN = 0
	nc.mu.Unlock()

	// Install the SIGINT/SIGTERM listener once per cache — emits a final
	// wrapper_disconnected on graceful shutdown. signalOnce makes
	// ConnectInvalidation safe to call after a Stop/Connect cycle (the
	// handler from the previous cycle was cancelled in StopInvalidation;
	// we won't re-register, but a fresh cache from ResetNativeCache gets
	// its own handler).
	nc.installSignalHandler()

	go nc.invalidationLoop()
}

func (nc *NativeCache) StopInvalidation() {
	nc.mu.Lock()
	if !nc.invRunning {
		nc.mu.Unlock()
		return
	}
	close(nc.invStop)
	nc.invRunning = false
	nc.invConnected = false
	nc.mu.Unlock()
	// Clear invConn under sendMu so a concurrent emitter on a cache-op
	// goroutine can't grab the FD reference between us nilling it and
	// closing it.
	nc.sendMu.Lock()
	conn := nc.invConn
	nc.invConn = nil
	nc.sendMu.Unlock()
	if conn != nil {
		conn.Close()
	}
	nc.stopSignalHandler()
}

func (nc *NativeCache) invalidationLoop() {
	port := nc.invPort
	sockPath := fmt.Sprintf("/tmp/goldlapel-%d.sock", port)

	for {
		select {
		case <-nc.invStop:
			return
		default:
		}

		var conn net.Conn

		// Try Unix socket first (faster), fall back to TCP
		if runtime.GOOS != "windows" {
			if _, statErr := os.Stat(sockPath); statErr == nil {
				conn, _ = net.DialTimeout("unix", sockPath, 5*time.Second)
			}
		}
		if conn == nil {
			var err error
			conn, err = net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)
			if err != nil {
				nc.scheduleReconnect()
				continue
			}
		}

		nc.mu.Lock()
		nc.invConnected = true
		nc.reconnectN = 0
		nc.mu.Unlock()
		// invConn is read under sendMu in sendLine and cleared under sendMu
		// in StopInvalidation / the post-read cleanup below; assign it
		// under sendMu too so the write/read pair synchronizes (the race
		// detector tolerates the previous layout because tests don't
		// observe simultaneous set+read, but the data race is still
		// real if a future caller emits before invalidationLoop has
		// finished setting up).
		nc.sendMu.Lock()
		nc.invConn = conn
		nc.sendMu.Unlock()

		// Emit wrapper_connected synchronously on the loop goroutine.
		// invConn is set above under sendMu so sendLine writes to the
		// live FD; the L1 telemetry pattern guarantees this is the
		// first line on the wire after a successful connect.
		nc.emitStateChange("wrapper_connected")

		nc.readInvalidations(conn)

		// Clear the socket reference under sendMu so any concurrent
		// emitter on a cache-op goroutine doesn't write to the FD we're
		// about to close. The Python wrapper has the same dance — without
		// it, sendLine could observe a non-nil conn, the loop closes it,
		// and the subsequent Write() lands on a closed FD (best-case a
		// swallowed error, worst-case a panic on platforms that map
		// closed FDs aggressively).
		nc.sendMu.Lock()
		if nc.invConn == conn {
			nc.invConn = nil
		}
		nc.sendMu.Unlock()

		nc.mu.Lock()
		wasConnected := nc.invConnected
		nc.invConnected = false
		nc.mu.Unlock()

		conn.Close()

		if wasConnected {
			nc.InvalidateAll()
		}

		nc.scheduleReconnect()
	}
}

func (nc *NativeCache) readInvalidations(conn net.Conn) {
	buf := make([]byte, 4096)
	var partial string

	for {
		select {
		case <-nc.invStop:
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := conn.Read(buf)
		if n > 0 {
			partial += string(buf[:n])
			for {
				idx := strings.IndexByte(partial, '\n')
				if idx < 0 {
					break
				}
				line := partial[:idx]
				partial = partial[idx+1:]
				nc.processSignal(line)
			}
		}
		if err != nil {
			return
		}
	}
}

func (nc *NativeCache) processSignal(line string) {
	// Backwards/forwards-compat: unknown prefixes are silently ignored.
	// The proxy may add new request types over time; older wrappers
	// must not crash on them.
	if strings.HasPrefix(line, "I:") {
		table := strings.TrimSpace(line[2:])
		if table == "*" {
			nc.InvalidateAll()
		} else {
			nc.InvalidateTable(table)
		}
		return
	}
	if strings.HasPrefix(line, "?:") {
		nc.processRequest(line[2:])
		return
	}
	// P: keepalive, C: config, anything else — ignored.
}

// processRequest handles ?:<request> from the proxy. Today the only
// recognised body is "snapshot" (or empty, treated as snapshot). The
// proxy may extend this with new request types; unrecognised bodies are
// silently dropped — only the proxy that issued the request is waiting
// on a reply, so the contract is local to each request type.
func (nc *NativeCache) processRequest(raw string) {
	body := strings.TrimSpace(raw)
	if body == "" || body == "snapshot" {
		nc.emitResponse()
	}
}

func (nc *NativeCache) scheduleReconnect() {
	nc.mu.Lock()
	delay := 1 << nc.reconnectN
	if delay > 15 {
		delay = 15
	}
	nc.reconnectN++
	stopCh := nc.invStop
	nc.mu.Unlock()

	select {
	case <-stopCh:
	case <-time.After(time.Duration(delay) * time.Second):
	}
}

func (nc *NativeCache) evictOne() {
	if len(nc.accessOrder) == 0 {
		return
	}
	var lruKey string
	var minCounter uint64 = ^uint64(0)
	for key, cnt := range nc.accessOrder {
		if cnt < minCounter {
			minCounter = cnt
			lruKey = key
		}
	}
	entry := nc.cache[lruKey]
	delete(nc.cache, lruKey)
	delete(nc.accessOrder, lruKey)
	if entry != nil {
		for table := range entry.Tables {
			if keys, ok := nc.tableIndex[table]; ok {
				delete(keys, lruKey)
				if len(keys) == 0 {
					delete(nc.tableIndex, table)
				}
			}
		}
	}
	atomic.AddInt64(&nc.statsEvictions, 1)
}

// --- L1 telemetry: sliding window + snapshot + emission ---

// recordEvictionLocked appends a put outcome (1 evicted, 0 inserted) to
// the sliding window. Caller must hold nc.mu. Bounded ring of length
// evictRateWindow; once at capacity it overwrites the oldest entry in
// O(1). The window's purpose is to detect a sustained imbalance between
// the working set and cache capacity.
func (nc *NativeCache) recordEvictionLocked(evicted int) {
	if len(nc.recentEvictions) < evictRateWindow {
		nc.recentEvictions = append(nc.recentEvictions, evicted)
		return
	}
	nc.recentEvictions[nc.recentEvictionsIdx] = evicted
	nc.recentEvictionsIdx = (nc.recentEvictionsIdx + 1) % evictRateWindow
}

// snapshot is the JSON shape the proxy aggregates per-tick. The proxy
// parses these structurally; field names + types must match the
// canonical pattern across all wrapper languages exactly. The State
// field is populated only on S: emissions. Disabled is surfaced only
// when the wrapper has explicit WithDisableNativeCache(true) — older
// proxies that don't know the field will simply ignore it; HQ/Manor
// consumers that do know it can render the wrapper's native-cache
// state correctly. The field lives nested under native_cache.wrappers[]
// in aggregated wire shape, so the short name disambiguates by context.
type snapshot struct {
	WrapperID          string `json:"wrapper_id"`
	Lang               string `json:"lang"`
	Version            string `json:"version"`
	Hits               int64  `json:"hits"`
	Misses             int64  `json:"misses"`
	Evictions          int64  `json:"evictions"`
	Invalidations      int64  `json:"invalidations"`
	CurrentSizeEntries int    `json:"current_size_entries"`
	CapacityEntries    int    `json:"capacity_entries"`
	TsMs               int64  `json:"ts_ms"`
	State              string `json:"state,omitempty"`
	Disabled           bool   `json:"disabled,omitempty"`
}

// buildSnapshot reads counters + size in a single critical section so
// the snapshot is internally consistent (no torn reads where, e.g., hits
// and misses straddle a concurrent get()). The proxy computes deltas
// across ticks; we just expose the raw counters.
func (nc *NativeCache) buildSnapshot() snapshot {
	nc.mu.Lock()
	size := len(nc.cache)
	disabled := nc.disabled
	nc.mu.Unlock()
	return snapshot{
		WrapperID:          nc.wrapperID,
		Lang:               nc.wrapperLang,
		Version:            nc.wrapperVersion,
		Hits:               atomic.LoadInt64(&nc.statsHits),
		Misses:             atomic.LoadInt64(&nc.statsMisses),
		Evictions:          atomic.LoadInt64(&nc.statsEvictions),
		Invalidations:      atomic.LoadInt64(&nc.statsInvalidations),
		CurrentSizeEntries: size,
		CapacityEntries:    nc.maxEntries,
		Disabled:           disabled,
	}
}

// sendLine serialises a line write under sendMu. Best-effort: socket
// errors are swallowed (the recv loop will detect the broken connection
// on its next iteration and reconnect). The sendFunc test seam shortcuts
// the socket entirely so unit tests can capture emissions deterministically.
func (nc *NativeCache) sendLine(line string) {
	nc.mu.Lock()
	enabled := nc.reportStats
	sf := nc.sendFunc
	nc.mu.Unlock()
	if !enabled {
		return
	}
	if sf != nil {
		_ = sf(line)
		return
	}
	if !strings.HasSuffix(line, "\n") {
		line += "\n"
	}
	nc.sendMu.Lock()
	conn := nc.invConn
	if conn == nil {
		nc.sendMu.Unlock()
		return
	}
	_, _ = conn.Write([]byte(line))
	nc.sendMu.Unlock()
}

// emitStateChange sends S:<json> with the current snapshot annotated
// with the supplied state name + a fresh ts_ms. No-op when reportStats
// is disabled.
func (nc *NativeCache) emitStateChange(state string) {
	if !nc.ReportStatsEnabled() {
		return
	}
	snap := nc.buildSnapshot()
	snap.State = state
	snap.TsMs = time.Now().UnixMilli()
	body, err := json.Marshal(snap)
	if err != nil {
		return
	}
	nc.sendLine("S:" + string(body))
}

// emitResponse sends R:<json> in reply to a ?:<request>. Same shape as
// S: but without the state field.
func (nc *NativeCache) emitResponse() {
	if !nc.ReportStatsEnabled() {
		return
	}
	snap := nc.buildSnapshot()
	snap.TsMs = time.Now().UnixMilli()
	body, err := json.Marshal(snap)
	if err != nil {
		return
	}
	nc.sendLine("R:" + string(body))
}

// maybeEmitEvictionRateStateChange checks the eviction-rate sliding
// window and emits a state change when the latched flag should flip.
// Hysteresis: rates above evictRateHigh trip cache_full; rates below
// evictRateLow trip cache_recovered; rates between leave the latched
// state unchanged (no flapping near the boundary). A warmup gate
// requires a full window of samples before any emission — a single
// eviction in 3 puts is noise, not signal.
func (nc *NativeCache) maybeEmitEvictionRateStateChange() {
	var emit string
	nc.mu.Lock()
	n := len(nc.recentEvictions)
	if n < evictRateWindow {
		nc.mu.Unlock()
		return
	}
	sum := 0
	for _, v := range nc.recentEvictions {
		sum += v
	}
	rate := float64(sum) / float64(n)
	if !nc.stateCacheFull && rate >= evictRateHigh {
		nc.stateCacheFull = true
		emit = "cache_full"
	} else if nc.stateCacheFull && rate <= evictRateLow {
		nc.stateCacheFull = false
		emit = "cache_recovered"
	}
	nc.mu.Unlock()
	if emit != "" {
		nc.emitStateChange(emit)
	}
}

// EmitWrapperDisconnected emits a final wrapper_disconnected snapshot
// before shutdown. Best-effort — the socket may already be torn down,
// in which case sendLine swallows the write error and the proxy will
// time the wrapper out anyway.
func (nc *NativeCache) EmitWrapperDisconnected() {
	nc.emitStateChange("wrapper_disconnected")
}

// installSignalHandler arranges for EmitWrapperDisconnected() to fire
// on SIGINT/SIGTERM. Polite-citizen design: we register an additional
// listener via signal.Notify, NOT signal.Reset — if the user installed
// their own handlers (or a framework like cobra/cli did), ours runs in
// parallel without preventing theirs from observing the signal. After
// emitting, the goroutine exits without calling os.Exit() so the rest
// of the process's shutdown sequence (defer chains, parent
// signal.Notify listeners) runs to completion.
//
// Idempotent via signalOnce — repeated ConnectInvalidation calls don't
// stack handlers. Cancellation via signalCancel terminates the goroutine
// when the cache is reset / invalidation stops.
func (nc *NativeCache) installSignalHandler() {
	nc.signalOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		nc.mu.Lock()
		nc.signalCancel = cancel
		nc.mu.Unlock()

		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)

		go func() {
			// signal.Stop is the documented way to detach our listener
			// from the signal source so the runtime stops trying to
			// deliver to a goroutine that's about to exit. Don't
			// close(ch) afterwards — signal.Stop drains pending
			// deliveries; closing the channel would race the runtime.
			defer signal.Stop(ch)
			select {
			case <-ch:
				nc.EmitWrapperDisconnected()
			case <-ctx.Done():
				return
			}
		}()
	})
}

// stopSignalHandler cancels the signal-handling goroutine if one is
// running. Called from StopInvalidation so a cache that was started
// then stopped doesn't leak the goroutine. Safe to call when no
// handler was installed (signalCancel is nil).
func (nc *NativeCache) stopSignalHandler() {
	nc.mu.Lock()
	cancel := nc.signalCancel
	nc.signalCancel = nil
	nc.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// --- SQL parsing functions ---

// makeKey builds the per-entry cache key from (sql, args, stateHash). The
// stateHash is the wrapper's per-connection unsafe-GUC fingerprint
// (Option Y, mirroring the proxy's src/guc_state.rs). Folding it into the
// key prevents one connection's RLS-shaped result set from being served
// to a peer connection with different unsafe-GUC settings — e.g. user A's
// rows leaking to user B when both share the wrapper's NativeCache
// singleton. A stateHash of 0 means "no unsafe GUCs set" and matches the
// baseline shared slot.
func makeKey(sql string, args []interface{}, stateHash uint64) string {
	hashHex := strconv.FormatUint(stateHash, 16)
	if len(args) == 0 {
		return hashHex + "\x00" + sql + "\x00"
	}
	return hashHex + "\x00" + sql + "\x00" + fmt.Sprint(args)
}

func detectWrite(sql string) string {
	trimmed := strings.TrimSpace(sql)
	tokens := strings.Fields(trimmed)
	if len(tokens) == 0 {
		return ""
	}
	first := strings.ToUpper(tokens[0])

	switch first {
	case "INSERT":
		if len(tokens) < 3 || !strings.EqualFold(tokens[1], "INTO") {
			return ""
		}
		return bareTable(tokens[2])
	case "UPDATE":
		if len(tokens) < 2 {
			return ""
		}
		return bareTable(tokens[1])
	case "DELETE":
		if len(tokens) < 3 || !strings.EqualFold(tokens[1], "FROM") {
			return ""
		}
		return bareTable(tokens[2])
	case "TRUNCATE":
		if len(tokens) < 2 {
			return ""
		}
		if strings.EqualFold(tokens[1], "TABLE") {
			if len(tokens) < 3 {
				return ""
			}
			return bareTable(tokens[2])
		}
		return bareTable(tokens[1])
	case "CREATE", "ALTER", "DROP", "REFRESH", "DO", "CALL":
		return ddlSentinel
	case "MERGE":
		if len(tokens) < 3 || !strings.EqualFold(tokens[1], "INTO") {
			return ""
		}
		return bareTable(tokens[2])
	case "SELECT":
		sawInto := false
		intoTarget := ""
		for _, tok := range tokens[1:] {
			upper := strings.ToUpper(tok)
			if upper == "INTO" && !sawInto {
				sawInto = true
				continue
			}
			if sawInto && intoTarget == "" {
				if upper == "TEMPORARY" || upper == "TEMP" || upper == "UNLOGGED" {
					continue
				}
				intoTarget = tok
				continue
			}
			if sawInto && intoTarget != "" && upper == "FROM" {
				return ddlSentinel
			}
			if upper == "FROM" {
				return ""
			}
		}
		return ""
	case "COPY":
		if len(tokens) < 2 {
			return ""
		}
		raw := tokens[1]
		if strings.HasPrefix(raw, "(") {
			return ""
		}
		tablePart := strings.SplitN(raw, "(", 2)[0]
		for _, tok := range tokens[2:] {
			upper := strings.ToUpper(tok)
			if upper == "FROM" {
				return bareTable(tablePart)
			}
			if upper == "TO" {
				return ""
			}
		}
		return ""
	case "WITH":
		restUpper := strings.ToUpper(trimmed[len(tokens[0]):])
		for _, token := range strings.Fields(restUpper) {
			word := strings.TrimLeft(token, "(")
			if word == "INSERT" || word == "UPDATE" || word == "DELETE" {
				return ddlSentinel
			}
		}
		return ""
	}

	return ""
}

func bareTable(raw string) string {
	table := strings.SplitN(raw, "(", 2)[0]
	parts := strings.Split(table, ".")
	table = parts[len(parts)-1]
	return strings.ToLower(table)
}

// detectWriteMulti runs detectWrite across every top-level segment of a
// possibly multi-statement SQL body. Returns the union of write tables
// (deduplicated) plus a sentinelHit flag set when any segment was DDL or
// CTE-with-INSERT/UPDATE/DELETE.
//
// This is the gap fixed by docs/todos/wrapper-multistatement-write-detection.md
// — single-statement detectWrite() looks at only the first token, so a body
// like `SET app.tenant='x'; INSERT INTO orders VALUES (1)` previously fell
// through to the read path and the INSERT never invalidated the orders cache.
//
// Fast path: when the SQL contains no ';' the splitter would return a
// single-element slice anyway, so we short-circuit directly to detectWrite
// and avoid the allocation.
func detectWriteMulti(sql string) (tables []string, sentinelHit bool) {
	if !strings.ContainsRune(sql, ';') {
		t := detectWrite(sql)
		if t == ddlSentinel {
			return nil, true
		}
		if t != "" {
			return []string{t}, false
		}
		return nil, false
	}
	seen := make(map[string]struct{})
	for _, seg := range SplitStatements(sql) {
		t := detectWrite(seg)
		if t == ddlSentinel {
			return nil, true
		}
		if t == "" {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		tables = append(tables, t)
	}
	return tables, false
}

// sessionStateCommands lists the first-token SQL keywords whose responses
// have no business in the cache: session/connection state mutations (SET /
// RESET), notification machinery (LISTEN / UNLISTEN / NOTIFY), and
// transaction control (BEGIN / COMMIT / ROLLBACK / SAVEPOINT / RELEASE /
// START / END / ABORT). Postgres returns command-tag-only responses for
// these — caching them bloats the LRU with empty entries that never serve
// real reads.
//
// Transaction-control keywords are also matched here as defence in depth;
// the Query/QueryRow paths already short-circuit isTxStart/isTxEnd before
// reaching the cache write step, but a future refactor that moves the put
// site shouldn't reintroduce the bug.
var sessionStateCommands = map[string]struct{}{
	"SET": {}, "RESET": {},
	"LISTEN": {}, "UNLISTEN": {}, "NOTIFY": {},
	"BEGIN": {}, "COMMIT": {}, "ROLLBACK": {},
	"SAVEPOINT": {}, "RELEASE": {},
	"START": {}, "END": {}, "ABORT": {},
}

// isSessionStateCommand returns true when sql's first non-whitespace token
// is a session-state / notification / transaction-control keyword whose
// response should never be cached. See sessionStateCommands for the full
// set and rationale. Multi-statement bodies are matched on their FIRST
// segment's first token only — a multi-statement body that begins with
// SELECT and ends with COMMIT is still a SELECT for caching purposes
// (and will be handled by the read path).
func isSessionStateCommand(sql string) bool {
	trimmed := strings.TrimLeft(sql, " \t\r\n")
	if trimmed == "" {
		return false
	}
	end := 0
	for end < len(trimmed) {
		c := trimmed[end]
		if c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == ';' {
			break
		}
		end++
	}
	first := strings.ToUpper(trimmed[:end])
	_, ok := sessionStateCommands[first]
	return ok
}

func extractTables(sql string) map[string]bool {
	tables := make(map[string]bool)
	matches := tablePattern.FindAllStringSubmatch(sql, -1)
	for _, m := range matches {
		table := strings.ToLower(m[2])
		if !sqlKeywords[table] {
			tables[table] = true
		}
	}
	return tables
}

func isTxStart(sql string) bool {
	return txStartRe.MatchString(sql)
}

func isTxEnd(sql string) bool {
	return txEndRe.MatchString(sql)
}
