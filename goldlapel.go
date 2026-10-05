// Package goldlapel is the Go wrapper for the Gold Lapel self-optimizing
// Postgres proxy. It spawns the bundled Rust binary as a subprocess, exposes
// the proxy connection string via URL(), and provides higher-level helpers
// (document store, full-text search, pub/sub, queues, counters, streams, ...)
// built on top of the standard database/sql package.
//
// Quick start:
//
//	import (
//	    "context"
//	    "database/sql"
//	    _ "github.com/jackc/pgx/v5/stdlib"  // any database/sql Postgres driver
//	    "github.com/goldlapel/goldlapel-go"
//	)
//
//	ctx := context.Background()
//	gl, err := goldlapel.Start(ctx, "postgresql://user:pass@db/mydb")
//	if err != nil { panic(err) }
//	defer gl.Stop(ctx)
//
//	db, _ := sql.Open("pgx", gl.URL())
//	defer db.Close()
//
//	hits, err := gl.Search(ctx, "articles", "body", "postgres tuning")
package goldlapel

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	DefaultProxyPort     = 7932
	DefaultDashboardPort = 7933
	startupTimeout       = 10 * time.Second
	startupPollInterval  = 50 * time.Millisecond
	shutdownTimeout      = 5 * time.Second
	dialTimeout          = 500 * time.Millisecond
)

var (
	// Regex patterns for URL parsing — same as all sibling wrappers.
	// We use regex instead of net/url to preserve percent-encoded characters.
	withPortRe    = regexp.MustCompile(`^(postgres(?:ql)?://(?:.*@)?)([^:/?#]+):(\d+)(.*)$`)
	withoutPortRe = regexp.MustCompile(`^(postgres(?:ql)?://(?:.*@)?)([^:/?#]+)(.*)$`)
	// Pre-existing application_name in a query string — match against either
	// `?application_name=` or `&application_name=`.
	appNameRe = regexp.MustCompile(`[?&]application_name=`)
	// The userinfo password of a URL, for redactPassword.
	passwordRe = regexp.MustCompile(`^([^:/?#]+://[^:/?#@]*:).*@`)

	// upstreamOnlyParams are connection parameters for the TLS / GSS hop to
	// the upstream. The proxy keeps using them (its --upstream URL is
	// unchanged) but they are stripped from the URL handed to the app: the
	// proxy declines TLS from the app unless started with its own
	// certificate, so e.g. ?sslmode=require would make every app
	// connection fail. Keys are matched case-insensitively.
	upstreamOnlyParams = map[string]bool{
		"sslmode": true, "sslcert": true, "sslkey": true, "sslrootcert": true,
		"sslcrl": true, "sslcrldir": true, "sslpassword": true, "sslsni": true,
		"sslnegotiation": true, "ssl_min_protocol_version": true,
		"ssl_max_protocol_version": true, "requiressl": true,
		"channel_binding": true, "gssencmode": true, "krbsrvname": true,
		"gsslib": true,
	}

	// validConfigKeys enumerates the tuning knobs still accepted inside the
	// structured `config` map. Top-level concepts (proxy_port, dashboard_port,
	// log_level, mode, license, client, config_file) are exposed via their own
	// With* functional options and are NOT valid keys here — passing them
	// through WithConfig raises at argv build time.
	validConfigKeys = map[string]bool{
		"min_pattern_count": true, "deep_pagination_threshold": true, "report_interval_secs": true,
		"proxy_cache_size": true, "batch_cache_size": true, "batch_cache_ttl_secs": true,
		"pool_size": true, "pool_timeout_secs": true, "pool_mode": true,
		"mgmt_idle_timeout": true, "fallback": true, "read_after_write_secs": true,
		"n1_threshold": true, "n1_window_ms": true, "n1_cross_threshold": true,
		"tls_cert": true, "tls_key": true, "tls_client_ca": true,
		"disable_btree_indexes": true, "disable_trigram_indexes": true,
		"disable_expression_indexes": true, "disable_partial_indexes": true,
		"disable_rewrite_prepared_cache": true, "disable_pool": true,
		"disable_n1": true, "disable_n1_cross_connection": true, "disable_coalescing": true,
		"replica": true, "exclude_tables": true,
	}

	booleanKeys = map[string]bool{
		"disable_btree_indexes": true, "disable_trigram_indexes": true,
		"disable_expression_indexes": true, "disable_partial_indexes": true,
		"disable_rewrite_prepared_cache": true, "disable_pool": true,
		"disable_n1": true, "disable_n1_cross_connection": true, "disable_coalescing": true,
	}

	listKeys = map[string]bool{
		"replica":        true,
		"exclude_tables": true,
	}

	// removedConfigKeys are former config keys, mapped to why they went, so
	// a stale caller gets a better answer than "unknown".
	removedConfigKeys = map[string]string{
		"invalidation_port":     "was removed with the in-process cache",
		"native_cache_size":     "was removed with the in-process cache",
		"disable_native_cache":  "was removed with the in-process cache",
		"aggressive_verify":     "was removed with the in-process cache",
		"report_stats":          "was removed with the in-process cache",
		"disable_matviews":      "was removed with the proxy's materialized views",
		"refresh_interval_secs": "was removed with the proxy's materialized views",
		"pattern_ttl_secs":      "was removed with the proxy's materialized views",
		"max_tables_per_view":   "was removed with the proxy's materialized views",
		"max_columns_per_view":  "was removed with the proxy's materialized views",
		"disable_consolidation": "was removed with the proxy's materialized views",
		"disable_rewrite":       "was removed with the proxy's materialized views",
		"disable_shadow_mode":   "was removed with the proxy's materialized views",
		"enable_coalescing":     "was replaced by disable_coalescing (coalescing is on by default)",
	}
)

// --- Options ---
//
// Options form a single Option interface that covers both construction-time
// options (passed to Start) and per-call options (passed to individual
// methods like DocInsert). Internally an Option can set fields on the
// GoldLapel struct before it boots (startOption) and/or override per-call
// state such as the transaction target (callOption). Any given option
// implements only the facets it cares about; irrelevant facets are no-ops.

// Option is the unified functional-options type accepted by Start and by
// every receiver method. Implementations typically set construction
// parameters (e.g. WithProxyPort) or per-call overrides (e.g. WithTx). A
// single Option may participate in any combination of the four facets:
//
//	applyStart  — set fields on *GoldLapel before spawn (WithProxyPort, WithConfig)
//	applyCall   — override per-call state such as the tx target (WithTx)
//	applySearch — populate search-specific options (WithLimit, WithLang, ...)
//	applyDoc    — populate DocFind-specific options (DocSort, DocLimit, ...)
//
// Implementations no-op the facets they do not care about. This keeps a
// single pipe of options flowing through every method in the API.
type Option interface {
	applyStart(*GoldLapel)
	applyCall(*callOptions)
	applySearch(*searchOptions)
	applyDoc(*docFindOptions)
}

// SearchOption is retained as an alias for Option so existing callers that
// named the type (e.g. when holding options in a variable) keep compiling.
// Every SearchOption is an Option and vice-versa.
type SearchOption = Option

// DocFindOption is retained as an alias for Option for the same reason.
type DocFindOption = Option

// startOnly is an Option that only affects construction.
type startOnly func(*GoldLapel)

func (f startOnly) applyStart(gl *GoldLapel)   { f(gl) }
func (f startOnly) applyCall(*callOptions)     {}
func (f startOnly) applySearch(*searchOptions) {}
func (f startOnly) applyDoc(*docFindOptions)   {}

// callOnly is an Option that only affects a single method call.
type callOnly func(*callOptions)

func (f callOnly) applyStart(*GoldLapel)      {}
func (f callOnly) applyCall(o *callOptions)   { f(o) }
func (f callOnly) applySearch(*searchOptions) {}
func (f callOnly) applyDoc(*docFindOptions)   {}

// searchOnly is an Option that only affects search-specific options.
type searchOnly func(*searchOptions)

func (f searchOnly) applyStart(*GoldLapel)        {}
func (f searchOnly) applyCall(*callOptions)       {}
func (f searchOnly) applySearch(o *searchOptions) { f(o) }
func (f searchOnly) applyDoc(*docFindOptions)     {}

// docOnly is an Option that only affects DocFind-specific options.
type docOnly func(*docFindOptions)

func (f docOnly) applyStart(*GoldLapel)      {}
func (f docOnly) applyCall(*callOptions)     {}
func (f docOnly) applySearch(*searchOptions) {}
func (f docOnly) applyDoc(o *docFindOptions) { f(o) }

// callOptions collects per-call overrides. Currently only a transaction
// target, but this is the hook for future per-call options.
type callOptions struct {
	tx *sql.Tx
}

// WithProxyPort sets the proxy listen port. When unset (or 0), Start picks
// the smallest free port from 7932 up whose dashboard port (port + 1) is
// free too, skipping ports this process's other proxies hold. An explicit
// port another proxy of this process holds is an error. Construction-time
// only.
func WithProxyPort(port int) Option {
	return startOnly(func(gl *GoldLapel) {
		gl.proxyPort = port
	})
}

// WithDashboardPort sets the dashboard listen port. When unset, the port is
// derived as proxy_port + 1. Set to 0 to disable the dashboard entirely.
// Construction-time only.
func WithDashboardPort(port int) Option {
	return startOnly(func(gl *GoldLapel) {
		gl.dashboardPort = port
		gl.dashboardPortSet = true
	})
}

// WithLogLevel sets the proxy log level. Accepted values:
// "trace", "debug", "info", "warn"/"warning", "error". Only trace/debug/info
// produce additional output; warn/error are the binary's default level and
// emit nothing extra. Any other value returns an error at Start time.
// Construction-time only.
func WithLogLevel(level string) Option {
	return startOnly(func(gl *GoldLapel) {
		gl.logLevel = level
	})
}

// WithMode sets the proxy operating mode (e.g. "waiter", "consideration"). Passed
// as --mode. Construction-time only.
func WithMode(mode string) Option {
	return startOnly(func(gl *GoldLapel) {
		gl.mode = mode
	})
}

// WithLicense sets the path to the license file. Passed as --license.
// Construction-time only.
func WithLicense(path string) Option {
	return startOnly(func(gl *GoldLapel) {
		gl.license = path
	})
}

// WithClient sets the client identifier, emitted via GOLDLAPEL_CLIENT for
// per-wrapper telemetry tagging. Defaults to "go" when unset.
// Construction-time only.
func WithClient(client string) Option {
	return startOnly(func(gl *GoldLapel) {
		gl.client = client
	})
}

// WithConfigFile sets the path to a TOML config file the Rust binary will
// parse. Passed as --config. Distinct from WithConfig (which accepts a
// structured map of tuning keys). Construction-time only.
func WithConfigFile(path string) Option {
	return startOnly(func(gl *GoldLapel) {
		gl.configFile = path
	})
}

// WithExtraArgs passes additional CLI flags to the binary. Construction-time only.
func WithExtraArgs(args ...string) Option {
	return startOnly(func(gl *GoldLapel) {
		gl.extraArgs = args
	})
}

// WithSilent toggles the one-line startup banner that Start would otherwise
// print to stderr. Pass true (or omit the argument if calling via the zero
// value) to suppress the banner in library code, CLI tools, or test
// harnesses. Construction-time only.
func WithSilent(silent bool) Option {
	return startOnly(func(gl *GoldLapel) {
		gl.silent = silent
	})
}

// WithMesh opts the proxy into the mesh at startup. HQ enforces the license:
// if the current plan doesn't cover mesh, the proxy continues running normally
// without clustering (concierge, not bouncer) — Start does not fail.
// Equivalent CLI flag: --mesh. Env var: GOLDLAPEL_MESH. TOML: [mesh] enabled.
// Construction-time only.
func WithMesh(mesh bool) Option {
	return startOnly(func(gl *GoldLapel) {
		gl.mesh = mesh
	})
}

// WithMeshTag sets the optional mesh tag. Instances sharing a tag cluster
// together; when unset, mesh-enabled instances join the account's default
// mesh. An empty string is normalised to no tag.
// Equivalent CLI flag: --mesh-tag. Env var: GOLDLAPEL_MESH_TAG.
// Construction-time only.
func WithMeshTag(tag string) Option {
	return startOnly(func(gl *GoldLapel) {
		gl.meshTag = tag
	})
}

// WithDisableProxyCache turns off the proxy's result cache. When true, the proxy emits --disable-proxy-cache
// so cache-eligible queries are passed straight through to Postgres.
// Default (option omitted): proxy decides (today: enabled). Construction-time only.
//
// Equivalent CLI flag: --disable-proxy-cache.
// Equivalent env var: GOLDLAPEL_DISABLE_PROXY_CACHE.
func WithDisableProxyCache(disable bool) Option {
	return startOnly(func(gl *GoldLapel) {
		gl.disableProxyCache = disable
	})
}

// WithDisableSqloptimize turns off the proxy's whole SQL Optimizer: COPY
// rewrite, expression rewrite, N+1 detection (per-connection and
// cross-connection) and query coalescing. When true, the proxy emits
// --disable-sqloptimize and each of those is skipped whatever its own flag
// says. Default (option omitted): proxy decides (today: enabled).
// Construction-time only.
//
// Equivalent CLI flag: --disable-sqloptimize.
// Equivalent env var: GOLDLAPEL_DISABLE_SQLOPTIMIZE.
func WithDisableSqloptimize(disable bool) Option {
	return startOnly(func(gl *GoldLapel) {
		gl.disableSqloptimize = disable
	})
}

// WithDisableAutoIndexes turns off automatic index creation. When true,
// the proxy emits --disable-auto-indexes so the various index-strategy
// kinds (btree, trigram, expression, partial, ...) do not auto-provision
// indexes against upstream. Default (option omitted): proxy decides
// (today: enabled). Construction-time only.
//
// Equivalent CLI flag: --disable-auto-indexes.
// Equivalent env var: GOLDLAPEL_DISABLE_AUTO_INDEXES.
func WithDisableAutoIndexes(disable bool) Option {
	return startOnly(func(gl *GoldLapel) {
		gl.disableAutoIndexes = disable
	})
}

// WithConfig passes structured configuration as CLI flags to the binary.
// Keys are snake_case strings mapping to CLI flags (e.g. "pool_size" → "--pool-size").
// Top-level concepts (proxy_port, dashboard_port, log_level, mode, license,
// client, config_file) must use their own WithX option and are rejected
// from this map at Start time.
// Construction-time only.
func WithConfig(config map[string]interface{}) Option {
	return startOnly(func(gl *GoldLapel) {
		if gl.config == nil {
			gl.config = map[string]interface{}{}
		}
		for k, v := range config {
			gl.config[k] = v
		}
	})
}

// WithTx directs a single wrapper method call at a specific transaction.
// Pass as the last argument to any receiver method:
//
//	gl.Documents.Insert(ctx, "events", doc, goldlapel.WithTx(tx))
//
// Per-call only; ignored if passed to Start.
func WithTx(tx *sql.Tx) Option {
	return callOnly(func(o *callOptions) {
		o.tx = tx
	})
}

// LogLevelToVerboseFlag translates the wrapper-facing log_level string into
// the proxy binary's count-based verbosity flag. The binary does not accept
// --log-level; it accepts -v / -vv / -vvv on top of a default (warn/error)
// level. Returns empty string when no flag should be emitted.
//
// Accepted inputs: "trace" → -vvv, "debug" → -vv, "info" → -v,
// "warn"/"warning"/"error" → "" (default level, no flag).
// Any other value returns an error with the expected set.
func LogLevelToVerboseFlag(level string) (string, error) {
	switch strings.ToLower(level) {
	case "":
		return "", nil
	case "trace":
		return "-vvv", nil
	case "debug":
		return "-vv", nil
	case "info":
		return "-v", nil
	case "warn", "warning", "error":
		return "", nil
	default:
		return "", fmt.Errorf("log_level must be one of: trace, debug, info, warn, error (got %q)", level)
	}
}

// ConfigToArgs converts a config map into CLI argument strings.
// Keys are snake_case strings; each is validated against the known set of
// tuning-key names. Boolean keys emit a bare flag when true, nothing when
// false. List keys emit repeated --flag value pairs for each element. All
// other keys emit --flag value pairs.
func ConfigToArgs(config map[string]interface{}) ([]string, error) {
	if len(config) == 0 {
		return nil, nil
	}

	keys := make([]string, 0, len(config))
	for k := range config {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var args []string
	for _, key := range keys {
		if !validConfigKeys[key] {
			if why, ok := removedConfigKeys[key]; ok {
				return nil, fmt.Errorf("config key %q %s", key, why)
			}
			return nil, fmt.Errorf("unknown config key: %q", key)
		}

		value := config[key]
		flag := "--" + strings.ReplaceAll(key, "_", "-")

		if booleanKeys[key] {
			b, ok := value.(bool)
			if !ok {
				return nil, fmt.Errorf("config key %q expects a bool value, got %T", key, value)
			}
			if b {
				args = append(args, flag)
			}
			continue
		}

		if listKeys[key] {
			switch v := value.(type) {
			case []interface{}:
				for _, item := range v {
					args = append(args, flag, fmt.Sprint(item))
				}
			case []string:
				for _, item := range v {
					args = append(args, flag, item)
				}
			default:
				return nil, fmt.Errorf("config key %q expects a list value, got %T", key, value)
			}
			continue
		}

		args = append(args, flag, fmt.Sprint(value))
	}

	return args, nil
}

// ConfigKeys returns a sorted list of all valid configuration key names.
func ConfigKeys() []string {
	keys := make([]string, 0, len(validConfigKeys))
	for k := range validConfigKeys {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// --- GoldLapel instance ---

// GoldLapel manages a Gold Lapel proxy process and exposes wrapper methods
// (document store, search, pub/sub, queues, etc.) bound to a database/sql
// connection pointed at the proxy.
type GoldLapel struct {
	upstream         string
	proxyPort        int
	dashboardPort    int
	dashboardPortSet bool // true once WithDashboardPort has overridden the derivation
	logLevel         string
	mode             string
	license          string
	client           string
	configFile       string
	config           map[string]interface{}
	extraArgs        []string
	proc             *proxyProcess // the running proxy this handle holds; nil before Start and after Stop
	proxyURL         string
	db               *sql.DB
	tx               *sql.Tx // non-nil only for GoldLapel instances returned by InTx
	silent           bool    // when true, printBanner is a no-op
	mesh             bool    // startup mesh intent (emits --mesh)
	meshTag          string  // optional mesh tag (emits --mesh-tag <tag>)
	// Proxy-side disable flags promoted out of the structured config map.
	// Only true is emitted, leaving the proxy free to honour its own
	// defaults / env-var fallbacks.
	disableProxyCache  bool
	disableSqloptimize bool
	disableAutoIndexes bool

	mu sync.Mutex
	// DDL API state — see ddl.go.
	dashboardToken string    // provisioned on spawn; cleared on Stop
	ddlCache       *sync.Map // per-instance cache keyed on "family:name" → *DdlEntry; shared with InTx scoped instances

	// Nested namespaces — schema-to-core sub-API instances. Each holds a
	// back-reference to this client for shared state (license, dashboard
	// token, db, DDL pattern cache). Phase 4 = Documents + Streams; Phase 5
	// = Counters / Zsets / Hashes / Queues / Geos. Remaining flat namespaces
	// (cache, search, percolate, pubsub) migrate in their own phases.
	Documents *Documents
	Streams   *Streams
	Counters  *Counters
	Zsets     *Zsets
	Hashes    *Hashes
	Queues    *Queues
	Geos      *Geos
}

// attachNamespaces wires the nested sub-API instances onto gl and lazily
// initialises the per-instance DDL cache. Called from Start (production
// path), from InTx (scoped path — but with the parent's cache pointer
// passed in), and from test harnesses so gl.Documents / gl.Streams /
// gl.ddlCache are non-nil in every code path that sees a constructed
// *GoldLapel.
func (gl *GoldLapel) attachNamespaces() {
	if gl.ddlCache == nil {
		gl.ddlCache = &sync.Map{}
	}
	gl.Documents = &Documents{gl: gl}
	gl.Streams = &Streams{gl: gl}
	gl.Counters = &Counters{gl: gl}
	gl.Zsets = &Zsets{gl: gl}
	gl.Hashes = &Hashes{gl: gl}
	gl.Queues = &Queues{gl: gl}
	gl.Geos = &Geos{gl: gl}
}

// proxyProcess is one running proxy binary. Every Start for the same
// upstream in this process shares it (each Start returns its own handle);
// the last handle's Stop terminates it.
type proxyProcess struct {
	upstream       string
	proxyPort      int
	dashboardPort  int // 0 when the dashboard is off
	proxyURL       string
	dashboardToken string
	cmd            *exec.Cmd
	ready          chan struct{} // closed once the start attempt has finished, ok or not
	done           chan struct{} // closed by the reaper once the process has exited
	waitErr        error         // cmd.Wait()'s result; read only after done is closed
	stderr         string        // everything the process wrote to stderr; likewise
	refs           int           // handles holding this proxy; guarded by proxiesMu
}

// exited reports whether the process has exited.
func (p *proxyProcess) exited() bool {
	return isClosed(p.done)
}

// proxies holds every proxy this process has started or is starting, so a
// second Start for the same upstream reuses the first proxy and a different
// upstream never lands on a port another proxy here already holds.
var (
	proxiesMu sync.Mutex
	proxies   = map[*proxyProcess]struct{}{}
)

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// Start spawns the Gold Lapel proxy against the given upstream, waits for it
// to accept connections, opens a pooled database/sql connection, and returns
// a ready-to-use *GoldLapel. Call gl.Stop(ctx) to terminate — typically via
// defer gl.Stop(ctx).
//
// Each upstream gets one proxy per process. If this process already runs a
// proxy for the same upstream, Start returns a new handle on it (with its
// own pool) instead of spawning another — that Start's other options are
// ignored — and the proxy stops when the last handle is stopped.
//
// Options may include construction-time settings (WithProxyPort, WithLogLevel,
// WithConfig, WithExtraArgs, WithDashboardPort, ...).
func Start(ctx context.Context, upstream string, opts ...Option) (*GoldLapel, error) {
	gl := &GoldLapel{upstream: upstream}
	gl.attachNamespaces()
	for _, opt := range opts {
		opt.applyStart(gl)
	}

	// Validate every option before claiming ports or spawning anything.
	args, err := gl.buildArgs()
	if err != nil {
		return nil, err
	}

	proc, reused, err := gl.acquireProxy(ctx)
	if err != nil {
		return nil, err
	}
	if reused {
		gl.attachProxy(ctx, proc)
		return gl, nil
	}

	err = gl.spawn(ctx, proc, args)
	proxiesMu.Lock()
	if err != nil {
		delete(proxies, proc)
	}
	close(proc.ready)
	proxiesMu.Unlock()
	if err != nil {
		return nil, err
	}
	gl.attachProxy(ctx, proc)
	gl.printBanner(os.Stderr)
	return gl, nil
}

// buildArgs turns the options into the binary's argv, minus --upstream and
// the port flags, which spawn adds once the ports are settled. Every option
// error surfaces here.
func (gl *GoldLapel) buildArgs() ([]string, error) {
	var args []string
	if gl.logLevel != "" {
		flag, err := LogLevelToVerboseFlag(gl.logLevel)
		if err != nil {
			return nil, fmt.Errorf("invalid log_level: %w", err)
		}
		if flag != "" {
			args = append(args, flag)
		}
	}
	if gl.mode != "" {
		args = append(args, "--mode", gl.mode)
	}
	if gl.license != "" {
		args = append(args, "--license", gl.license)
	}
	if gl.client != "" {
		args = append(args, "--client", gl.client)
	}
	if gl.configFile != "" {
		args = append(args, "--config", gl.configFile)
	}
	if gl.mesh {
		args = append(args, "--mesh")
	}
	if gl.meshTag != "" {
		args = append(args, "--mesh-tag", gl.meshTag)
	}
	// Promoted disable-flag options. Each emits a bare presence-flag when
	// true — the proxy CLI surface doesn't have an "enable-X" counterpart,
	// so WithDisable*(false) is a no-op (the proxy's default applies,
	// modulo the corresponding GOLDLAPEL_DISABLE_X env var).
	if gl.disableProxyCache {
		args = append(args, "--disable-proxy-cache")
	}
	if gl.disableSqloptimize {
		args = append(args, "--disable-sqloptimize")
	}
	if gl.disableAutoIndexes {
		args = append(args, "--disable-auto-indexes")
	}
	if gl.config != nil {
		configArgs, err := ConfigToArgs(gl.config)
		if err != nil {
			return nil, fmt.Errorf("invalid config: %w", err)
		}
		args = append(args, configArgs...)
	}
	return append(args, gl.extraArgs...), nil
}

// acquireProxy finds this process's live proxy for gl.upstream and takes a
// reference on it (reused = true), or settles gl's ports and registers a new,
// not-yet-spawned proxy for Start to spawn. A Start already in flight for
// the same upstream is waited for, so concurrent Starts share one proxy.
func (gl *GoldLapel) acquireProxy(ctx context.Context) (proc *proxyProcess, reused bool, err error) {
	for {
		proxiesMu.Lock()
		var starting *proxyProcess
		for p := range proxies {
			// refs == 0 means its last Stop is tearing it down.
			if p.upstream != gl.upstream || p.refs == 0 {
				continue
			}
			if !isClosed(p.ready) {
				starting = p
				break
			}
			if !p.exited() {
				p.refs++
				proxiesMu.Unlock()
				return p, true, nil
			}
		}
		if starting == nil {
			proc, err = gl.claimPortsLocked()
			if err == nil {
				proxies[proc] = struct{}{}
			}
			proxiesMu.Unlock()
			return proc, false, err
		}
		proxiesMu.Unlock()
		select {
		case <-starting.ready:
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
}

// portClaim is a port a live proxy of this process holds.
type portClaim struct {
	proc *proxyProcess
	role string // "proxy" or "dashboard"
}

// claimPortsLocked settles gl.proxyPort and gl.dashboardPort and returns
// the proxy that will hold them. An explicit port another live proxy of
// this process holds is an error; otherwise the proxy port is the smallest
// P >= DefaultProxyPort such that neither P nor its dashboard port is held
// here or busy at the OS level. An explicit dashboard port is the caller's
// choice, so only P is searched. Caller holds proxiesMu.
func (gl *GoldLapel) claimPortsLocked() (*proxyProcess, error) {
	claimed := map[int]portClaim{}
	for p := range proxies {
		if isClosed(p.ready) && p.exited() {
			continue
		}
		claimed[p.proxyPort] = portClaim{p, "proxy"}
		if p.dashboardPort > 0 {
			claimed[p.dashboardPort] = portClaim{p, "dashboard"}
		}
	}
	collision := func(port int, role string) error {
		c, ok := claimed[port]
		if port <= 0 || !ok {
			return nil
		}
		return fmt.Errorf("Gold Lapel cannot use port %d as the %s port: this process's proxy for %s already holds it as its %s port. Choose another port, or omit WithProxyPort and WithDashboardPort to have a free pair assigned",
			port, role, redactPassword(c.proc.upstream), c.role)
	}

	if gl.proxyPort != 0 {
		if !gl.dashboardPortSet {
			gl.dashboardPort = gl.proxyPort + 1
		}
		if err := collision(gl.proxyPort, "proxy"); err != nil {
			return nil, err
		}
		if err := collision(gl.dashboardPort, "dashboard"); err != nil {
			return nil, err
		}
	} else {
		if gl.dashboardPortSet {
			if err := collision(gl.dashboardPort, "dashboard"); err != nil {
				return nil, err
			}
		}
		for port := DefaultProxyPort; port < 65535; port++ {
			dash := gl.dashboardPort
			if !gl.dashboardPortSet {
				dash = port + 1
			}
			if _, held := claimed[port]; held || port == dash || !portBindable(port) {
				continue
			}
			if !gl.dashboardPortSet {
				if _, held := claimed[dash]; held || !portBindable(dash) {
					continue
				}
			}
			gl.proxyPort, gl.dashboardPort = port, dash
			break
		}
		if gl.proxyPort == 0 {
			return nil, fmt.Errorf("Gold Lapel could not find a free proxy port from %d up", DefaultProxyPort)
		}
	}

	return &proxyProcess{
		upstream:      gl.upstream,
		proxyPort:     gl.proxyPort,
		dashboardPort: gl.dashboardPort,
		ready:         make(chan struct{}),
		done:          make(chan struct{}),
		refs:          1,
	}, nil
}

// portBindable reports whether port can be bound on every interface right
// now — the same check the proxy makes before it starts.
func portBindable(port int) bool {
	ln, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		return false
	}
	ln.Close()
	return true
}

// redactPassword returns url with the password in its userinfo replaced by
// "***", for error messages.
func redactPassword(url string) string {
	return passwordRe.ReplaceAllString(url, "${1}***@")
}

// attachProxy points gl at a running proxy and opens gl's own pool on it.
func (gl *GoldLapel) attachProxy(ctx context.Context, proc *proxyProcess) {
	gl.proc = proc
	gl.proxyPort = proc.proxyPort
	gl.dashboardPort = proc.dashboardPort
	gl.proxyURL = proc.proxyURL
	gl.dashboardToken = proc.dashboardToken

	// Eagerly open a database/sql pool against the proxy. The user may
	// register any Postgres driver — we prefer "pgx" (from
	// github.com/jackc/pgx/v5/stdlib) and fall back to "postgres"
	// (github.com/lib/pq). If neither is registered, gl.db stays nil
	// and URL() remains usable for the caller to open their own pool.
	db, openErr := openDB(gl.proxyURL)
	if openErr == nil {
		// Verify the pool can actually reach the proxy.
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if pingErr := db.PingContext(pingCtx); pingErr != nil {
			db.Close()
			db = nil
		}
		cancel()
	}
	gl.db = db
}

// spawn boots the binary for proc on gl's settled ports and waits until it
// answers. Called exclusively from Start. proc is already registered, but
// other goroutines read only its ports and done channel until Start closes
// proc.ready, so the fields set here need no lock.
func (gl *GoldLapel) spawn(ctx context.Context, proc *proxyProcess, args []string) error {
	bin, err := FindBinary()
	if err != nil {
		return err
	}

	portArgs := []string{"--upstream", gl.upstream, "--proxy-port", fmt.Sprintf("%d", gl.proxyPort)}
	if gl.dashboardPortSet {
		portArgs = append(portArgs, "--dashboard-port", fmt.Sprintf("%d", gl.dashboardPort))
	}
	cmd := exec.Command(bin, append(portArgs, args...)...)
	cmd.Env = os.Environ()
	// GOLDLAPEL_CLIENT env var is only set when the caller hasn't supplied
	// --client via WithClient (explicit --client flag takes precedence) and
	// the env var isn't already set by the surrounding shell.
	if gl.client == "" && os.Getenv("GOLDLAPEL_CLIENT") == "" {
		cmd.Env = append(cmd.Env, "GOLDLAPEL_CLIENT=go")
	}
	// Provision a session-scoped dashboard token so ddl.go can authenticate
	// against /api/ddl/*. Pre-set env wins.
	if t := os.Getenv("GOLDLAPEL_DASHBOARD_TOKEN"); t != "" {
		proc.dashboardToken = t
	} else {
		buf := make([]byte, 32)
		if _, err := rand.Read(buf); err != nil {
			return fmt.Errorf("generate dashboard token: %w", err)
		}
		proc.dashboardToken = hex.EncodeToString(buf)
		cmd.Env = append(cmd.Env, "GOLDLAPEL_DASHBOARD_TOKEN="+proc.dashboardToken)
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	// If something already listens on the proxy port, a connect succeeds
	// whether or not our proxy is up — see waitForProxy.
	portWasBusy := !portBindable(gl.proxyPort)

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start Gold Lapel: %w", err)
	}
	proc.cmd = cmd

	// Reaper: collects stderr, then the exit status. Stop and the readiness
	// wait synchronise on proc.done, so the writes here happen-before any
	// read of proc.stderr / proc.waitErr.
	go func() {
		var stderrBuf strings.Builder
		io.Copy(&stderrBuf, stderrPipe)
		proc.stderr = stderrBuf.String()
		proc.waitErr = cmd.Wait()
		close(proc.done)
	}()

	if !waitForProxy(ctx, gl.proxyPort, proc.done, portWasBusy) {
		if proc.exited() {
			return fmt.Errorf("Gold Lapel exited during startup (%v).\nstderr: %s", proc.waitErr, stderrTail(proc.stderr))
		}
		cmd.Process.Kill()
		<-proc.done
		return fmt.Errorf("Gold Lapel failed to start on port %d within %ds.\nstderr: %s",
			gl.proxyPort, int(startupTimeout.Seconds()), stderrTail(proc.stderr))
	}

	proc.proxyURL = makeProxyURL(gl.upstream, gl.proxyPort, gl.clientTLS())
	return nil
}

// clientTLS reports whether the proxy is being started with its own TLS
// certificate, i.e. it accepts TLS from the app.
func (gl *GoldLapel) clientTLS() bool {
	if gl.config["tls_cert"] != nil && gl.config["tls_key"] != nil {
		return true
	}
	for _, a := range gl.extraArgs {
		if a == "--tls-cert" || strings.HasPrefix(a, "--tls-cert=") {
			return true
		}
	}
	return false
}

// stderrTail returns the last few KB of a process's stderr — enough for the
// proxy's own error message without flooding the caller's error.
func stderrTail(stderr string) string {
	const max = 4096
	stderr = strings.TrimSpace(stderr)
	if len(stderr) > max {
		stderr = "…" + stderr[len(stderr)-max:]
	}
	return stderr
}

// printBanner writes the one-line startup banner to w. Library code should
// never write to stdout, so Start calls this with os.Stderr. WithSilent()
// makes it a no-op. Exposed as a method (rather than inlined in spawn) so
// tests can exercise both the routing and the silent-suppression paths
// without spawning the real binary.
func (gl *GoldLapel) printBanner(w io.Writer) {
	if gl.silent {
		return
	}
	if gl.dashboardPort > 0 {
		fmt.Fprintf(w, "goldlapel → :%d (proxy) | http://127.0.0.1:%d (dashboard)\n", gl.proxyPort, gl.dashboardPort)
	} else {
		fmt.Fprintf(w, "goldlapel → :%d (proxy)\n", gl.proxyPort)
	}
}

// openDB tries common registered Postgres drivers in a stable order.
// Users are expected to import a driver (pgx or lib/pq) into their app.
func openDB(url string) (*sql.DB, error) {
	var lastErr error
	for _, drv := range []string{"pgx", "postgres"} {
		db, err := sql.Open(drv, url)
		if err == nil {
			return db, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no Postgres database/sql driver registered (import pgx/stdlib or lib/pq)")
	}
	return nil, lastErr
}

// Stop releases this handle: it closes the handle's database pool and, if
// no other handle from Start still holds the proxy, terminates the proxy
// process. Safe to call multiple times. The context is honoured during the
// graceful shutdown wait — if ctx is cancelled the process is killed
// immediately.
//
// Return value contract:
//   - nil when the proxy shut down as expected — including the normal case
//     where Stop itself signalled SIGTERM/Kill (the resulting non-zero exit
//     is expected, not an error) — or is still serving other handles.
//   - non-nil when the subprocess exited on its own before Stop was called
//     (e.g. crashed with a non-zero status, OOM-killed) — in that case the
//     exit error from cmd.Wait() is surfaced so callers checking the return
//     value see the failure.
//
// Scoped instances (the *GoldLapel returned by InTx) share the parent's
// process and pool but must not tear them down if a caller mistakenly
// calls Stop on them. Stop is a no-op on a scoped instance — the caller
// should Stop the parent.
func (gl *GoldLapel) Stop(ctx context.Context) error {
	gl.mu.Lock()
	defer gl.mu.Unlock()

	// Scoped instance (bound to an *sql.Tx from InTx): never tear down
	// the shared proxy process or pool. The parent owns those.
	if gl.tx != nil {
		return nil
	}

	// Drop cached DDL patterns — they're tied to the proxy we're releasing.
	// sync.Map has no clear, so walk + delete.
	gl.ddlCache.Range(func(k, _ any) bool {
		gl.ddlCache.Delete(k)
		return true
	})
	gl.dashboardToken = ""
	gl.proxyURL = ""
	if gl.db != nil {
		gl.db.Close()
		gl.db = nil
	}

	proc := gl.proc
	gl.proc = nil
	if proc == nil {
		// Unstarted / already stopped: nothing to do.
		return nil
	}

	proxiesMu.Lock()
	proc.refs--
	last := proc.refs == 0
	proxiesMu.Unlock()

	if !last {
		if proc.exited() {
			return filterStopExit(proc.waitErr, false)
		}
		return nil
	}

	err := proc.terminate(ctx)
	proxiesMu.Lock()
	delete(proxies, proc)
	proxiesMu.Unlock()
	return err
}

// terminate stops the process and waits for the reaper. If the process had
// already exited on its own, its exit error is returned; the non-zero exit
// our own signal causes is not.
func (p *proxyProcess) terminate(ctx context.Context) error {
	// Fast path: process exited on its own before Stop was called. This
	// is NOT an our-signal shutdown — surface any Wait() error so callers
	// see e.g. a non-zero crash exit.
	if p.exited() {
		return filterStopExit(p.waitErr, false)
	}

	if runtime.GOOS == "windows" {
		p.cmd.Process.Kill()
	} else {
		p.cmd.Process.Signal(syscall.SIGTERM)
	}

	select {
	case <-p.done:
	case <-ctx.Done():
		p.cmd.Process.Kill()
		<-p.done
	case <-time.After(shutdownTimeout):
		p.cmd.Process.Kill()
		<-p.done
	}
	return filterStopExit(p.waitErr, true)
}

// filterStopExit classifies cmd.Wait()'s error and decides whether to
// surface it. When weSignaled is true, the subprocess was killed by us
// (SIGTERM or Process.Kill) — *exec.ExitError in that case reflects the
// expected shutdown path and is swallowed. Any other non-nil error
// (subprocess-crashed, OOM, I/O errors inside Wait) is returned so the
// caller of Stop sees the failure.
//
// This is platform-agnostic: on both POSIX and Windows, cmd.Wait() returns
// an *exec.ExitError for any non-zero exit — including signal-induced
// termination on POSIX and Kill-induced termination on Windows. Filtering
// by "did we issue the kill?" rather than by platform-specific WaitStatus
// bits keeps the logic simple and uniform across all four targets
// (linux-x86_64, linux-aarch64, darwin-aarch64, windows-x86_64).
func filterStopExit(err error, weSignaled bool) error {
	if err == nil {
		return nil
	}
	if !weSignaled {
		return err
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return nil
	}
	return err
}

// Close implements io.Closer. It calls Stop with context.Background().
// Prefer Stop(ctx) in application code so the shutdown deadline is explicit.
// Like Stop, Close is a no-op on scoped instances returned by InTx.
func (gl *GoldLapel) Close() error {
	return gl.Stop(context.Background())
}

// URL returns the proxy connection string, or "" if the proxy is stopped.
// Use with database/sql: sql.Open("pgx", gl.URL()).
func (gl *GoldLapel) URL() string {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	return gl.proxyURL
}

// ProxyPort returns the configured proxy port.
func (gl *GoldLapel) ProxyPort() int {
	return gl.proxyPort
}

// Running reports whether the proxy process is still alive.
func (gl *GoldLapel) Running() bool {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	return gl.proc != nil && !gl.proc.exited()
}

// DashboardURL returns the dashboard URL while the proxy is running, or "".
func (gl *GoldLapel) DashboardURL() string {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	if gl.dashboardPort > 0 && gl.proc != nil {
		return fmt.Sprintf("http://127.0.0.1:%d", gl.dashboardPort)
	}
	return ""
}

// DashboardPort returns the dashboard port (proxy port + 1 by default). Used
// by the DDL API client.
func (gl *GoldLapel) DashboardPort() int {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	return gl.dashboardPort
}

// DashboardToken returns the dashboard token this instance provisioned for
// the proxy subprocess. Returns "" when the proxy was launched externally
// (in that case ddl.go falls back to env / ~/.goldlapel/dashboard-token).
func (gl *GoldLapel) DashboardToken() string {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	return gl.dashboardToken
}

// DB returns the underlying *sql.DB connected to the proxy.
// Returns nil if Start did not manage to open a pool (e.g. no driver
// registered). Users can always sql.Open(..., gl.URL()) themselves.
func (gl *GoldLapel) DB() *sql.DB {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	return gl.db
}

// ErrNotConnected is returned by receiver methods when no database handle is
// available — typically because Start failed to open a pool.
var ErrNotConnected = errors.New("goldlapel: proxy not started or database connection unavailable")

// execQuerier is the subset of *sql.DB / *sql.Tx our wrapper methods need.
// Both types implement ExecContext, QueryContext, and QueryRowContext with
// identical signatures, so method bodies can target either freely.
type execQuerier interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// resolveExec selects the query target for a single call, honouring
// WithTx overrides and any transaction bound by InTx.
func (gl *GoldLapel) resolveExec(opts []Option) (execQuerier, error) {
	co := callOptions{}
	for _, opt := range opts {
		opt.applyCall(&co)
	}
	if co.tx != nil {
		return co.tx, nil
	}
	gl.mu.Lock()
	if gl.tx != nil {
		tx := gl.tx
		gl.mu.Unlock()
		return tx, nil
	}
	db := gl.db
	gl.mu.Unlock()
	if db == nil {
		return nil, ErrNotConnected
	}
	return db, nil
}

// execQuerier is a convenience accessor that returns the active query
// target: the scoped *sql.Tx (inside InTx), otherwise the pool *sql.DB.
// Returns nil if the instance is stopped. Used by stream* to thread the DDL
// API patterns through to the right connection.
func (gl *GoldLapel) execQuerier() execQuerier {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	if gl.tx != nil {
		return gl.tx
	}
	return gl.db
}

// requireExec returns the active query target or ErrNotConnected.
// Thin wrapper over execQuerier() with a nil-check.
func (gl *GoldLapel) requireExec() (execQuerier, error) {
	q := gl.execQuerier()
	if q == nil {
		return nil, ErrNotConnected
	}
	return q, nil
}

// UseDB registers a caller-supplied *sql.DB with this instance so that
// Stream*, Doc*, etc. methods resolve against it. Useful when the wrapper's
// auto-opener couldn't connect (e.g. a driver requiring sslmode=disable that
// isn't encoded in the default URL) and the caller opens their own pool.
// Passing nil clears the registration.
//
// Ownership: once UseDB is called, the instance's Stop() WILL close the db
// (same as the auto-opened pool). If you need to share the *sql.DB across
// multiple GoldLapel instances or outlive the proxy, open a fresh pool per
// instance or Close() manually before Stop.
func (gl *GoldLapel) UseDB(db *sql.DB) {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	gl.db = db
}

// InTx runs fn inside a database transaction. It begins a transaction on db,
// hands fn a scoped *GoldLapel whose wrapper methods automatically target
// that transaction, and commits on success or rolls back on error/panic.
//
// The scoped instance shares the proxy process with its parent, so URL(),
// Running(), DashboardURL(), etc. continue to work inside the closure.
// WithTx on an individual call inside fn overrides the scoped transaction.
func (gl *GoldLapel) InTx(ctx context.Context, db *sql.DB, fn func(*GoldLapel) error) (err error) {
	if db == nil {
		// Fall back to the pool we opened at Start, if the caller didn't
		// bring their own *sql.DB.
		db = gl.DB()
	}
	if db == nil {
		return ErrNotConnected
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	// Build a scoped instance that shares the process state but carries
	// the transaction. Copying the struct under the lock keeps us from
	// racing against Stop/Start.
	gl.mu.Lock()
	scoped := &GoldLapel{
		upstream:      gl.upstream,
		proxyPort:     gl.proxyPort,
		dashboardPort: gl.dashboardPort,
		proc:          gl.proc,
		proxyURL:      gl.proxyURL,
		db:            gl.db,
		tx:            tx,
		// Inherit the dashboard token so DDL fetches inside the tx work
		// the same as on the parent — without this, gl.Documents.Insert
		// inside InTx would fall back to env/file lookup.
		dashboardToken: gl.dashboardToken,
		// Share the parent's DDL pattern cache so a hit on the parent
		// (e.g. test pre-population, or a prior call this session)
		// is also a hit inside InTx — without this, the scoped instance
		// would always miss and re-POST to the dashboard.
		ddlCache: gl.ddlCache,
	}
	scoped.attachNamespaces()
	gl.mu.Unlock()

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
				err = fmt.Errorf("%w (rollback also failed: %v)", err, rbErr)
			}
			return
		}
		if cmErr := tx.Commit(); cmErr != nil {
			err = fmt.Errorf("commit tx: %w", cmErr)
		}
	}()

	return fn(scoped)
}

// --- Binary lookup ---

// FindBinary locates the Gold Lapel binary using a 3-tier lookup:
// GOLDLAPEL_BINARY env var, bundled binary next to this source, system PATH.
func FindBinary() (string, error) {
	if envPath := os.Getenv("GOLDLAPEL_BINARY"); envPath != "" {
		info, err := os.Stat(envPath)
		if err != nil || !info.Mode().IsRegular() {
			return "", fmt.Errorf("GOLDLAPEL_BINARY points to %s but file not found", envPath)
		}
		return envPath, nil
	}

	osName := runtime.GOOS
	arch := runtime.GOARCH
	switch arch {
	case "amd64":
		arch = "x86_64"
	case "arm64":
		arch = "aarch64"
	}

	binaryName := fmt.Sprintf("goldlapel-%s-%s", osName, arch)
	if osName == "linux" && isMusl(arch) {
		binaryName += "-musl"
	}
	if osName == "windows" {
		binaryName += ".exe"
	}
	_, thisFile, _, ok := runtime.Caller(0)
	if ok {
		bundled := filepath.Join(filepath.Dir(thisFile), "bin", binaryName)
		if info, err := os.Stat(bundled); err == nil && info.Mode().IsRegular() {
			if isExecutable(info) {
				return bundled, nil
			}
			if tmp, err := copyToExecutableTemp(bundled, binaryName); err == nil {
				return tmp, nil
			}
		}
	}

	if path, err := exec.LookPath("goldlapel"); err == nil {
		return path, nil
	}

	return "", fmt.Errorf("Gold Lapel binary not found. Set GOLDLAPEL_BINARY env var, install the platform-specific package, or ensure 'goldlapel' is on PATH.")
}

func isMusl(arch string) bool {
	_, err := os.Stat(fmt.Sprintf("/lib/ld-musl-%s.so.1", arch))
	return err == nil
}

func isExecutable(info os.FileInfo) bool {
	return info.Mode()&0111 != 0
}

func copyToExecutableTemp(src, name string) (string, error) {
	data, err := os.ReadFile(src)
	if err != nil {
		return "", fmt.Errorf("failed to read bundled binary: %w", err)
	}

	dir := filepath.Join(os.TempDir(), "goldlapel-bin")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", fmt.Errorf("failed to create temp directory: %w", err)
	}

	hash := sha256.Sum256(data)
	hashPrefix := hex.EncodeToString(hash[:8])
	dst := filepath.Join(dir, name+"-"+hashPrefix)

	if info, err := os.Stat(dst); err == nil && info.Mode().IsRegular() && isExecutable(info) {
		return dst, nil
	}

	if err := os.WriteFile(dst, data, 0755); err != nil {
		return "", fmt.Errorf("failed to write executable copy: %w", err)
	}

	cleanOldTempBinaries(dir, name, dst)

	return dst, nil
}

func cleanOldTempBinaries(dir, namePrefix, keep string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	prefix := namePrefix + "-"
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		full := filepath.Join(dir, e.Name())
		if strings.HasPrefix(e.Name(), prefix) && full != keep {
			os.Remove(full)
		}
	}
}

// toInt coerces a config value to an int. Returns an error if the value is a
// string that cannot be parsed or a type we don't understand — this prevents
// silent conversion of e.g. "abc" into 0, which would quietly disable ports.
func toInt(v interface{}) (int, error) {
	switch n := v.(type) {
	case int:
		return n, nil
	case int64:
		return int(n), nil
	case float64:
		return int(n), nil
	case string:
		var i int
		if _, err := fmt.Sscanf(n, "%d", &i); err != nil {
			return 0, fmt.Errorf("cannot parse %q as int: %w", n, err)
		}
		return i, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int", v)
	}
}

// --- URL rewriting ---

// WrapperVersion returns the Go wrapper's installed version, used to build the
// application_name marker on PG connections. Read from runtime/debug build
// info (Go modules expose this for tagged versions); falls back to "0.0.0" for
// dev builds and tagged-development scenarios.
func WrapperVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		// Look for our own module record. info.Main is the binary's main module
		// when built as a binary; for library consumers we walk Deps.
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
	return "0.0.0"
}

// ApplicationNameMarker returns the application_name string the wrapper sets
// on PG connections so they're recognisable in pg_stat_activity. The proxy
// doesn't treat them differently — its cache serves every client the same way.
func ApplicationNameMarker() string {
	return "goldlapel:go:" + WrapperVersion()
}

// injectApplicationName appends application_name=goldlapel:go:<version> to
// the URL unless one is already present (or PGAPPNAME is set in the env).
// Idempotent and override-respecting.
func injectApplicationName(url string) string {
	if appNameRe.MatchString(url) {
		return url
	}
	if v := os.Getenv("PGAPPNAME"); v != "" {
		return url
	}
	sep := "?"
	if strings.Contains(url, "?") {
		sep = "&"
	}
	return url + sep + "application_name=" + ApplicationNameMarker()
}

// MakeProxyURL rewrites an upstream connection string to point at the local
// proxy. The upstream's TLS / GSS parameters (sslmode, sslrootcert,
// channel_binding, ...) are dropped and sslmode=disable is set instead: the
// proxy talks plaintext to the app unless it was started with its own
// certificate, and lib/pq treats a missing sslmode as require. Other query
// parameters are kept.
func MakeProxyURL(upstream string, port int) string {
	return makeProxyURL(upstream, port, false)
}

// makeProxyURL is MakeProxyURL; with clientTLS (the proxy has its own
// certificate) the query string is left as the upstream had it.
func makeProxyURL(upstream string, port int, clientTLS bool) string {
	portStr := fmt.Sprintf("%d", port)

	var prefix, rest string
	if m := withPortRe.FindStringSubmatch(upstream); m != nil {
		prefix, rest = m[1], m[4]
	} else if m := withoutPortRe.FindStringSubmatch(upstream); m != nil {
		prefix, rest = m[1], m[3]
	} else {
		// Bare-host form skips the marker — atypical caller path.
		return "localhost:" + portStr
	}
	if !clientTLS {
		rest = plaintextClientParams(rest)
	}
	return injectApplicationName(prefix + "localhost:" + portStr + rest)
}

// plaintextClientParams drops upstreamOnlyParams from the query string in
// rest (the part of a URL after host:port) and appends sslmode=disable.
func plaintextClientParams(rest string) string {
	path, query, _ := strings.Cut(rest, "?")
	var kept []string
	for _, kv := range strings.Split(query, "&") {
		key, _, _ := strings.Cut(kv, "=")
		if kv == "" || upstreamOnlyParams[strings.ToLower(key)] {
			continue
		}
		kept = append(kept, kv)
	}
	return path + "?" + strings.Join(append(kept, "sslmode=disable"), "&")
}

// --- Port readiness ---

// WaitForPort polls until a TCP connection succeeds or the timeout expires.
func WaitForPort(host string, port int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	for time.Now().Before(deadline) {
		if conn, err := net.DialTimeout("tcp", addr, dialTimeout); err == nil {
			conn.Close()
			return true
		}
		time.Sleep(startupPollInterval)
	}
	return false
}

// busyPortGrace is how long a proxy started on a port something else
// already held must survive after the port answers: the proxy refuses a
// busy port within moments of starting.
const busyPortGrace = time.Second

// waitForProxy polls until the proxy answers on 127.0.0.1:port while its
// process is still alive. It gives up when the process exits (closing
// exited), ctx is done, or startupTimeout passes. If the port was already
// held when the proxy was spawned, an answer may come from the other holder,
// so the proxy must also outlive busyPortGrace.
func waitForProxy(ctx context.Context, port int, exited chan struct{}, portWasBusy bool) bool {
	deadline := time.Now().Add(startupTimeout)
	addr := net.JoinHostPort("127.0.0.1", fmt.Sprintf("%d", port))

	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, dialTimeout)
		if err == nil {
			conn.Close()
			if !portWasBusy {
				return !isClosed(exited)
			}
			select {
			case <-exited:
				return false
			case <-ctx.Done():
				return false
			case <-time.After(busyPortGrace):
				return !isClosed(exited)
			}
		}
		select {
		case <-exited:
			return false
		case <-ctx.Done():
			return false
		case <-time.After(startupPollInterval):
		}
	}
	return false
}
