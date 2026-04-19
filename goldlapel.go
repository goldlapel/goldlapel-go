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
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	DefaultPort          = 7932
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

	validConfigKeys = map[string]bool{
		"mode": true, "min_pattern_count": true, "refresh_interval_secs": true,
		"pattern_ttl_secs": true, "max_tables_per_view": true, "max_columns_per_view": true,
		"deep_pagination_threshold": true, "report_interval_secs": true,
		"result_cache_size": true, "batch_cache_size": true, "batch_cache_ttl_secs": true,
		"pool_size": true, "pool_timeout_secs": true, "pool_mode": true,
		"mgmt_idle_timeout": true, "fallback": true, "read_after_write_secs": true,
		"n1_threshold": true, "n1_window_ms": true, "n1_cross_threshold": true,
		"tls_cert": true, "tls_key": true, "tls_client_ca": true, "config": true,
		"dashboard_port": true, "invalidation_port": true, "log_level": true,
		"disable_matviews": true, "disable_consolidation": true, "disable_btree_indexes": true,
		"disable_trigram_indexes": true, "disable_expression_indexes": true,
		"disable_partial_indexes": true, "disable_rewrite": true, "disable_prepared_cache": true,
		"disable_result_cache": true, "disable_pool": true,
		"disable_n1": true, "disable_n1_cross_connection": true, "disable_shadow_mode": true,
		"enable_coalescing": true,
		"replica":           true, "exclude_tables": true,
	}

	booleanKeys = map[string]bool{
		"disable_matviews": true, "disable_consolidation": true, "disable_btree_indexes": true,
		"disable_trigram_indexes": true, "disable_expression_indexes": true,
		"disable_partial_indexes": true, "disable_rewrite": true, "disable_prepared_cache": true,
		"disable_result_cache": true, "disable_pool": true,
		"disable_n1": true, "disable_n1_cross_connection": true, "disable_shadow_mode": true,
		"enable_coalescing": true,
	}

	listKeys = map[string]bool{
		"replica":        true,
		"exclude_tables": true,
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
// parameters (e.g. WithPort) or per-call overrides (e.g. WithTx). A single
// Option may participate in any combination of the four facets:
//
//	applyStart  — set fields on *GoldLapel before spawn (WithPort, WithConfig)
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

func (f startOnly) applyStart(gl *GoldLapel)  { f(gl) }
func (f startOnly) applyCall(*callOptions)    {}
func (f startOnly) applySearch(*searchOptions) {}
func (f startOnly) applyDoc(*docFindOptions)   {}

// callOnly is an Option that only affects a single method call.
type callOnly func(*callOptions)

func (f callOnly) applyStart(*GoldLapel)       {}
func (f callOnly) applyCall(o *callOptions)    { f(o) }
func (f callOnly) applySearch(*searchOptions)  {}
func (f callOnly) applyDoc(*docFindOptions)    {}

// searchOnly is an Option that only affects search-specific options.
type searchOnly func(*searchOptions)

func (f searchOnly) applyStart(*GoldLapel)       {}
func (f searchOnly) applyCall(*callOptions)      {}
func (f searchOnly) applySearch(o *searchOptions) { f(o) }
func (f searchOnly) applyDoc(*docFindOptions)    {}

// docOnly is an Option that only affects DocFind-specific options.
type docOnly func(*docFindOptions)

func (f docOnly) applyStart(*GoldLapel)       {}
func (f docOnly) applyCall(*callOptions)      {}
func (f docOnly) applySearch(*searchOptions)  {}
func (f docOnly) applyDoc(o *docFindOptions)  { f(o) }

// callOptions collects per-call overrides. Currently only a transaction
// target, but this is the hook for future per-call options.
type callOptions struct {
	tx *sql.Tx
}

// WithPort sets the proxy listen port. Construction-time only.
func WithPort(port int) Option {
	return startOnly(func(gl *GoldLapel) {
		gl.port = port
	})
}

// WithLogLevel sets the proxy log level. Accepted values:
// "trace", "debug", "info", "warn"/"warning", "error". Only trace/debug/info
// produce additional output; warn/error are the binary's default level and
// emit nothing extra. Any other value returns an error at Start time.
// Construction-time only.
func WithLogLevel(level string) Option {
	return startOnly(func(gl *GoldLapel) {
		if gl.config == nil {
			gl.config = map[string]interface{}{}
		}
		gl.config["log_level"] = level
	})
}

// WithExtraArgs passes additional CLI flags to the binary. Construction-time only.
func WithExtraArgs(args ...string) Option {
	return startOnly(func(gl *GoldLapel) {
		gl.extraArgs = args
	})
}

// WithSilent suppresses the one-line startup banner that Start would
// otherwise print to stderr. Use it in library code, CLI tools, or test
// harnesses that don't want the wrapper chattering on their streams.
// Construction-time only.
func WithSilent() Option {
	return startOnly(func(gl *GoldLapel) {
		gl.silent = true
	})
}

// WithConfig passes structured configuration as CLI flags to the binary.
// Keys are snake_case strings mapping to CLI flags (e.g. "pool_size" → "--pool-size").
// Construction-time only.
func WithConfig(config map[string]interface{}) Option {
	return startOnly(func(gl *GoldLapel) {
		// Merge into any pre-existing config (e.g. from WithLogLevel).
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
//	gl.DocInsert(ctx, "events", doc, goldlapel.WithTx(tx))
//
// Per-call only; ignored if passed to Start.
func WithTx(tx *sql.Tx) Option {
	return callOnly(func(o *callOptions) {
		o.tx = tx
	})
}

// logLevelToVerboseFlag translates the wrapper-facing log_level string into
// the proxy binary's count-based verbosity flag. The binary does not accept
// --log-level; it accepts -v / -vv / -vvv on top of a default (warn/error)
// level. Returns empty string when no flag should be emitted.
//
// Accepted inputs: "trace" → -vvv, "debug" → -vv, "info" → -v,
// "warn"/"warning"/"error" → "" (default level, no flag).
// Any other value returns an error with the expected set.
func logLevelToVerboseFlag(level string) (string, error) {
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
// config keys. Boolean keys emit a bare flag when true, nothing when false.
// List keys emit repeated --flag value pairs for each element. The
// log_level key is a wrapper-side concept: it translates to the proxy's
// count-based -v/-vv/-vvv flag rather than --log-level. All other keys
// emit --flag value pairs.
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
			return nil, fmt.Errorf("unknown config key: %q", key)
		}

		value := config[key]

		if key == "log_level" {
			levelStr, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("config key %q expects a string value, got %T", key, value)
			}
			flag, err := logLevelToVerboseFlag(levelStr)
			if err != nil {
				return nil, err
			}
			if flag != "" {
				args = append(args, flag)
			}
			continue
		}

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
	upstream      string
	port          int
	dashboardPort int
	config        map[string]interface{}
	extraArgs     []string
	cmd           *exec.Cmd
	proxyURL      string
	stderr        string
	done          chan struct{} // closed when process exits
	waitErr       error         // set by spawn's reaper goroutine before closing done
	weSignaled    bool          // set by Stop before issuing SIGTERM/Kill, so Stop can filter the resulting ExitError
	db            *sql.DB
	tx            *sql.Tx // non-nil only for GoldLapel instances returned by InTx
	silent        bool    // when true, printBanner is a no-op
	mu            sync.Mutex
}

// Start spawns the Gold Lapel proxy against the given upstream, waits for it
// to accept connections, opens a pooled database/sql connection, and returns
// a ready-to-use *GoldLapel. Call gl.Stop(ctx) to terminate — typically via
// defer gl.Stop(ctx).
//
// Options may include construction-time settings (WithPort, WithLogLevel,
// WithConfig, WithExtraArgs).
func Start(ctx context.Context, upstream string, opts ...Option) (*GoldLapel, error) {
	gl := &GoldLapel{
		upstream: upstream,
		port:     DefaultPort,
	}
	for _, opt := range opts {
		opt.applyStart(gl)
	}
	// Dashboard defaults to proxy port + 1 (matches what the Rust binary
	// binds when no --dashboard-port is passed). Only when the user supplies
	// an explicit dashboard_port via WithConfig does that value override the
	// derivation. This means WithPort(17932) correctly reports the dashboard
	// at :17933 rather than the hardcoded 7933.
	gl.dashboardPort = gl.port + 1
	if gl.config != nil {
		if dp, ok := gl.config["dashboard_port"]; ok {
			n, err := toInt(dp)
			if err != nil {
				return nil, fmt.Errorf("invalid dashboard_port: %w", err)
			}
			gl.dashboardPort = n
		}
		// Validate invalidation_port up front so Wrap()'s later lookup via
		// detectInvalidationPort() is guaranteed to succeed.
		if ip, ok := gl.config["invalidation_port"]; ok {
			if _, err := toInt(ip); err != nil {
				return nil, fmt.Errorf("invalid invalidation_port: %w", err)
			}
		}
	}

	if err := gl.spawn(ctx); err != nil {
		return nil, err
	}
	registerStartedInstance(gl)
	return gl, nil
}

// spawn boots the underlying binary and opens the database pool. Called
// exclusively from Start.
//
// No gl.mu lock is held across spawn: the *GoldLapel being constructed is
// not yet visible to any other goroutine (Start hasn't returned, so no
// public method has a receiver yet, and registerStartedInstance runs only
// after spawn returns). Holding the mutex across the 10s port poll + 5s
// ping would serialise any concurrent accessor for the entire startup
// window once gl became visible — it was guarding against nothing and
// penalising every path that sees the instance after Start returns.
func (gl *GoldLapel) spawn(ctx context.Context) error {
	bin, err := FindBinary()
	if err != nil {
		return err
	}

	args := []string{"--upstream", gl.upstream, "--proxy-port", fmt.Sprintf("%d", gl.port)}
	if gl.config != nil {
		configArgs, err := ConfigToArgs(gl.config)
		if err != nil {
			return fmt.Errorf("invalid config: %w", err)
		}
		args = append(args, configArgs...)
	}
	args = append(args, gl.extraArgs...)

	gl.cmd = exec.Command(bin, args...)
	gl.cmd.Env = os.Environ()
	if os.Getenv("GOLDLAPEL_CLIENT") == "" {
		gl.cmd.Env = append(gl.cmd.Env, "GOLDLAPEL_CLIENT=go")
	}

	stderrPipe, err := gl.cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := gl.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start Gold Lapel: %w", err)
	}

	var stderrBuf strings.Builder
	stderrDone := make(chan struct{})
	go func() {
		io.Copy(&stderrBuf, stderrPipe)
		close(stderrDone)
	}()

	if !waitForPortCtx(ctx, "127.0.0.1", gl.port, startupTimeout) {
		gl.cmd.Process.Kill()
		gl.cmd.Wait()
		<-stderrDone
		gl.stderr = stderrBuf.String()
		gl.cmd = nil
		return fmt.Errorf("Gold Lapel failed to start on port %d within %ds.\nstderr: %s",
			gl.port, int(startupTimeout.Seconds()), gl.stderr)
	}

	gl.done = make(chan struct{})
	go func() {
		<-stderrDone
		gl.stderr = stderrBuf.String()
		// Capture Wait()'s error so Stop can surface it (E2). Stop
		// synchronises on <-gl.done, so the write here happens-before any
		// read of gl.waitErr on the Stop side — no additional locking
		// is needed for this field.
		gl.waitErr = gl.cmd.Wait()
		close(gl.done)
	}()

	gl.proxyURL = MakeProxyURL(gl.upstream, gl.port)

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

	gl.printBanner(os.Stderr)

	return nil
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
		fmt.Fprintf(w, "goldlapel → :%d (proxy) | http://127.0.0.1:%d (dashboard)\n", gl.port, gl.dashboardPort)
	} else {
		fmt.Fprintf(w, "goldlapel → :%d (proxy)\n", gl.port)
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

// Stop terminates the proxy process and closes the database pool.
// Safe to call multiple times. The context is honoured during the graceful
// shutdown wait — if ctx is cancelled the process is killed immediately.
//
// Return value contract:
//   - nil when the proxy shut down as expected — including the normal case
//     where Stop itself signalled SIGTERM/Kill (the resulting non-zero exit
//     is expected, not an error).
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

	// Unstarted / already-stopped: nothing to do.
	//
	// F: the guard chain used to have a third path (cmd != nil && done == nil)
	// that returned without nilling cmd — harmless in practice because
	// spawn's port-poll timeout path already nils cmd, but fragile if a
	// future spawn error path left cmd set without installing a reaper.
	// The check below consolidates to a single exit: if there's no reaper
	// channel to wait on, we clear any stale cmd defensively and return.
	// Callers never see such an instance today (Start returns nil on
	// failure), but this keeps Stop reliably idempotent under any
	// construction path (including hand-rolled test harnesses).
	if gl.done == nil {
		gl.cmd = nil
		return nil
	}

	// Fast path: process exited on its own before Stop was called. This
	// is NOT an our-signal shutdown — surface any Wait() error so callers
	// see e.g. a non-zero crash exit.
	select {
	case <-gl.done:
		gl.teardownLocked()
		return filterStopExit(gl.waitErr, gl.weSignaled)
	default:
	}

	if gl.db != nil {
		gl.db.Close()
		gl.db = nil
	}

	// Mark that we're about to issue a signal so the resulting ExitError
	// from cmd.Wait() can be filtered as expected-shutdown rather than
	// surfaced to the caller.
	gl.weSignaled = true

	if runtime.GOOS == "windows" {
		gl.cmd.Process.Kill()
	} else {
		gl.cmd.Process.Signal(syscall.SIGTERM)
	}

	select {
	case <-gl.done:
	case <-ctx.Done():
		gl.cmd.Process.Kill()
		<-gl.done
	case <-time.After(shutdownTimeout):
		gl.cmd.Process.Kill()
		<-gl.done
	}

	gl.teardownLocked()
	return filterStopExit(gl.waitErr, gl.weSignaled)
}

// teardownLocked nils out the process/pool state after the reaper has
// completed. Caller must hold gl.mu. Centralising this in one helper
// keeps Stop's two return paths (already-exited fast path, post-signal
// path) in sync — previously they diverged on whether gl.cmd was cleared.
func (gl *GoldLapel) teardownLocked() {
	if gl.db != nil {
		gl.db.Close()
		gl.db = nil
	}
	gl.done = nil
	gl.cmd = nil
	gl.proxyURL = ""
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

// Port returns the configured proxy port.
func (gl *GoldLapel) Port() int {
	return gl.port
}

// Running reports whether the proxy process is still alive.
func (gl *GoldLapel) Running() bool {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	if gl.done == nil {
		return false
	}
	select {
	case <-gl.done:
		return false
	default:
		return true
	}
}

// DashboardURL returns the dashboard URL while the proxy is running, or "".
func (gl *GoldLapel) DashboardURL() string {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	if gl.dashboardPort > 0 && gl.cmd != nil && gl.cmd.Process != nil {
		return fmt.Sprintf("http://127.0.0.1:%d", gl.dashboardPort)
	}
	return ""
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
		port:          gl.port,
		dashboardPort: gl.dashboardPort,
		cmd:           gl.cmd,
		proxyURL:      gl.proxyURL,
		done:          gl.done,
		db:            gl.db,
		tx:            tx,
	}
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

// MakeProxyURL rewrites an upstream connection string to point at the local proxy.
func MakeProxyURL(upstream string, port int) string {
	portStr := fmt.Sprintf("%d", port)

	if m := withPortRe.FindStringSubmatch(upstream); m != nil {
		return m[1] + "localhost:" + portStr + m[4]
	}

	if m := withoutPortRe.FindStringSubmatch(upstream); m != nil {
		return m[1] + "localhost:" + portStr + m[3]
	}

	if !strings.Contains(upstream, "://") && strings.Contains(upstream, ":") {
		return "localhost:" + portStr
	}

	return "localhost:" + portStr
}

// --- Port readiness ---

// WaitForPort polls until a TCP connection succeeds or the timeout expires.
func WaitForPort(host string, port int, timeout time.Duration) bool {
	return waitForPortCtx(context.Background(), host, port, timeout)
}

func waitForPortCtx(ctx context.Context, host string, port int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return false
		default:
		}
		conn, err := net.DialTimeout("tcp", addr, dialTimeout)
		if err == nil {
			conn.Close()
			return true
		}
		time.Sleep(startupPollInterval)
	}
	return false
}
