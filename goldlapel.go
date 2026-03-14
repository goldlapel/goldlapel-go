package goldlapel

import (
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
	DefaultPort         = 7932
	startupTimeout      = 10 * time.Second
	startupPollInterval = 50 * time.Millisecond
	shutdownTimeout     = 5 * time.Second
	dialTimeout         = 500 * time.Millisecond
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
		"redis_url": true, "pool_size": true, "pool_timeout_secs": true, "pool_mode": true,
		"mgmt_idle_timeout": true, "fallback": true, "read_after_write_secs": true,
		"n1_threshold": true, "n1_window_ms": true, "n1_cross_threshold": true,
		"tls_cert": true, "tls_key": true, "tls_client_ca": true, "config": true,
		"dashboard_port": true,
		"disable_matviews": true, "disable_consolidation": true, "disable_btree_indexes": true,
		"disable_trigram_indexes": true, "disable_expression_indexes": true,
		"disable_partial_indexes": true, "disable_rewrite": true, "disable_prepared_cache": true,
		"disable_result_cache": true, "disable_redis_cache": true, "disable_pool": true,
		"disable_n1": true, "disable_n1_cross_connection": true, "disable_shadow_mode": true,
		"enable_coalescing": true,
		"replica": true, "exclude_tables": true,
	}

	booleanKeys = map[string]bool{
		"disable_matviews": true, "disable_consolidation": true, "disable_btree_indexes": true,
		"disable_trigram_indexes": true, "disable_expression_indexes": true,
		"disable_partial_indexes": true, "disable_rewrite": true, "disable_prepared_cache": true,
		"disable_result_cache": true, "disable_redis_cache": true, "disable_pool": true,
		"disable_n1": true, "disable_n1_cross_connection": true, "disable_shadow_mode": true,
		"enable_coalescing": true,
	}

	listKeys = map[string]bool{
		"replica":        true,
		"exclude_tables": true,
	}
)

// Option configures a GoldLapel instance.
type Option func(*GoldLapel)

// WithPort sets the proxy listen port.
func WithPort(port int) Option {
	return func(gl *GoldLapel) {
		gl.port = port
	}
}

// WithExtraArgs passes additional CLI flags to the binary.
func WithExtraArgs(args ...string) Option {
	return func(gl *GoldLapel) {
		gl.extraArgs = args
	}
}

// WithConfig passes structured configuration as CLI flags to the binary.
// Keys are snake_case strings mapping to CLI flags (e.g. "pool_size" → "--pool-size").
func WithConfig(config map[string]interface{}) Option {
	return func(gl *GoldLapel) {
		gl.config = config
	}
}

// ConfigToArgs converts a config map into CLI argument strings.
// Keys are snake_case strings; each is validated against the known set of config keys.
// Boolean keys emit a bare flag when true, nothing when false.
// List keys emit repeated --flag value pairs for each element.
// All other keys emit --flag value pairs.
func ConfigToArgs(config map[string]interface{}) ([]string, error) {
	if len(config) == 0 {
		return nil, nil
	}

	// Sort keys for deterministic output
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

// GoldLapel manages a Gold Lapel proxy process.
type GoldLapel struct {
	upstream  string
	port      int
	config    map[string]interface{}
	extraArgs []string
	cmd       *exec.Cmd
	proxyURL  string
	stderr    string
	done      chan struct{} // closed when process exits
	mu        sync.Mutex
}

// New creates a new GoldLapel instance.
func New(upstream string, opts ...Option) *GoldLapel {
	gl := &GoldLapel{
		upstream: upstream,
		port:     DefaultPort,
	}
	for _, opt := range opts {
		opt(gl)
	}
	return gl
}

// Start spawns the proxy and returns the proxy connection string.
func (gl *GoldLapel) Start() (string, error) {
	gl.mu.Lock()
	defer gl.mu.Unlock()

	if gl.done != nil {
		select {
		case <-gl.done:
			// Process exited, fall through to restart
		default:
			// Still running
			return gl.proxyURL, nil
		}
	}

	bin, err := FindBinary()
	if err != nil {
		return "", err
	}

	args := []string{"--upstream", gl.upstream, "--port", fmt.Sprintf("%d", gl.port)}
	if gl.config != nil {
		configArgs, err := ConfigToArgs(gl.config)
		if err != nil {
			return "", fmt.Errorf("invalid config: %w", err)
		}
		args = append(args, configArgs...)
	}
	args = append(args, gl.extraArgs...)

	gl.cmd = exec.Command(bin, args...)

	stderrPipe, err := gl.cmd.StderrPipe()
	if err != nil {
		return "", fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := gl.cmd.Start(); err != nil {
		return "", fmt.Errorf("failed to start Gold Lapel: %w", err)
	}

	// Drain stderr in background to prevent deadlock
	var stderrBuf strings.Builder
	stderrDone := make(chan struct{})
	go func() {
		io.Copy(&stderrBuf, stderrPipe)
		close(stderrDone)
	}()

	if !WaitForPort("127.0.0.1", gl.port, startupTimeout) {
		// Startup failed — kill and collect stderr
		gl.cmd.Process.Kill()
		gl.cmd.Wait()
		<-stderrDone
		gl.stderr = stderrBuf.String()
		gl.cmd = nil
		return "", fmt.Errorf("Gold Lapel failed to start on port %d within %ds.\nstderr: %s",
			gl.port, int(startupTimeout.Seconds()), gl.stderr)
	}

	gl.done = make(chan struct{})
	go func() {
		<-stderrDone
		gl.stderr = stderrBuf.String()
		gl.cmd.Wait()
		close(gl.done)
	}()

	gl.proxyURL = MakeProxyURL(gl.upstream, gl.port)
	return gl.proxyURL, nil
}

// Stop terminates the proxy process.
func (gl *GoldLapel) Stop() error {
	gl.mu.Lock()
	defer gl.mu.Unlock()

	if gl.done == nil {
		return nil
	}

	// Check if already exited
	select {
	case <-gl.done:
		gl.done = nil
		gl.proxyURL = ""
		return nil
	default:
	}

	// Graceful shutdown: SIGTERM on Unix, Kill on Windows (no SIGTERM support)
	if runtime.GOOS == "windows" {
		gl.cmd.Process.Kill()
	} else {
		gl.cmd.Process.Signal(syscall.SIGTERM)
	}

	select {
	case <-gl.done:
		// Exited
	case <-time.After(shutdownTimeout):
		// Force kill (only needed for Unix SIGTERM path)
		gl.cmd.Process.Kill()
		<-gl.done
	}

	gl.done = nil
	gl.proxyURL = ""
	return nil
}

// URL returns the proxy connection string, or "" if not running.
func (gl *GoldLapel) URL() string {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	return gl.proxyURL
}

// Port returns the configured proxy port.
func (gl *GoldLapel) Port() int {
	return gl.port
}

// Running returns true if the proxy process is alive.
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

// --- Singleton API ---

var (
	instance   *GoldLapel
	singletonMu sync.Mutex
)

// Start creates or reuses a singleton proxy instance.
func Start(upstream string, opts ...Option) (string, error) {
	singletonMu.Lock()
	defer singletonMu.Unlock()

	if instance != nil && instance.Running() {
		if instance.upstream != upstream {
			return "", fmt.Errorf("Gold Lapel is already running for a different upstream. Call goldlapel.Stop() before starting with a new upstream.")
		}
		return instance.URL(), nil
	}

	instance = New(upstream, opts...)
	return instance.Start()
}

// Stop stops the singleton proxy instance.
func Stop() error {
	singletonMu.Lock()
	defer singletonMu.Unlock()

	if instance == nil {
		return nil
	}

	err := instance.Stop()
	instance = nil
	return err
}

// ProxyURL returns the singleton's proxy URL, or "" if not started.
func ProxyURL() string {
	singletonMu.Lock()
	defer singletonMu.Unlock()

	if instance == nil {
		return ""
	}
	return instance.URL()
}

// --- Binary lookup ---

// FindBinary locates the Gold Lapel binary using a 3-tier lookup.
func FindBinary() (string, error) {
	// 1. GOLDLAPEL_BINARY env var
	if envPath := os.Getenv("GOLDLAPEL_BINARY"); envPath != "" {
		info, err := os.Stat(envPath)
		if err != nil || !info.Mode().IsRegular() {
			return "", fmt.Errorf("GOLDLAPEL_BINARY points to %s but file not found", envPath)
		}
		return envPath, nil
	}

	// 2. Bundled binary relative to this source file
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
			return bundled, nil
		}
	}

	// 3. System PATH
	if path, err := exec.LookPath("goldlapel"); err == nil {
		return path, nil
	}

	return "", fmt.Errorf("Gold Lapel binary not found. Set GOLDLAPEL_BINARY env var, install the platform-specific package, or ensure 'goldlapel' is on PATH.")
}

func isMusl(arch string) bool {
	_, err := os.Stat(fmt.Sprintf("/lib/ld-musl-%s.so.1", arch))
	return err == nil
}

// --- URL rewriting ---

// MakeProxyURL rewrites an upstream connection string to point at the local proxy.
func MakeProxyURL(upstream string, port int) string {
	portStr := fmt.Sprintf("%d", port)

	// PostgreSQL URL with explicit port
	if m := withPortRe.FindStringSubmatch(upstream); m != nil {
		return m[1] + "localhost:" + portStr + m[4]
	}

	// PostgreSQL URL without explicit port
	if m := withoutPortRe.FindStringSubmatch(upstream); m != nil {
		return m[1] + "localhost:" + portStr + m[3]
	}

	// Bare host:port (no ://)
	if !strings.Contains(upstream, "://") && strings.Contains(upstream, ":") {
		return "localhost:" + portStr
	}

	// Bare hostname
	return "localhost:" + portStr
}

// --- Port readiness ---

// WaitForPort polls until a TCP connection succeeds or the timeout expires.
func WaitForPort(host string, port int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))

	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, dialTimeout)
		if err == nil {
			conn.Close()
			return true
		}
		time.Sleep(startupPollInterval)
	}
	return false
}
