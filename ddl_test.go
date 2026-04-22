package goldlapel

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeDashboard wraps httptest.Server with canned responses + request capture.
type fakeDashboard struct {
	srv       *httptest.Server
	mu        sync.Mutex
	responses []fakeResponse
	captured  []fakeCapture
}

type fakeResponse struct {
	status int
	body   map[string]any
}
type fakeCapture struct {
	path    string
	headers http.Header
	body    map[string]any
}

func newFakeDashboard() *fakeDashboard {
	f := &fakeDashboard{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, _ := io.ReadAll(r.Body)
		var parsed map[string]any
		_ = json.Unmarshal(bodyBytes, &parsed)
		f.mu.Lock()
		f.captured = append(f.captured, fakeCapture{
			path: r.URL.Path, headers: r.Header.Clone(), body: parsed,
		})
		var resp fakeResponse
		if len(f.responses) > 0 {
			resp = f.responses[0]
			f.responses = f.responses[1:]
		} else {
			resp = fakeResponse{status: 500, body: map[string]any{"error": "no_response"}}
		}
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.status)
		_ = json.NewEncoder(w).Encode(resp.body)
	}))
	return f
}

func (f *fakeDashboard) queue(status int, body map[string]any) {
	f.mu.Lock()
	f.responses = append(f.responses, fakeResponse{status, body})
	f.mu.Unlock()
}

func (f *fakeDashboard) close() { f.srv.Close() }

func (f *fakeDashboard) port() int {
	// Extract :port from URL like "http://127.0.0.1:12345"
	u := f.srv.URL
	idx := strings.LastIndex(u, ":")
	portStr := u[idx+1:]
	var p int
	for _, c := range portStr {
		if c >= '0' && c <= '9' {
			p = p*10 + int(c-'0')
		}
	}
	return p
}

func TestSupportedVersion_Stream_IsV1(t *testing.T) {
	if got := SupportedVersion("stream"); got != "v1" {
		t.Fatalf("want v1, got %q", got)
	}
}

func TestFetchDDL_HappyPath_PostsCorrectBodyAndHeaders(t *testing.T) {
	f := newFakeDashboard()
	defer f.close()
	f.queue(200, map[string]any{
		"accepted":       true,
		"family":         "stream",
		"schema_version": "v1",
		"tables":         map[string]any{"main": "_goldlapel.stream_events"},
		"query_patterns": map[string]any{"insert": "INSERT ..."},
	})

	gl := &GoldLapel{dashboardPort: f.port(), dashboardToken: "tok"}
	entry, err := gl.FetchDDL(context.Background(), "stream", "events")
	if err != nil {
		t.Fatal(err)
	}
	if entry.Tables["main"] != "_goldlapel.stream_events" {
		t.Errorf("bad tables: %+v", entry.Tables)
	}
	if entry.QueryPatterns["insert"] != "INSERT ..." {
		t.Errorf("bad query_patterns: %+v", entry.QueryPatterns)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.captured) != 1 {
		t.Fatalf("want 1 request, got %d", len(f.captured))
	}
	c := f.captured[0]
	if c.path != "/api/ddl/stream/create" {
		t.Errorf("wrong path: %s", c.path)
	}
	if c.headers.Get("X-GL-Dashboard") != "tok" {
		t.Errorf("wrong token: %s", c.headers.Get("X-GL-Dashboard"))
	}
	if c.body["name"] != "events" {
		t.Errorf("wrong name: %v", c.body["name"])
	}
	if c.body["schema_version"] != "v1" {
		t.Errorf("wrong version: %v", c.body["schema_version"])
	}
}

func TestFetchDDL_CacheHit_DoesNotRePost(t *testing.T) {
	f := newFakeDashboard()
	defer f.close()
	f.queue(200, map[string]any{
		"tables":         map[string]any{"main": "x"},
		"query_patterns": map[string]any{"insert": "X"},
	})
	gl := &GoldLapel{dashboardPort: f.port(), dashboardToken: "tok"}
	a, err := gl.FetchDDL(context.Background(), "stream", "events")
	if err != nil {
		t.Fatal(err)
	}
	b, err := gl.FetchDDL(context.Background(), "stream", "events")
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Error("expected identical cached pointer")
	}
	f.mu.Lock()
	if len(f.captured) != 1 {
		t.Errorf("want 1 POST, got %d", len(f.captured))
	}
	f.mu.Unlock()
}

func TestFetchDDL_VersionMismatch_Actionable(t *testing.T) {
	f := newFakeDashboard()
	defer f.close()
	f.queue(409, map[string]any{
		"error":  "version_mismatch",
		"detail": "wrapper requested v1; proxy speaks v2 — upgrade proxy",
	})
	gl := &GoldLapel{dashboardPort: f.port(), dashboardToken: "tok"}
	_, err := gl.FetchDDL(context.Background(), "stream", "events")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "schema version mismatch") {
		t.Errorf("expected 'schema version mismatch', got %v", err)
	}
}

func TestFetchDDL_Forbidden_TokenError(t *testing.T) {
	f := newFakeDashboard()
	defer f.close()
	f.queue(403, map[string]any{"error": "forbidden"})
	gl := &GoldLapel{dashboardPort: f.port(), dashboardToken: "tok"}
	_, err := gl.FetchDDL(context.Background(), "stream", "events")
	if err == nil || !strings.Contains(err.Error(), "dashboard token") {
		t.Errorf("expected token error, got %v", err)
	}
}

func TestFetchDDL_MissingToken_ErrsBeforeHttp(t *testing.T) {
	// Use t.Setenv so cleanup is automatic.
	t.Setenv("GOLDLAPEL_DASHBOARD_TOKEN", "")
	// Also set HOME to a tempdir so no ~/.goldlapel/dashboard_token file
	// is found.
	t.Setenv("HOME", t.TempDir())
	gl := &GoldLapel{dashboardPort: 9999, dashboardToken: ""}
	_, err := gl.FetchDDL(context.Background(), "stream", "events")
	if err == nil || !strings.Contains(err.Error(), "No dashboard token") {
		t.Errorf("expected missing-token error, got %v", err)
	}
}

func TestFetchDDL_MissingPort_Errs(t *testing.T) {
	gl := &GoldLapel{dashboardPort: 0, dashboardToken: "tok"}
	_, err := gl.FetchDDL(context.Background(), "stream", "events")
	if err == nil || !strings.Contains(err.Error(), "No dashboard port") {
		t.Errorf("expected missing-port error, got %v", err)
	}
}

func TestFetchDDL_Unreachable_Actionable(t *testing.T) {
	gl := &GoldLapel{dashboardPort: 1, dashboardToken: "tok"}
	_, err := gl.FetchDDL(context.Background(), "stream", "events")
	if err == nil || !strings.Contains(err.Error(), "dashboard not reachable") {
		t.Errorf("expected unreachable error, got %v", err)
	}
}

