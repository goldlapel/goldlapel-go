package goldlapel

// DDL API client — fetches canonical helper-table DDL + query patterns from
// the Rust proxy's dashboard port so the wrapper never hand-writes CREATE
// TABLE for helper families (streams, docs, counters, ...).
//
// Architecture: see docs/wrapper-v0.2/SCHEMA-TO-CORE-PLAN.md in the goldlapel
// repo.
//
//   - One HTTP call per (family, name) per session (cached on *GoldLapel).
//   - Cache key: "family:name". Value: *DdlEntry.
//   - Errors: HTTP failures return errors with actionable text.
//
// Token + port resolution:
//   - (gl *GoldLapel).dashboardToken is populated when the wrapper spawned the
//     proxy (happy path).
//   - For externally-launched proxies, tokenFromEnvOrFile() reads
//     GOLDLAPEL_DASHBOARD_TOKEN env or ~/.goldlapel/dashboard-token.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var supportedVersions = map[string]string{
	"stream": "v1",
}

// DdlEntry holds the canonical table names and query patterns for a helper.
type DdlEntry struct {
	Tables        map[string]string `json:"tables"`
	QueryPatterns map[string]string `json:"query_patterns"`
}

// SupportedVersion returns the schema version the wrapper pins for family.
func SupportedVersion(family string) string {
	return supportedVersions[family]
}

// TokenFromEnvOrFile resolves the dashboard token for externally-launched
// proxies. Returns "" if nothing is set; caller should surface an actionable
// error.
func TokenFromEnvOrFile() string {
	if t := strings.TrimSpace(os.Getenv("GOLDLAPEL_DASHBOARD_TOKEN")); t != "" {
		return t
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	path := filepath.Join(home, ".goldlapel", "dashboard-token")
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// httpPoster is the injection seam tests use to swap the HTTP layer.
type httpPoster func(ctx context.Context, url, token string, body []byte) (status int, respBody []byte, err error)

// defaultPost is the production implementation; tests assign a counting/fake
// version to ddlPost (package-level, test-only).
var ddlPost httpPoster = func(ctx context.Context, url, token string, body []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GL-Dashboard", token)

	c := &http.Client{Timeout: 10 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf(
			"Gold Lapel dashboard not reachable at %s: %w. "+
				"Is `goldlapel` running? The dashboard port must be open "+
				"for helper families (streams, docs, ...) to work.",
			url, err,
		)
	}
	defer resp.Body.Close()
	b, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return resp.StatusCode, nil, readErr
	}
	return resp.StatusCode, b, nil
}

// FetchDDL fetches (and caches per-instance) the canonical {tables,
// query_patterns} for a helper on a specific GoldLapel instance.
func (gl *GoldLapel) FetchDDL(ctx context.Context, family, name string) (*DdlEntry, error) {
	key := family + ":" + name
	if cached, ok := gl.ddlCache.Load(key); ok {
		return cached.(*DdlEntry), nil
	}

	token := gl.dashboardToken
	if token == "" {
		token = TokenFromEnvOrFile()
	}
	if token == "" {
		return nil, fmt.Errorf(
			"No dashboard token available. Set GOLDLAPEL_DASHBOARD_TOKEN or let " +
				"goldlapel.Start spawn the proxy (which provisions a token automatically).",
		)
	}
	port := gl.dashboardPort
	if port <= 0 {
		return nil, fmt.Errorf(
			"No dashboard port available. Gold Lapel's helper families (%s, ...) "+
				"require the proxy's dashboard to be reachable.",
			family,
		)
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/api/ddl/%s/create", port, family)
	reqBody, _ := json.Marshal(map[string]string{
		"name":           name,
		"schema_version": SupportedVersion(family),
	})

	status, respBody, err := ddlPost(ctx, url, token, reqBody)
	if err != nil {
		return nil, err
	}

	if status != http.StatusOK {
		// Try to unpack an error envelope.
		var e struct {
			Error    string `json:"error"`
			Detail   string `json:"detail"`
			Canonical string `json:"canonical,omitempty"`
			Requested string `json:"requested,omitempty"`
		}
		_ = json.Unmarshal(respBody, &e)
		if status == http.StatusConflict && e.Error == "version_mismatch" {
			return nil, fmt.Errorf(
				"Gold Lapel schema version mismatch for %s '%s': %s. "+
					"Upgrade the proxy or the wrapper so versions agree.",
				family, name, e.Detail,
			)
		}
		if status == http.StatusForbidden {
			return nil, fmt.Errorf(
				"Gold Lapel dashboard rejected the DDL request (403). " +
					"The dashboard token is missing or incorrect — check " +
					"GOLDLAPEL_DASHBOARD_TOKEN or ~/.goldlapel/dashboard-token.",
			)
		}
		errMsg := e.Error
		if errMsg == "" {
			errMsg = "unknown"
		}
		detail := e.Detail
		if detail == "" {
			detail = string(respBody)
		}
		return nil, fmt.Errorf(
			"Gold Lapel DDL API %s/%s failed with %d %s: %s",
			family, name, status, errMsg, detail,
		)
	}

	var entry DdlEntry
	if err := json.Unmarshal(respBody, &entry); err != nil {
		return nil, fmt.Errorf("Gold Lapel DDL API returned invalid JSON: %w", err)
	}
	// LoadOrStore handles the concurrent-fetch case: first one wins.
	actual, _ := gl.ddlCache.LoadOrStore(key, &entry)
	return actual.(*DdlEntry), nil
}

// StreamPatterns is a typed convenience for callers that only want the
// query_patterns map.
func (gl *GoldLapel) streamPatterns(ctx context.Context, stream string) (map[string]string, error) {
	entry, err := gl.FetchDDL(ctx, "stream", stream)
	if err != nil {
		return nil, err
	}
	return entry.QueryPatterns, nil
}

// requireStreamPattern returns the SQL for the given pattern key or an error.
// database/sql with lib/pq uses PostgreSQL's numbered-placeholder style
// natively, so the proxy's $1/$2 patterns pass through verbatim.
func requireStreamPattern(qp map[string]string, key, fn string) (string, error) {
	if qp == nil {
		return "", fmt.Errorf(
			"%s requires DDL patterns from the proxy — call via "+
				"gl.%s(...) rather than the goldlapel.%s module function directly.",
			fn, fn, fn,
		)
	}
	sql, ok := qp[key]
	if !ok {
		return "", fmt.Errorf("DDL API response missing pattern '%s' for %s", key, fn)
	}
	return sql, nil
}
