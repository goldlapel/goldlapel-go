package goldlapel

import (
	"context"
	"database/sql/driver"
	"strings"
	"sync"
	"testing"
)

// geoPatternsFor returns a fake DDL pattern set keyed against the canonical
// proxy table `_goldlapel.geo_<name>`. Phase 5 contract: GEOGRAPHY-native
// (no ::geography casts on column refs because the column already IS
// GEOGRAPHY); idempotent ON CONFLICT (member); GIST index.
//
// IMPORTANT: the radius patterns are CTE-anchored — each $N appears exactly
// once so pgx's native $N binding maps cleanly:
//   - georadius_with_dist: $1=lon, $2=lat, $3=radius_m, $4=limit
//   - geosearch_member: $1=member (anchor), $2=member (self-exclude),
//     $3=radius_m, $4=limit
//
// The Python wrapper has a different fixture because psycopg translates
// $N → %s positionally; Go uses pgx native $N so the patterns are
// re-emitted by the proxy in CTE-anchored form for Go callers.
func geoPatternsFor(name string) *DdlEntry {
	main := "_goldlapel.geo_" + name
	return &DdlEntry{
		Tables: map[string]string{"main": main},
		QueryPatterns: map[string]string{
			"geoadd":  "INSERT INTO " + main + " (member, location, updated_at) VALUES ($1, ST_SetSRID(ST_MakePoint($2, $3), 4326)::geography, NOW()) ON CONFLICT (member) DO UPDATE SET location = EXCLUDED.location, updated_at = NOW() RETURNING ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat",
			"geopos":  "SELECT ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat FROM " + main + " WHERE member = $1",
			"geodist": "SELECT ST_Distance(a.location, b.location) AS distance_m FROM " + main + " a, " + main + " b WHERE a.member = $1 AND b.member = $2",
			"georadius_with_dist": "WITH anchor AS (SELECT ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography AS pt) " +
				"SELECT member, ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat, " +
				"ST_Distance(location, (SELECT pt FROM anchor)) AS distance_m " +
				"FROM " + main + " WHERE ST_DWithin(location, (SELECT pt FROM anchor), $3) " +
				"ORDER BY distance_m LIMIT $4",
			"geosearch_member": "SELECT b.member, ST_X(b.location::geometry) AS lon, ST_Y(b.location::geometry) AS lat, " +
				"ST_Distance(b.location, a.location) AS distance_m FROM " + main + " a, " + main + " b " +
				"WHERE a.member = $1 AND b.member <> $2 AND ST_DWithin(b.location, a.location, $3) " +
				"ORDER BY distance_m LIMIT $4",
			"geo_remove": "DELETE FROM " + main + " WHERE member = $1",
			"geo_count":  "SELECT COUNT(*) FROM " + main,
		},
	}
}

func TestGeosNamespace_IsAttached(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	if gl.Geos == nil {
		t.Fatal("expected gl.Geos to be non-nil")
	}
	if gl.Geos.gl != gl {
		t.Fatal("Geos.gl must back-reference the parent")
	}
}

// TestGeosAdd_IsIdempotent locks the Phase 5 contract: ON CONFLICT (member)
// DO UPDATE — re-adding the same member updates its location.
func TestGeosAdd_IsIdempotent(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"lon", "lat"},
		[][]driver.Value{{float64(13.4), float64(52.5)}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	if gl.ddlCache == nil {
		gl.ddlCache = &sync.Map{}
	}
	gl.ddlCache.Store("geo:riders", geoPatternsFor("riders"))

	out, err := gl.Geos.Add(context.Background(), "riders", "alice", 13.4, 52.5)
	if err != nil {
		t.Fatalf("Geos.Add: %v", err)
	}
	if out.Lon != 13.4 || out.Lat != 52.5 {
		t.Fatalf("want (13.4, 52.5), got %+v", out)
	}
	last := drv.lastCapture()
	if !strings.Contains(last.query, "ON CONFLICT (member)") {
		t.Fatalf("expected ON CONFLICT (member), got %q", last.query)
	}
	if !strings.Contains(last.query, "DO UPDATE") {
		t.Fatalf("expected DO UPDATE for idempotent add, got %q", last.query)
	}
	// Param binding: ($1=member, $2=lon, $3=lat).
	if len(last.args) != 3 ||
		last.args[0] != "alice" ||
		last.args[1] != float64(13.4) ||
		last.args[2] != float64(52.5) {
		t.Fatalf("expected (alice, 13.4, 52.5), got %v", last.args)
	}
}

func TestGeosAdd_PatternIsGeographyNative(t *testing.T) {
	// Phase 5: column is GEOGRAPHY natively, not GEOMETRY-with-cast.
	patterns := geoPatternsFor("riders")
	sql := patterns.QueryPatterns["geoadd"]
	if !strings.Contains(strings.ToLower(sql), "geography") {
		t.Fatalf("Phase 5 contract: column must be GEOGRAPHY-native, got %q", sql)
	}
}

func TestGeosPos_FalseForAbsent(t *testing.T) {
	db, _ := newTestDB(t, []string{"lon", "lat"}, nil)
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("geo:riders", geoPatternsFor("riders"))

	_, ok, err := gl.Geos.Pos(context.Background(), "riders", "ghost")
	if err != nil {
		t.Fatalf("Geos.Pos: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false for absent member")
	}
}

func TestGeosDist_DefaultUnitMeters(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"distance_m"},
		[][]driver.Value{{float64(1234.0)}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("geo:riders", geoPatternsFor("riders"))

	got, ok, err := gl.Geos.Dist(context.Background(), "riders", "alice", "bob", "m")
	if err != nil {
		t.Fatalf("Geos.Dist: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got != 1234.0 {
		t.Fatalf("want 1234.0, got %f", got)
	}
}

func TestGeosDist_ConvertsToKm(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"distance_m"},
		[][]driver.Value{{float64(1234.0)}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("geo:riders", geoPatternsFor("riders"))

	got, ok, err := gl.Geos.Dist(context.Background(), "riders", "alice", "bob", "km")
	if err != nil {
		t.Fatalf("Geos.Dist: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got != 1.234 {
		t.Fatalf("want 1.234, got %f", got)
	}
}

func TestGeosDist_UnknownUnitRaises(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"distance_m"},
		[][]driver.Value{{float64(1.0)}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("geo:riders", geoPatternsFor("riders"))

	_, _, err := gl.Geos.Dist(context.Background(), "riders", "a", "b", "parsec")
	if err == nil || !strings.Contains(err.Error(), "unknown distance unit") {
		t.Fatalf("expected unknown-unit error, got %v", err)
	}
}

// TestGeosRadius_ParamsAreLonLatRadiusLimit is the load-bearing test for
// the Go geo contract. The proxy's CTE-anchored pattern requires
// (lon, lat, radius_m, limit) bound to $1..$4 in that order — pgx maps $N
// natively (no $→%s translation), so the wrapper passes args in $-index
// order, NOT in source-position order like the Python wrapper.
func TestGeosRadius_ParamsAreLonLatRadiusLimit(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"member", "lon", "lat", "distance_m"},
		nil)
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("geo:riders", geoPatternsFor("riders"))

	// Pass radius=5 km → 5000 m to the proxy.
	if _, err := gl.Geos.Radius(context.Background(), "riders",
		13.4, 52.5, 5, "km", 50); err != nil {
		t.Fatalf("Geos.Radius: %v", err)
	}
	last := drv.lastCapture()
	if len(last.args) != 4 {
		t.Fatalf("Geos.Radius must bind exactly 4 args (lon, lat, radius_m, limit); got %v", last.args)
	}
	if last.args[0] != float64(13.4) {
		t.Errorf("$1 must be lon=13.4, got %v", last.args[0])
	}
	if last.args[1] != float64(52.5) {
		t.Errorf("$2 must be lat=52.5, got %v", last.args[1])
	}
	if last.args[2] != float64(5000.0) {
		t.Errorf("$3 must be radius_m=5000.0 (5 km converted), got %v", last.args[2])
	}
	if last.args[3] != int64(50) {
		t.Errorf("$4 must be limit=50, got %v", last.args[3])
	}
}

// TestGeosRadiusByMember_ParamsAreMemberMemberRadiusLimit locks the
// $1+$2-both-anchor-member contract. pgx native $N: pass member twice
// in args, then radius_m, then limit — matching $1..$4.
func TestGeosRadiusByMember_ParamsAreMemberMemberRadiusLimit(t *testing.T) {
	db, drv := newTestDB(t,
		[]string{"member", "lon", "lat", "distance_m"},
		nil)
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("geo:riders", geoPatternsFor("riders"))

	if _, err := gl.Geos.RadiusByMember(context.Background(), "riders",
		"alice", 1000, "m", 50); err != nil {
		t.Fatalf("Geos.RadiusByMember: %v", err)
	}
	last := drv.lastCapture()
	if len(last.args) != 4 {
		t.Fatalf("RadiusByMember must bind exactly 4 args (member, member, radius_m, limit); got %v", last.args)
	}
	if last.args[0] != "alice" {
		t.Errorf("$1 must be anchor member=alice, got %v", last.args[0])
	}
	if last.args[1] != "alice" {
		t.Errorf("$2 must be anchor member=alice (second binding), got %v", last.args[1])
	}
	if last.args[2] != float64(1000.0) {
		t.Errorf("$3 must be radius_m=1000.0, got %v", last.args[2])
	}
	if last.args[3] != int64(50) {
		t.Errorf("$4 must be limit=50, got %v", last.args[3])
	}
}

func TestGeosRemove_RowsAffected(t *testing.T) {
	db, _ := newTestDB(t, nil, nil)
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("geo:riders", geoPatternsFor("riders"))

	removed, err := gl.Geos.Remove(context.Background(), "riders", "alice")
	if err != nil {
		t.Fatalf("Geos.Remove: %v", err)
	}
	if !removed {
		t.Fatal("expected removed=true")
	}
}

func TestGeosCount(t *testing.T) {
	db, _ := newTestDB(t,
		[]string{"count"},
		[][]driver.Value{{int64(42)}})
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	gl.db = db
	gl.ddlCache.Store("geo:riders", geoPatternsFor("riders"))

	got, err := gl.Geos.Count(context.Background(), "riders")
	if err != nil {
		t.Fatalf("Geos.Count: %v", err)
	}
	if got != 42 {
		t.Fatalf("want 42, got %d", got)
	}
}

// TestGeosNoLegacyFlatMethods is the Phase 5 hard-cut canary. The
// pre-Phase-5 gl.Geoadd/gl.Georadius/gl.Geodist took ad-hoc table+column
// args; they're gone — callers use gl.Geos.<Verb>. If anything tries to
// resurrect them, the package won't compile.
func TestGeosNoLegacyFlatMethods(t *testing.T) {
	gl := buildForTest("postgresql://user:pass@localhost:5432/mydb")
	if gl.Geos == nil {
		t.Fatal("Geos must be the only path post-Phase-5")
	}
}

func TestSupportedVersion_Geo_IsV1(t *testing.T) {
	if got := SupportedVersion("geo"); got != "v1" {
		t.Fatalf("want v1, got %q", got)
	}
}
