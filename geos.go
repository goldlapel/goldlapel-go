package goldlapel

// Geos namespace API — gl.Geos.<Verb>(ctx, name, ...).
//
// Phase 5 of schema-to-core. The proxy's v1 geo schema uses GEOGRAPHY (not
// GEOMETRY), `member TEXT PRIMARY KEY` (not BIGSERIAL + name), and a GIST
// index on the location column. `Geos.Add` is idempotent on the member name
// — re-adding a member updates its location.
//
// Distance unit: methods accept m / km / mi / ft. The proxy column is
// meters-native (GEOGRAPHY default); wrappers convert at the edge.
//
// Param contract (Go pgx supports $N natively — no $→%s translation):
//   - Radius / RadiusWithDist: pass (lon, lat, radius_m, limit) — 4 args.
//   - RadiusByMember: pass (member, member, radius_m, limit) — $1 + $2 are
//     both the anchor member.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Geos is the geos sub-API — accessible as gl.Geos.
type Geos struct {
	gl *GoldLapel
}

// GeoMember is one row returned by Radius / RadiusByMember.
type GeoMember struct {
	Member    string
	Lon       float64
	Lat       float64
	DistanceM float64
}

// LonLat is the (lon, lat) pair returned by Add and Pos.
type LonLat struct {
	Lon float64
	Lat float64
}

// Distance unit conversion — proxy returns meters always (GEOGRAPHY default);
// wrappers translate at the edge so callers can ask in km / mi / ft.
var geoUnits = map[string]float64{
	"m":  1.0,
	"km": 1000.0,
	"mi": 1609.344,
	"ft": 0.3048,
}

func toMeters(value float64, unit string) (float64, error) {
	factor, ok := geoUnits[unit]
	if !ok {
		return 0, fmt.Errorf("unknown distance unit %q (choose m/km/mi/ft)", unit)
	}
	return value * factor, nil
}

func fromMeters(meters float64, unit string) (float64, error) {
	factor, ok := geoUnits[unit]
	if !ok {
		return 0, fmt.Errorf("unknown distance unit %q (choose m/km/mi/ft)", unit)
	}
	return meters / factor, nil
}

func (g *Geos) patterns(ctx context.Context, name string) (*DdlEntry, error) {
	if err := validateIdentifier(name); err != nil {
		return nil, err
	}
	return g.gl.FetchPatterns(ctx, "geo", name)
}

func (g *Geos) requirePattern(entry *DdlEntry, key, verb string) (string, error) {
	if entry == nil || entry.QueryPatterns == nil {
		return "", fmt.Errorf("geo/%s: proxy returned no query_patterns", verb)
	}
	sqlStr, ok := entry.QueryPatterns[key]
	if !ok {
		return "", fmt.Errorf("geo/%s: missing pattern %q", verb, key)
	}
	return sqlStr, nil
}

// Create eagerly materializes the geo table on the proxy.
func (g *Geos) Create(ctx context.Context, name string) error {
	_, err := g.patterns(ctx, name)
	return err
}

// Add idempotently set-or-updates a member's lon/lat. Returns the just-
// stored (lon, lat).
func (g *Geos) Add(ctx context.Context, name, member string, lon, lat float64, opts ...Option) (LonLat, error) {
	entry, err := g.patterns(ctx, name)
	if err != nil {
		return LonLat{}, err
	}
	sqlStr, err := g.requirePattern(entry, "geoadd", "add")
	if err != nil {
		return LonLat{}, err
	}
	q, err := g.gl.resolveExec(opts)
	if err != nil {
		return LonLat{}, err
	}
	var out LonLat
	row := q.QueryRowContext(ctx, sqlStr, member, lon, lat)
	if err := row.Scan(&out.Lon, &out.Lat); err != nil {
		return LonLat{}, fmt.Errorf("geo add %s/%s: %w", name, member, err)
	}
	return out, nil
}

// Pos fetches a member's (lon, lat). The bool is false if the member is
// absent.
func (g *Geos) Pos(ctx context.Context, name, member string, opts ...Option) (LonLat, bool, error) {
	entry, err := g.patterns(ctx, name)
	if err != nil {
		return LonLat{}, false, err
	}
	sqlStr, err := g.requirePattern(entry, "geopos", "pos")
	if err != nil {
		return LonLat{}, false, err
	}
	q, err := g.gl.resolveExec(opts)
	if err != nil {
		return LonLat{}, false, err
	}
	var out LonLat
	row := q.QueryRowContext(ctx, sqlStr, member)
	if err := row.Scan(&out.Lon, &out.Lat); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return LonLat{}, false, nil
		}
		return LonLat{}, false, fmt.Errorf("geo pos %s/%s: %w", name, member, err)
	}
	return out, true, nil
}

// Dist returns the distance between two members in the requested unit. The
// bool is false if either member is absent.
func (g *Geos) Dist(ctx context.Context, name, memberA, memberB, unit string, opts ...Option) (float64, bool, error) {
	if unit == "" {
		unit = "m"
	}
	entry, err := g.patterns(ctx, name)
	if err != nil {
		return 0, false, err
	}
	sqlStr, err := g.requirePattern(entry, "geodist", "dist")
	if err != nil {
		return 0, false, err
	}
	q, err := g.gl.resolveExec(opts)
	if err != nil {
		return 0, false, err
	}
	var meters sql.NullFloat64
	row := q.QueryRowContext(ctx, sqlStr, memberA, memberB)
	if err := row.Scan(&meters); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("geo dist %s/%s/%s: %w", name, memberA, memberB, err)
	}
	if !meters.Valid {
		return 0, false, nil
	}
	out, err := fromMeters(meters.Float64, unit)
	if err != nil {
		return 0, false, err
	}
	return out, true, nil
}

// Radius returns members within `radius` of (lon, lat). Returns []GeoMember
// (member, lon, lat, distance_m). Distance is always in meters in the result
// (proxy-native).
//
// Param contract: the proxy's `georadius_with_dist` pattern is CTE-anchored
// so each $N appears exactly once: $1=lon, $2=lat, $3=radius_m, $4=limit.
// pgx binds $N natively — no translation needed.
func (g *Geos) Radius(ctx context.Context, name string, lon, lat, radius float64, unit string, limit int, opts ...Option) ([]GeoMember, error) {
	if unit == "" {
		unit = "m"
	}
	if limit <= 0 {
		limit = 50
	}
	entry, err := g.patterns(ctx, name)
	if err != nil {
		return nil, err
	}
	sqlStr, err := g.requirePattern(entry, "georadius_with_dist", "radius")
	if err != nil {
		return nil, err
	}
	q, err := g.gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	radiusM, err := toMeters(radius, unit)
	if err != nil {
		return nil, err
	}
	rows, err := q.QueryContext(ctx, sqlStr, lon, lat, radiusM, limit)
	if err != nil {
		return nil, fmt.Errorf("geo radius %s: %w", name, err)
	}
	defer rows.Close()
	var out []GeoMember
	for rows.Next() {
		var m GeoMember
		if err := rows.Scan(&m.Member, &m.Lon, &m.Lat, &m.DistanceM); err != nil {
			return nil, fmt.Errorf("geo radius scan: %w", err)
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// RadiusByMember returns members within `radius` of `member`'s location.
//
// Param contract: the proxy's `geosearch_member` pattern uses $1 and $2 both
// as the anchor member name (one for the join, one for the self-exclusion);
// $3=radius_m, $4=limit. pgx binds $N natively, so we pass
// (member, member, radius_m, limit).
func (g *Geos) RadiusByMember(ctx context.Context, name, member string, radius float64, unit string, limit int, opts ...Option) ([]GeoMember, error) {
	if unit == "" {
		unit = "m"
	}
	if limit <= 0 {
		limit = 50
	}
	entry, err := g.patterns(ctx, name)
	if err != nil {
		return nil, err
	}
	sqlStr, err := g.requirePattern(entry, "geosearch_member", "radius_by_member")
	if err != nil {
		return nil, err
	}
	q, err := g.gl.resolveExec(opts)
	if err != nil {
		return nil, err
	}
	radiusM, err := toMeters(radius, unit)
	if err != nil {
		return nil, err
	}
	rows, err := q.QueryContext(ctx, sqlStr, member, member, radiusM, limit)
	if err != nil {
		return nil, fmt.Errorf("geo radius_by_member %s/%s: %w", name, member, err)
	}
	defer rows.Close()
	var out []GeoMember
	for rows.Next() {
		var m GeoMember
		if err := rows.Scan(&m.Member, &m.Lon, &m.Lat, &m.DistanceM); err != nil {
			return nil, fmt.Errorf("geo radius_by_member scan: %w", err)
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// Remove drops a member; true if removed, false if absent.
func (g *Geos) Remove(ctx context.Context, name, member string, opts ...Option) (bool, error) {
	entry, err := g.patterns(ctx, name)
	if err != nil {
		return false, err
	}
	sqlStr, err := g.requirePattern(entry, "geo_remove", "remove")
	if err != nil {
		return false, err
	}
	q, err := g.gl.resolveExec(opts)
	if err != nil {
		return false, err
	}
	res, err := q.ExecContext(ctx, sqlStr, member)
	if err != nil {
		return false, fmt.Errorf("geo remove %s/%s: %w", name, member, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// Count returns the total members in the namespace.
func (g *Geos) Count(ctx context.Context, name string, opts ...Option) (int64, error) {
	entry, err := g.patterns(ctx, name)
	if err != nil {
		return 0, err
	}
	sqlStr, err := g.requirePattern(entry, "geo_count", "count")
	if err != nil {
		return 0, err
	}
	q, err := g.gl.resolveExec(opts)
	if err != nil {
		return 0, err
	}
	var out int64
	if err := q.QueryRowContext(ctx, sqlStr).Scan(&out); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("geo count %s: %w", name, err)
	}
	return out, nil
}
