// Pool integration: ensure DISCARD ALL fires on connection return.
//
// Connection pools (pgxpool, database/sql, etc.) recycle physical
// connections across logical "checkouts" — a connection that user A
// SET app.user_id = '42' on can be borrowed by user B who never sees
// the SET. Without a pool-level reset hook, user B's queries inherit
// user A's session state.
//
// PG's `DISCARD ALL` clears the entire session state (every non-
// default GUC, prepared statements, temp tables, advisory locks,
// listens). Issuing it as part of pool-release is the canonical fix —
// see the pgx project's docs and the PG manual entry on DISCARD.
//
// This file provides decoupled helpers:
//
//   - `PoolReleaseDiscarder`: wraps a per-connection state tracker so
//     callers can ask "should I issue DISCARD ALL now?" and "the SQL
//     just ran, mark state as clean". Idempotent; an already-clean
//     connection skips the DISCARD round trip.
//
//   - `OnAfterRelease`: a higher-level helper that takes a generic
//     exec callback and runs the DISCARD ALL + state-clean cycle.
//     Designed to be invoked from inside pgxpool.Config.AfterRelease
//     (or any equivalent pool hook) without requiring this package to
//     import pgxpool / pgx.
//
// The wrapper deliberately does NOT take a hard dependency on pgx /
// pgxpool — goldlapel-go already has zero external runtime deps
// (only `lib/pq` for the database/sql probe, used at Start). Adding
// pgx would force every consumer to compile in pgxpool whether they
// use it or not. The decoupled helper pattern lets pgx users wire
// the integration in one line without polluting non-pgx consumers.
//
// For `database/sql` callers (which expose no per-pool hook —
// sql.DB.Conn → sql.Conn provides only a Close that returns the
// connection to the pool), the verify-on-checkout fallback in
// CachedConn.maybeVerifyOnCheckout is the safety net: a connection
// recycled with stale state will trigger a verify on the next
// CachedConn checkout that observes IsDirty.

package goldlapel

import (
	"context"
	"sync/atomic"
)

// PoolReleaseDiscarder tracks whether a logical pgxpool-style
// connection has any state worth DISCARDing on return to the pool.
// One PoolReleaseDiscarder per *pgx.Conn (or equivalent) — the
// user constructs one alongside their pool config and queries it
// from their AfterRelease hook.
//
// State machine:
//
//   - dirty=false → ShouldDiscard returns false; AfterRelease can
//     skip the DISCARD ALL round trip. This is the steady state for
//     connections whose users never SET anything unsafe.
//   - dirty=true  → ShouldDiscard returns true; the AfterRelease hook
//     issues DISCARD ALL and then calls MarkClean to reset the flag.
//
// MarkDirty is intended to be called by user code that SETs an unsafe
// GUC outside the wrapper's view (e.g. raw conn.Exec("SET ..." )
// without going through CachedConn.Exec). Inside CachedConn,
// ObserveSQL already updates the wrapper's per-connection state
// hash; PoolReleaseDiscarder is the orthogonal pool-level
// mechanism.
//
// Concurrency-safe via atomic.Bool. The pgxpool model is one
// connection-one-borrower at a time, so contention is rare; the
// atomic is for safety against unexpected goroutines (e.g. a
// finalizer-style cleanup goroutine).
type PoolReleaseDiscarder struct {
	dirty atomic.Bool
}

// NewPoolReleaseDiscarder returns a fresh tracker in the clean
// state.
func NewPoolReleaseDiscarder() *PoolReleaseDiscarder {
	return &PoolReleaseDiscarder{}
}

// MarkDirty signals that this connection has had session state set
// since the last DISCARD. Called by user code after issuing a SET /
// SET LOCAL / set_config that's unsafe to leak across pool borrowers.
func (p *PoolReleaseDiscarder) MarkDirty() {
	p.dirty.Store(true)
}

// MarkClean resets the dirty flag. Called after a successful
// DISCARD ALL (or after observing one in the SQL stream).
func (p *PoolReleaseDiscarder) MarkClean() {
	p.dirty.Store(false)
}

// ShouldDiscard reports whether the connection has any state that
// warrants a DISCARD ALL on return to the pool.
func (p *PoolReleaseDiscarder) ShouldDiscard() bool {
	return p.dirty.Load()
}

// OnAfterRelease is the canonical pgxpool.Config.AfterRelease helper.
// It runs the discard-on-release cycle: if state is dirty, issue
// `DISCARD ALL` via the supplied exec callback and mark state clean.
//
// The exec callback is generic by design — callers pass a closure
// that bridges to their pool's connection type (typically pgx.Conn):
//
//	cfg, _ := pgxpool.ParseConfig(databaseURL)
//	discarder := goldlapel.NewPoolReleaseDiscarder()
//	cfg.AfterRelease = func(c *pgx.Conn) bool {
//	    return goldlapel.OnAfterRelease(context.Background(), discarder,
//	        func(ctx context.Context, sql string) error {
//	            _, err := c.Exec(ctx, sql)
//	            return err
//	        })
//	}
//
// Returns true on success (pgxpool convention: "keep the connection
// in the pool"); returns false if the DISCARD failed, signalling the
// pool to drop the connection rather than recycle it with potentially
// stale state. A clean connection (ShouldDiscard=false) skips the
// round trip and returns true immediately.
//
// The exec callback receives the literal string "DISCARD ALL" — no
// arguments, no parameters. Callers can substitute their own SQL
// (e.g. a tighter `RESET ROLE; RESET SESSION AUTHORIZATION` pair) by
// not using this helper and rolling their own AfterRelease, but the
// PG-blessed pattern is DISCARD ALL.
func OnAfterRelease(ctx context.Context, p *PoolReleaseDiscarder, exec func(ctx context.Context, sql string) error) bool {
	if p == nil {
		// No tracker means "always-clean" by convention — the caller
		// opted out of pool-level DISCARD. Return true so pgxpool
		// keeps the connection.
		return true
	}
	if !p.ShouldDiscard() {
		return true
	}
	if exec == nil {
		// Caller bug — they wired the discarder but forgot the exec
		// callback. Returning false signals pgxpool to drop the
		// connection (defence in depth: better to recreate than to
		// recycle dirty state).
		return false
	}
	if err := exec(ctx, "DISCARD ALL"); err != nil {
		return false
	}
	p.MarkClean()
	return true
}

// AttachDiscarderTo wires a PoolReleaseDiscarder to a CachedConn's
// GUC-state events: every observed SET / RESET / DISCARD that moves
// the wrapper's hash flips the discarder's dirty flag, and observed
// DISCARD ALL / RESET ALL clear it.
//
// Optional convenience — callers can also just call
// discarder.MarkDirty() themselves after issuing a raw SET. The
// wrapper-level integration ensures that SETs routed through
// CachedConn.Exec (the most common path) are tracked automatically,
// without the user having to remember.
//
// Returns the CachedConn unchanged so the call chain reads naturally:
//
//	cc := goldlapel.AttachDiscarderTo(goldlapel.Wrap(conn, port), discarder)
func AttachDiscarderTo(cc *CachedConn, p *PoolReleaseDiscarder) *CachedConn {
	if cc == nil || p == nil {
		return cc
	}
	cc.poolDiscarder = p
	return cc
}
