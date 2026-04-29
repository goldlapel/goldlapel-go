package goldlapel

// Streams namespace API — gl.Streams.<Verb>(...).
//
// Wraps the wire-level stream methods in a sub-API instance held on the
// parent GoldLapel client. State (license, dashboard token, db, ddl
// cache) is shared via the parent reference held in s.gl — no
// duplication.
//
// This is the canonical sub-API shape for the schema-to-core wrapper
// rollout. Other namespaces (cache, search, queues, counters, hashes,
// zsets, geo, auth, …) stay flat for now; they migrate to nested form
// one-at-a-time as their own schema-to-core phase fires.

import "context"

// Streams is the streams sub-API — accessible as gl.Streams. Methods take
// the stream name as the first argument; remaining args mirror the
// existing StreamX free-function signatures.
type Streams struct {
	gl *GoldLapel
}

// Add adds a payload to a stream. Like Redis XADD.
func (s *Streams) Add(ctx context.Context, stream string, payload string) (int64, error) {
	return StreamAdd(ctx, s.gl, stream, payload)
}

// CreateGroup creates a consumer group for a stream. Like Redis XGROUP_CREATE.
func (s *Streams) CreateGroup(ctx context.Context, stream, group string) error {
	return StreamCreateGroup(ctx, s.gl, stream, group)
}

// Read reads up to count messages for a consumer group. Like Redis XREADGROUP.
func (s *Streams) Read(ctx context.Context, stream, group, consumer string, count int) ([]StreamMessage, error) {
	return StreamRead(ctx, s.gl, stream, group, consumer, count)
}

// Ack acknowledges a message in a consumer group. Like Redis XACK.
func (s *Streams) Ack(ctx context.Context, stream, group string, messageID int64) (bool, error) {
	return StreamAck(ctx, s.gl, stream, group, messageID)
}

// Claim claims idle messages from other consumers. Like Redis XCLAIM.
func (s *Streams) Claim(ctx context.Context, stream, group, consumer string, minIdleMs int64) ([]StreamMessage, error) {
	return StreamClaim(ctx, s.gl, stream, group, consumer, minIdleMs)
}
