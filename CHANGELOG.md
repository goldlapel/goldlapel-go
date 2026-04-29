# Changelog

All notable changes to `goldlapel-go` are documented here. The Go module
itself is versioned via git tags — there is no in-file `Version` constant.

## Unreleased

### Breaking

- **Doc-store and streams moved to nested namespaces.** Replace
  `gl.Doc<Verb>(ctx, ...)` with `gl.Documents.<Verb>(ctx, ...)` and
  `gl.Stream<Verb>(ctx, ...)` with `gl.Streams.<Verb>(ctx, ...)`. The flat
  receiver methods were removed without aliases — search and replace once.
  See README "Document store and streams" for the canonical shape.
- **Doc-store DDL ownership moved to the proxy.** `gl.Documents.<Verb>`
  POSTs `/api/ddl/doc_store/create` on first call for each collection
  (idempotent), receives the canonical `_goldlapel.doc_<name>` table
  name, and runs SQL against that — instead of CREATE-ing tables in the
  user's schema. Per-session pattern cache lives on the *GoldLapel
  instance and is shared with the scoped instance returned by `InTx`.
- **`FetchPatterns` accepts variadic `DDLOption`s.** New
  `WithDDLOptions(map[string]interface{})` forwards per-family creation
  options (e.g. `unlogged: true` for doc_store). Existing call sites that
  pass no options compile unchanged.

### Added

- `*Documents` and `*Streams` sub-API types, plus `gl.Documents` /
  `gl.Streams` fields on `*GoldLapel`. Each holds a back-reference to the
  parent client (AWS-SDK pattern); state (license, dashboard token, db,
  pattern cache) is shared by reference, never duplicated.
- `DocUnlogged(bool)` option for `gl.Documents.CreateCollection` —
  forwards `options.unlogged` to the proxy on the create call.
- `gl.Documents.Aggregate` resolves `$lookup.from` collections through
  the proxy: each unique from-collection in the pipeline triggers an
  idempotent describe/create and is cached for the session.
- `SupportedVersion("doc_store")` returns `"v1"`.
