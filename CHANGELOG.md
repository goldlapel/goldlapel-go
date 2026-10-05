# Changelog

All notable changes to `goldlapel-go` are documented here. The Go module
itself is versioned via git tags — there is no in-file `Version` constant.

## Unreleased

### Breaking

- **The in-process cache (L1) is gone.** The proxy's result cache now
  serves every client the same way, so the wrapper no longer carries its
  own. Deleted with it: `NativeCache` / `GetNativeCache` /
  `ResetNativeCache`, `Wrap` and `CachedConn` (plus the `Querier` / `Rows`
  / `Row` / `FieldDescription` interfaces and `ErrNoRows` that existed only
  for it), the session-settings tracker (`ConnectionGucState`,
  `ParseSetCommand`, `SplitStatements`, `IsUnsafeGUC`, ...), aggressive
  post-DML verify (`AggressiveVerifyMode`), the invalidation-socket client
  and its stats reporting, and the pool DISCARD helpers
  (`PoolReleaseDiscarder`, `OnAfterRelease`, `AttachDiscarderTo`). Use the
  plain `*sql.DB` from your driver (`gl.DB()` or `sql.Open(..., gl.URL())`).
- **Removed options, no aliases:** `WithInvalidationPort`,
  `WithDisableNativeCache`, `WithReportStats`, `WithAggressiveVerify`,
  `WithDisableMatviews`, and the `gl.InvalidationPort()` accessor. The
  `GOLDLAPEL_NATIVE_CACHE`, `GOLDLAPEL_NATIVE_CACHE_SIZE` and
  `GOLDLAPEL_REPORT_STATS` env vars are no longer read. The proxy now uses
  two ports: proxy and dashboard (proxy + 1).
- **Removed `WithConfig` keys** for materialized views, which the proxy no
  longer has: `refresh_interval_secs`, `pattern_ttl_secs`,
  `max_tables_per_view`, `max_columns_per_view`, `disable_consolidation`,
  `disable_rewrite`, `disable_shadow_mode`. `enable_coalescing` is replaced
  by `disable_coalescing`, matching the proxy (coalescing is on by default).
  Passing a removed key fails at `Start`.
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
