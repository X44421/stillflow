# Issue #5 — Implementation Contract

> **Issue**: [backend: define Arrow connector contracts and domain types](https://github.com/X44421/stillflow/issues/5)
> **Risk**: `risk:high`
> **Branch**: `agent/arrow-connector-contracts`
> **Status**: Implemented — pending Sol review and merge
> **Architecture**: [`docs/data-ingestion-architecture.md`](../data-ingestion-architecture.md)

## Summary

Freeze the Arrow-based connector boundary and core ingestion domain model before any source-specific adapter (Polars, Calamine, SQLx, object_store) is implemented.

## Goals

- Define `SourceConnector` with test, discover, inspect, preview, batch-read, and checkpoint operations.
- Define `ConnectorCapabilities` with explicit capability negotiation.
- Define domain types: `SourceConnection`, `SourceAsset`, `AssetMetadata`, `PreviewRequest`, `PreviewData`, `ReadRequest`, `Checkpoint`, `DatasetSnapshot`, `Session`, `Dataset`.
- Define `BatchStream` with cancellation and deadline propagation.
- Define stable error categories, retryability, and sanitized error context.
- Define object/event mapping for Session, SourceConnection, SourceAsset, Dataset, Snapshot.
- Define `ConnectorRegistry` keyed by `ConnectorKind`.

## Non-goals

- File, workbook, object-store, or SQL implementations.
- HTTP API routes or UI integration.
- Polars, DuckDB, SQLx, or Axum dependencies.

## Allowed crates and files

| Crate | Allowed changes |
| --- | --- |
| `stillflow-core` | `src/domain/*`, `src/error/*`, `src/events/*`, `src/request/*`, `src/stream/*`, `src/lib.rs`, `src/serde_tests.rs`, `Cargo.toml` |
| `stillflow-connectors` | `src/capabilities.rs`, `src/connector.rs`, `src/registry.rs`, `src/lib.rs`, `Cargo.toml` |
| `stillflow-engine` | Smoke-test wiring only (`src/lib.rs`) |
| `stillflow-api` | Smoke-test wiring only (`src/lib.rs`) |
| Workspace root | `backend/Cargo.toml`, `backend/Cargo.lock` |

## Frozen public API (post-merge)

Downstream issues **must not modify** these without a new high-risk contract:

### `stillflow_connectors::SourceConnector`

```rust
#[async_trait]
pub trait SourceConnector: Send + Sync {
    fn kind(&self) -> ConnectorKind;
    fn capabilities(&self) -> ConnectorCapabilities;
    async fn test_connection(&self) -> ConnectorResult<ConnectionStatus>;
    async fn discover(&self, request: DiscoverRequest) -> ConnectorResult<Vec<SourceAsset>>;
    async fn inspect(&self, asset: &SourceAsset) -> ConnectorResult<AssetMetadata>;
    async fn preview(&self, request: PreviewRequest) -> ConnectorResult<PreviewData>;
    async fn read_batches(&self, request: ReadRequest) -> ConnectorResult<BatchStream>;
    async fn checkpoint(&self, asset: &SourceAsset) -> ConnectorResult<Option<Checkpoint>>;
}
```

### Stream boundary

```rust
pub type BatchItem = Result<RecordBatch, ConnectorError>;
pub type BatchStream = Pin<Box<dyn Stream<Item = BatchItem> + Send>>;
```

`attach_request_context(stream, RequestContext)` wraps streams to honour cancellation and deadlines.

### Capability negotiation

`ConnectorCapabilities::ensure(Capability)` returns `ConnectorError::for_unsupported_capability(...)` — never silent degradation.

### Error categories

`Authentication`, `Authorization`, `NotFound`, `InvalidConfiguration`, `InvalidData`, `SchemaDrift`, `RateLimited`, `Timeout`, `Cancelled`, `UnsupportedCapability`, `TransientSource`, `Internal`.

### Secret policy

- `SourceConnection` stores `CredentialRef`, not raw secrets.
- `ensure_no_secret_fields` rejects config keys matching password/secret/token/api_key patterns.
- `sanitize_message` redacts credential fragments in user-visible messages.
- `IngestionEvent` carries `SanitizedErrorSummary` only — no internal chains.

## Allowed workspace dependencies (new)

| Crate | Purpose |
| --- | --- |
| `arrow-array`, `arrow-schema` | Arrow boundary (replaces `arrow` meta crate) |
| `async-trait` | Object-safe `SourceConnector` |
| `futures` | `BatchStream` |
| `tokio`, `tokio-util` | `RequestContext`, cancellation token |
| `chrono`, `serde`, `serde_json`, `uuid`, `thiserror` | Domain serialization and errors |

## Dependency rules

- `stillflow-core` must not depend on any other workspace crate.
- No Polars, DuckDB, SQLx, Axum, or full `arrow` meta crate.

## Cancellation and deadline semantics

- `RequestContext` carries `CancellationToken` and optional `Instant` deadline.
- `ensure_active()` returns `Cancelled` or `Timeout` before I/O.
- `CancellableBatchStream` checks context on every `poll_next`.

## Test matrix (required)

| Area | Tests |
| --- | --- |
| Capability negotiation | Unsupported capability → `UnsupportedCapability`, not retryable |
| Error sanitization | Password fragments redacted; nested secret keys rejected |
| Cancellation | Preview and stream honour cancelled token |
| Deadline | Expired deadline detected |
| Event mapping | Session/Connection/Asset/Dataset/Snapshot events; no secrets in JSON |
| Registry | Dynamic dispatch; duplicate kind rejected |
| Serde round-trip | `SourceConnection`, `SourceAsset`, `Checkpoint`, `Session`, `Dataset`, `DatasetSnapshot` |

## Acceptance mapping

| Criterion | Implementation |
| --- | --- |
| Arrow schemas and bounded `RecordBatch` streams | `PreviewData`, `BatchStream`, `AssetMetadata` |
| Dynamic dispatch | `SourceConnectorRef = Arc<dyn SourceConnector>`, `ConnectorRegistry` |
| `UnsupportedCapability` on missing optimization | `ConnectorCapabilities::ensure`, `for_unsupported_capability` |
| Preview limits + projection + filter + deadline + cancellation | `PreviewRequest`, `RequestContext` |
| No secrets in domain/events | `CredentialRef`, `ensure_no_secret_fields`, event tests |
| Unit tests | 21 workspace tests |
| API docs + serde tests | Module docs + `serde_tests.rs` |

## Stop conditions for downstream issues (#6+)

Adapters must **stop and escalate** if they need to change:

- `SourceConnector` method signatures
- `ReadRequest` / `PreviewRequest` fields
- `BatchStream` type alias
- `ConnectorError` / `ErrorCategory`
- `RequestContext` semantics

## Review checklist for Sol

- [ ] Crate dependency direction preserved
- [ ] No `arrow` meta crate; only `arrow-array` + `arrow-schema`
- [ ] No Polars/DuckDB/SQLx/Axum in `stillflow-core`
- [ ] Third-party types do not leak past connector boundary in public API
- [ ] Unsupported capabilities fail explicitly
- [ ] Cancellation propagates through streams
- [ ] Secrets absent from serialized domain objects and events
- [ ] No unauthorized frontend changes
- [ ] CI: fmt, clippy, test, frontend build
