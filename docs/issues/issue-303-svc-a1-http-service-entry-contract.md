# SVC-A1 — HTTP Service Entry Frozen Implementation Contract

> Status: Frozen · Issue: [#303](https://github.com/X44421/stillflow/issues/303) · Risk class: **L3**
> Base: `main@0af8f38a28dce5dccbe357f77bbb3e2048e36982` · Ledger: #81 · Client roadmap: [openship#1](https://github.com/X44421/openship/issues/1)
> Boundary authority: [H3 release gate](../evidence/release/h3.md) — HTTP listener wiring, process installers, and OS service registration are explicitly outside the transport-neutral backend contract and are added here, and only here, as an adapter.

## 1. Objective and risk class

Deliver a startable StillFlow service process: an HTTP listener that maps the
authoritative route manifest onto the existing `ApiService` method surface, a
binary that assembles the stack (`ControlPlaneStore`/`SnapshotStore`,
connectors, `ExecutionEngine`, `JobRuntime`) and owns process lifecycle
(start, ready, graceful shutdown, restart reconciliation). After SVC-A1 a real
client can complete **handshake → submit → query → cancel → artifact read**
over TCP, and shutdown/restart behavior is verifiable.

L3 because it adds a new process-level public runtime surface and a cross-crate
composition root. It changes **no** existing public contract: every request is
served by existing `ApiService` handlers; the HTTP layer is a pure adapter and
is never the source of domain semantics (per #93).

## 2. Authorized public changes (additive only)

1. New workspace member `backend/crates/stillflow-service` (composition root;
   depends on `stillflow-api`, `stillflow-engine`, `stillflow-connectors`,
   `stillflow-storage`, `stillflow-core` — direction stays one-way, no cycles).
2. New binary target `stillflow-server` inside that crate.
3. Root `backend/Cargo.toml`: one workspace member entry and shared dependency
   declarations (`axum`, `tower`); `Cargo.lock` regenerated accordingly.
4. `stillflow-service` enables the off-by-default `event-stream` feature of
   `stillflow-api` (feature unification is disclosed here; the feature gate
   itself is unchanged).
5. **No modification** to any existing crate's public API, schema, error
   taxonomy, limits, or storage format.

## 3. Transport contract

### 3.1 Server shape

- HTTP/1.1, JSON payloads, `Content-Type: application/json` (except SSE).
- Bind address/port: `ServiceConfig::bind_host` / `bind_port` (`0` = ephemeral).
- Limits inherited from `ApiLimits` and enforced at the adapter: request body
  cap = `max_request_bytes` via axum body limit; global in-flight request cap =
  `max_concurrent_requests` via a concurrency layer; request timeout =
  `request_timeout_seconds`. `ApiLimits::bounded()` narrowing is honored.
- Envelope unchanged: success → `200` with `ApiResponse<T>` JSON; failure →
  mapped status with `ApiErrorResponse` JSON. `code`/`message` remain the
  authoritative error contract; the status code is advisory.

### 3.2 Status mapping (fixed table)

| `ApiErrorCode`      | HTTP status |
| ------------------- | ----------- |
| UnsupportedVersion  | 400         |
| InvalidRequest      | 400         |
| NotFound            | 404         |
| Conflict            | 409         |
| LimitExceeded       | 413         |
| Unauthorized        | 401         |
| Internal            | 500         |

### 3.3 Request carrying (uniform rule)

- Non-GET manifest routes: request body is the full `ApiRequest<T>` envelope.
- GET manifest routes (request schema `EmptyRequest` excepted): the envelope is
  reassembled from query parameters — meta keys `apiVersion`, `requestId`,
  `workspaceId`, `idempotencyKey`, `principal` (optional), plus the top-level
  body fields by their camelCase names. Each value is JSON-parsed when possible
  (numbers, booleans, `null`, nested objects/arrays), otherwise taken as a raw
  string. Rationale: `fetch()` forbids GET bodies, so the Web client cannot use
  envelope bodies on GET; query reassembly keeps manifest ↔ handler mapping 1:1.
  An optional `principal` is reassembled from its JSON-encoded form (the serde
  form of `RequestPrincipal`); callers that must not expose a principal in a
  URL should prefer routes whose manifest method carries a body. Under
  `server` authorization a GET that cannot reassemble a principal fails closed
  with 401 (`Unauthorized`), never a degraded local-trusted fallback.
- API-version fail-closed behavior is the existing `ApiService` validation
  (unknown version → `unsupported_version`); the adapter adds no second
  version registry.

### 3.4 SSE event extension

- `GET /v1/events/stream` — query: `workspaceId`, `streamKind`, `streamId`,
  `cursor` (optional sequence), `limit`, `requestId`. Response:
  `text/event-stream`; each frame is one `EventFrame` JSON payload.
- Backed by `EventStreamService` (durable log is the only replay authority; the
  pump is its existing 20 ms bounded poller; buffer/subscriber bounds are its
  existing defaults, expressible via `with_bounds`).
- Pre-stream failures map through the §3.2 table: `InvalidRequest`/`InvalidCursor`
  → 400, `StreamNotFound` → 404, `SubscriberLimit` → 429, `DurableStateUnavailable`
  → 500. A `SlowConsumer` after the stream opens terminates the stream; clients
  recover by cursor replay (`event.list` / replay), never by a second log.
- This path is a transport extension and is intentionally **not** added to the
  route manifest.

## 4. Process contract

### 4.1 Composition root (startup order)

1. Load process config (§4.2), `ServiceConfig::validate()` must pass.
2. `SnapshotStore::open(managed_root/"store", StorageLimits::default())`;
   control plane via `snapshot_store.control_plane()` (single SQLite schema,
   same as the E5-G1 gate stack).
3. `ConnectorRegistry` with the three bounded connectors
   (local-tabular, workbook, object-store); one registry for the engine, one
   for `ApiService`, as in the gate stack.
4. `ExecutionEngine::new(engine_registry)`.
5. `DurableJobRequestResolver` — **new production resolver** (none exists on
   `main`; only the E5-G1 test's `GateResolver` does). Semantics fixed here:
   reload `PlanVersion` → locate the `Scan` node's `source_asset_id` → load
   asset + connection records → rebuild `SourceConnection`/`SourceAsset` from
   stored `safe_config`/`safe_locator` only → locate the dataset bound to that
   asset by paging `list_datasets` (cap 1000, the storage page bound,
   fail-closed if absent) →
   `batch_size` from the operation descriptor (`Materialize` → materialize
   policy, `Verification` → verification policy, otherwise 1024). All reads are
   durable-store reads; no process-local dispatch state crosses the boundary.
6. Workspace binding (see §4.3), `JobRuntime::new_with_system_identity(...)`,
   `runtime.start().await` (restart reconciliation inside the library).
7. `ApiService::new(control_plane).with_connectors(...).with_engine(...).
   with_runtime(...).with_snapshot_store(...).with_limits(...)`.
   with_authorization_mode(...)` — then bind and accept.

### 4.2 Process config and CLI

- Binary: `stillflow-server --config <path> [--bind-host H] [--bind-port P]
  [--port-file <path>]`.
- Config file (JSON, camelCase) wraps the frozen `ServiceConfig` verbatim plus
  process-level fields: `authorizationMode` (`local-trusted` | `server`) and a
  **required** `workspaceId` (UUID) — the process serves exactly that
  workspace. Config files must never carry credentials — only `credential://`
  refs pass `ServiceConfig` validation.
- Ready announcement: exactly one JSON line on stdout
  `{"event":"ready","pid":…,"bindHost":…,"port":…,"apiVersion":1,
  "transport":"desktop-local"}`; `--port-file` additionally writes that object
  to the given path for tooling.
- Startup is fail-closed: invalid config, storage open failure, or workspace
  invariant violation abort the process before bind.

### 4.3 Workspace binding (single-workspace invariant)

`JobRuntime` is constructed with one workspace id. SVC-A1 therefore serves
**exactly one workspace per process** (DesktopLocal): the configured
`workspaceId` must exist or be creatable at startup — absent: bootstrap-create
and announce on the ready line; present: adopt; creation conflicting with a
different existing row or any storage error: fail-closed. `ControlPlaneStore`
has no workspace listing API on `main` and adding one is a storage-surface
change out of SVC-A1 scope, so broader multi-workspace detection/refusal and
WebRemote serving must be chartered separately.

### 4.4 Shutdown and restart

- SIGINT/SIGTERM → `DaemonLifecycle.begin_shutdown()` (Draining) → axum
  graceful shutdown notify → wait for in-flight requests up to
  `shutdown_grace_seconds` (hard cap; force stop after) → `runtime.shutdown()`
  (cancels workers, joins) → `complete_shutdown()` → exit code 0.
- Manifest health routes stay served by `ApiService` unchanged during draining;
  the adapter does not synthesize a second health authority.
- Restart = run the binary again against the same managed root: reconciliation
  is the library path (`reconcile_on_start` + `SnapshotStore::recover`);
  process-level restart behavior must be observable in e2e (§6 T6).

## 5. Invariants and bounds

- Envelope, error taxonomy, page limits (≤1024), payload bounds (2 MiB default
  request cap, 64 KiB event payload), SSE subscriber cap (64) and buffer (64)
  are inherited from existing authorities, never restated locally.
- No secrets in config files, logs, frames, or fixtures (`CredentialRef` only).
- Adapter adds no domain logic: every manifest route delegates to exactly one
  `ApiService` method; no route synthesizes responses.
- `OpenAPI`-facing output remains `openapi_representation()`; the adapter wraps
  it only with `info`/`servers` in its OpenAPI GET handler if provided, and
  never edits `routes`/`schemas`.

## 6. Objective acceptance tests

| ID  | Test |
| --- | --- |
| T1  | Real-TCP handshake: supported version succeeds; unknown version fails closed (`unsupported_version`, HTTP 400). |
| T2  | Real-TCP client loop over a temp fixture: workspace/session/connection register → discover/inspect/preview → dataset/plan create → plan version save/publish → `job.submit` → poll `job.read` to terminal → `artifact.list`/`read`/`content`. |
| T3  | `job.cancel` on a queued job reaches `Cancelled` terminal state over TCP. |
| T4  | `event.list` cursor pagination; SSE stream delivers frames for a submitted job and resumes from a cursor. |
| T5  | Spawned binary (temp managed root): ready line parses; SIGTERM → exit 0 within grace; port file written when requested. |
| T6  | Process restart on the same managed root: a queued job left by the previous process is reconciled (worker_lost/Failed or completed) and state/events remain queryable — process-level equivalent of the E5-G1 restart cases. |
| T7  | Manifest coverage: test iterates `E5_A1_ROUTES` and asserts every (method, path) is registered; route set ⊆ manifest always. |

Delivery is staged: PR-1 lands the adapter, process binary, and T1–T7 with the
client-loop route subset; SVC-A1 closes only at 100% manifest coverage (T7 over
the full manifest).

**Staging note — typed-binary response views (2026-09-05, at PR-1; resolved by
§6.1 in the closing PR, 2026-09-06).**
`asset.preview`, `engine.preview`, and `artifact.content` were excluded from
PR-1: their response views (`PreviewView`, `EnginePreviewView`,
`ArtifactContentPage`) carry typed binary `Vec<BatchEnvelope>` payloads and are
deliberately not `Serialize` (the API crate's own doc: preview is "typed binary
data; not coerced into an unbounded JSON value"). Serving them over HTTP
required a binary wire-format decision — Arrow IPC framing per AGENTS rule 3/4
— which is a contract-level change, not an adapter detail. That framing is now
frozen as §6.1; the closing PR wires the three routes to it and extends T7 to
the full manifest. In PR-1, T2's artifact verification therefore used
`artifact.list`/`artifact.read` (metadata + digest) only.

### 6.1 Typed-binary response wire format (frozen 2026-09-06)

Applies to exactly the three manifest routes whose response schema is
`PreviewView`, `EnginePreviewView`, or `ArtifactContentPage` (`asset.preview`,
`engine.preview`, `artifact.content`). Every other route keeps §3.1 JSON
envelope semantics unchanged.

Success response:

- HTTP 200 with `Content-Type: application/vnd.apache.arrow.stream` (the IANA
  media type of the Arrow IPC stream format).
- The body is one Arrow IPC **stream**: exactly one schema message, then one
  record-batch message per `BatchEnvelope` in the view's batch order, then the
  end-of-stream marker. For the two preview views the stream schema is
  `logical_schema_to_arrow(view.schema)`; for `artifact.content` it is the
  payload schema shared by the page's batches (an empty schema message when
  the page carries zero batches).
- All StillFlow metadata rides the schema message's `custom_metadata` under
  the single key `stillflow.wire.v1` — one UTF-8 JSON object (camelCase):

  ```json
  {
    "wireVersion": 1,
    "meta": { "apiVersion": 1, "requestId": "<uuid>" },
    "envelope": {
      "version": 1,
      "schemaFingerprint": [<32 bytes>],
      "sourceAssetId": "<uuid>"
    },
    "batches": [
      { "sequence": 0, "rowCount": 100, "byteCount": 20480 }
    ],
    "view": { "…": "route-specific scalars below" }
  }
  ```

  - `meta` is the serde JSON of `ApiResponse::meta` (`ResponseMetadata`):
    the §3.1 envelope semantics are preserved verbatim on the binary path.
  - `envelope` is the shared `BatchEnvelope` identity of every batch in the
    response (`version`, `schemaFingerprint` as the serde JSON of
    `LogicalSchemaFingerprint`, `sourceAssetId`); it is `null` for a
    zero-batch view.
  - `batches` is index-parallel to the record-batch messages; `byteCount` is
    the authoritative envelope value, not re-derivable from the stream.
  - `view` is exactly one of:
    - `asset.preview` → `{"view":"assetPreview","rowsReturned":…,
      "bytesReturned":…,"rowsTruncated":…,"bytesTruncated":…,
      "warnings":[…],"schema":…}` where `"schema"` is the serde JSON of the
      view's `LogicalSchema`.
    - `engine.preview` → `{"view":"enginePreview","planFingerprint":"…",
      "targetNodeId":"<uuid>","rowsReturned":…,"bytesReturned":…,
      "sourceRowsScanned":…,"sourceBytesScanned":…,"rowsTruncated":…,
      "bytesTruncated":…,"scanTruncated":…,"sourceExhausted":…,"schema":…}`
      with the same `"schema"` member.
    - `artifact.content` → `{"view":"artifactContent",
      "nextPartitionSequence": <u32|null>}`.
- Uniformity is fail-closed: all envelopes of one response must share
  `version`, `schema_fingerprint`, and `source_asset_id`, and every payload
  must match the stream schema. Divergence maps to `Internal` (500) before
  any bytes are written — a response is never a partial stream.
- Failures keep the §3.2 JSON `ApiErrorResponse` mapping; page bounds
  (`max_rows`/`max_bytes`, preview row/byte limits) remain enforced upstream
  by `ApiService` exactly as in-process, so §5 limits inheritance is
  unchanged and the response body stays bounded by the same limits.
- This is **not** a second envelope serialization: batches are never
  JSON-encoded, and the metadata JSON carries only the scalar envelope/view
  fields the in-process views already expose.
- Reader rules (client contract): `batches[i].rowCount` must equal
  `batch_message[i].num_rows()`, `batches.len()` must equal the number of
  record-batch messages, `wireVersion` must be recognized, and the message
  order defines sequence order. Any mismatch is transport corruption and must
  fail closed.

## 7. Ordered checklist

1. Workspace member + Cargo manifests (axum/tower workspace deps, lock update).
2. Adapter core: error mapping, envelope extractors (body + GET query), limits
   layers.
3. Route table: client-loop subset (handshake, health×3, metrics, workspace,
   member/role/service-account, credentials, sessions, connections, assets,
   datasets, plans, engine.preview, jobs, runs, events, artifacts, exports,
   automations) → then the remainder to 100%.
4. SSE endpoint + `EventStreamService` wiring.
5. `DurableJobRequestResolver`.
6. Process config + binary + ready line + signal handling + grace shutdown.
7. T1–T7 e2e suite (`tokio::test` + `reqwest` dev-dep; process tests via
   `CARGO_BIN_EXE_stillflow-server`).
8. Registry claim `service:http-entry` (taskctl CAS path; if taskctl remains
   unavailable in the environment, the PR discloses the limitation and the
   claim window).
9. Gates: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets
   -- -D warnings`, `cargo test --workspace`, `npm run typecheck`, `npm run build`.

## 8. New dependencies

- `axum` (0.8.x, workspace-pinned), `tower` (0.5.x, features `limit` +
  `timeout`), `tower-http` (0.6.x, feature `timeout` for the 408 request
  timeout layer) — runtime.
- Dev-only: `reqwest` (0.12, `default-features = false`, features `json`),
  `libc` (test signal delivery). `tempfile` reuses the workspace pin.
- `tokio` gains the `signal` feature (additive feature unification).

## 9. Risks and stop conditions

- GET query reassembly must match openship #22's generated client; the
  carrying rule (§3.3) is the binding text for that codegen. Any divergence
  found during FE1-S4 stops and returns here.
- Single-workspace binding is a deliberate SVC-A1 bound; multi-workspace or
  WebRemote multi-tenant serving is a new charter, not an extension.
- If implementation requires changes to `ApiError`, `ApiLimits`, manifest
  schema names, or new storage methods, stop and return to contract review.
- Registry CAS unavailability does not gate implementation start (single
  writer, disclosed), but merge requires the exact-head acceptance per AGENTS.
