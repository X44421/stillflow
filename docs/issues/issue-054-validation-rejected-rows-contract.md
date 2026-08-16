# Issue #54 Implementation Contract: validation, rejected rows, and exact deduplication (E4-C0)

> Status: Frozen for architecture review (not approved)
> Revision: C0
> Risk: High
> Issue: #54 (contract), #55 (expected draft PR)
> Parent: Issue #46 revision R3, merged at
> `32f1c53d9903f66aeaca1c2676c0b81abfb2a702` in PR #47
> Authorized base: `main@85502cbebb1fab461fe42d30fe019ad20613aa7c`
> Branch: `agent/issue-054-validation-rejected-rows-contract`
> Last updated: 2026-08-16
> Review: PR #55 is expected to remain draft. Architecture approval binds
> exactly one new commit SHA of this file. E4 runtime implementation starts
> only after that approval and only from the then-latest accepted `main`.

This document freezes the public contract and objective acceptance matrix for
Engine E4-C0: `Rule::Validate`, rejected rows, and exact `Rule::Deduplicate`.
It does **not** authorize any Rust runtime code, dependency change, lockfile
change, CI change, or frontend change.

## 1. Objective

Freeze a single deterministic cleaning-publication path that:

1. assigns a connector-partition-stable, zero-based `source_row_ordinal`;
2. executes `Rule::Validate` with frozen true / false / null semantics;
3. executes `Rule::Deduplicate` with exact, stable keep-first semantics;
4. publishes accepted and rejected rows atomically, with validation
   diagnostics attached to every quality event;
5. keeps deduplication state external, exact, recoverable, and bounded;
6. reuses the E2 preflight, typing, lowering, chunker, Arrow interchange,
   error taxonomy, and sanitized-error semantics from Issue #46.

E4-C0 must not create a second cleaning language, a second executor, or a
separate Preview interpretation of Validate/Deduplicate.

## 2. Source policy and branch discipline

- The authorized base is `main@85502cbebb1fab461fe42d30fe019ad20613aa7c`.
- This branch is created from that exact commit. It must not merge, rebase
  onto, or cherry-pick from PR #53, PR #49, or any historical branch.
- PR #53 is a read-only reference for the current E3 public surface. E4
  runtime must wait until PR #53 merges and must then rebuild from the
  latest accepted `main`; this contract does not authorize changing E3 or
  workflow/architecture files now.
- This delivery is docs-only. The only authorized file is
  `docs/issues/issue-054-validation-rejected-rows-contract.md`.
- No Rust source, `Cargo.toml`, `Cargo.lock`, frontend file, CI workflow,
  architecture file, or Issue #46 / #48 / #50 contract may be modified.
- If this contract needs a public type or field outside the current PR #53
  public API, it is named `Proposed` in section 7.3 and is not implemented
  in this PR.

## 3. Risk and compatibility

This work is `risk:high` because it defines a second snapshot publication,
a rejected-row data plane, exact key equality, external deduplication state,
and an atomic two-snapshot commit path used by all later E5 job/API work.

Compatibility decision:

- The existing `ExecutionRequest`, `ExecutionIdentities`, `ExecutionEngine`,
  `PreviewRequest`, `PreviewResult`, `BatchEnvelope`, `LogicalSchema`,
  `ColumnId`, `Rule`, `Expr`, and `SnapshotDraft` version 1 contracts are
  not changed by this document.
- `ExecutionEngine::materialize` keeps the frozen E2 behavior and keeps
  returning `UnsupportedRule` for `Rule::Validate` / `Rule::Deduplicate`.
- E4 adds a new `materialize_cleaning` entry point and new result types in
  section 7.3. Those are `Proposed` until the later E4 runtime PR and must be
  reconciled against the then-current E3 public API after PR #53 merges.
- Existing storage single-snapshot `begin_snapshot` / `commit` remain valid.
  E4 proposes an additive pair API in section 7.3; it does not redefine the
  old API.
- No compatibility shim is provided for Join/Union execution, DuckDB SQL,
  SQLx, arbitrary engine code, approximate deduplication, or hash-only
  deduplication.

## 4. Scope

In scope for this contract:

- Frozen `source_row_ordinal` assignment and row routing.
- Frozen Validate semantics, including multi-rule collection and caps.
- Frozen Deduplicate key equality and canonical key bytes.
- Bounded, external, collision-safe SQLite dedup index and its lifecycle.
- Accepted snapshot plus rejected rows snapshot data model.
- Diagnostics association and original-value preservation.
- Atomic accepted/rejected publication and failure cleanup.
- Preview relationship (section 9).
- Security boundary for raw values and validation messages.
- Numeric resource ceilings and the V01–V22 acceptance matrix.

Explicit non-goals:

- HTTP routes, Axum handlers, job tables, or E5 status machines.
- Frontend layout, components, CSS, tokens, or generated types.
- DuckDB, SQLx, ConnectorX, or SQL Connector #9.
- `Join` / `Union` execution.
- A second Validate/Deduplicate Preview implementation.
- AI execution, Python, SQL strings, or arbitrary Polars/Python/SQL programs.
- Sampling, reservoir, random selection, or approximate duplicate detection.
- Dependabot updates mixed into this branch.

## 5. Row identity and routing

### 5.1 `source_row_ordinal`

`source_row_ordinal` is a `u64`, zero-based, assigned exactly once per
logical source row in Scan output order.

1. The assignment domain is the ordered connector stream after the Scan
   projection decision and **before** `Scan.predicate` evaluation.
   Concretely: if push projection is false, ordinals are assigned to the
   projected connector rows; if push projection is true, ordinals are
   assigned to connector rows as delivered.
2. Ordinal `0` is the first projected row of the first connector envelope.
   Every subsequent projected row gets `previous + 1`, using checked
   addition. Envelope sequence contiguity is still enforced by the E2
   stream wrapper.
3. Ordinals are assigned in logical row order, never from envelope
   boundaries, connector partition numbers, physical file offsets, or
   `BatchEnvelope.sequence()`. Changing only connector batch partitioning
   must not change any row's ordinal.
4. `Scan.predicate`, `Filter`, and `FilterRows` drop rows without reusing
   or renumbering ordinals. Ordinals of later surviving rows therefore may
   contain gaps. A gap is stable and intentional; consumers must not infer
   row counts from the maximum ordinal.
5. At most `MAX_SNAPSHOT_ROWS` rows may enter the cleaning path. Checked
   ordinal overflow or exceeding `MAX_SNAPSHOT_ROWS` is
   `EngineError::BoundExceeded`.
6. The ordinal is copied into every rejected/diagnostic row so diagnostics
   correlate to the original projected source row without embedding cell
   values in control-plane metadata.

### 5.2 Validate true / false / null semantics

For `Rule::Validate { predicate, severity, message }`, preflight type-checks
the predicate against the current working schema exactly like any E2
Boolean expression. The predicate must infer to `LogicalType::Boolean`;
otherwise preflight returns `EngineError::TypeError`.

At runtime, for each input row:

| Predicate result | `severity = Warning` | `severity = Error` |
| --- | --- | --- |
| Boolean `true` | pass; no diagnostic event | pass; no diagnostic event |
| Boolean `false` | validation failure; keep row in accepted stream; emit `validation_warning` event | validation failure; route row to rejected data plane; emit `validation_error` event; stop evaluating that row |
| `null` | same as `false` | same as `false` |

`null` is therefore always a validation failure, never an implicit pass.

Multi-rule collection rule:

- Rules are evaluated in listed order inside each `ApplyRules` node and in
  plan order across nodes.
- Warning failures are collected and the row continues to later rules.
- The first Error failure terminates evaluation of that row: the row is
  routed to rejected data plane and no later rule, operator, or node sees
  it. Warnings already collected for that row are retained.
- A row removed by Error is never re-admitted and never promoted by a later
  deduplication rule.
- Hard cap: at most `MAX_QUALITY_EVENTS_PER_ROW` quality events may be
  emitted for one source row across the whole run. Exceeding the cap is
  `EngineError::BoundExceeded`; no snapshot is published.

Validation message rules:

- `message` must remain non-empty after trim, must be UTF-8, must be at
  most `MAX_VALIDATION_MESSAGE_BYTES` bytes after trim, and must pass the
  existing `ensure_no_secret_fields` check used by `Rule::validate`.
- The exact plan-authored message bytes are stored in the rejected data
  plane; the message is never copied into `EngineError`, logs, events, or a
  sanitized summary.

### 5.3 Routing summary

| Event | Accepted stream | Rejected data plane |
| --- | --- | --- |
| Validate predicate `true` | row continues | none |
| Validate predicate `false` / null, Warning | row continues | one `validation_warning` diagnostic row |
| Validate predicate `false` / null, Error | row removed | one `validation_error` diagnostic row |
| Deduplicate first occurrence | row continues | none |
| Deduplicate later occurrence | row removed | one `duplicate` diagnostic row |

Rows dropped by `Scan.predicate`, `Filter`, or `FilterRows` are silently
dropped exactly as in E2. They receive ordinals but produce no rejected row
and no diagnostic.

## 6. Deduplicate exact semantics

### 6.1 Rule contract

`Rule::Deduplicate { keys }`:

- `keys` is the ordered `Vec<ColumnId>` already validated by
  `stillflow-plan` (non-empty, no duplicate ids).
- E4 preflight adds: `keys.len() <= MAX_DEDUP_KEY_COLUMNS`; every key id
  exists in the current working schema at that rule; the current working
  type of every key is one of the version-1 `LogicalType` variants listed
  in section 6.3; and the canonical encoded composite key length can never
  exceed `MAX_DEDUP_KEY_BYTES` (section 6.4). A violation is
  `EngineError::UnknownColumn`, `EngineError::TypeError`, or
  `EngineError::BoundExceeded` respectively.
- Deduplicate does not change the working schema, row values, or row order.
- Multiple `Deduplicate` rules are independent namespaces keyed by
  `(node_id, rule_ordinal)`. The same physical row may be first in one rule
  and duplicate in another; a later duplicate emits its own terminal event
  and stops that row.
- Keep-first is decided solely by ascending `source_row_ordinal`. There is
  no tie: one projected source row has exactly one ordinal. If the first
  row of a key class is later rejected by a subsequent Validate Error, the
  class has no accepted row; later duplicates are **not** promoted.

### 6.2 Key equality

Equality is exact, typed tuple equality over the ordered key columns. No
hash digest, no Unicode normalization, no collation, no trimming, no
approximate or phonetic comparison is permitted.

| Logical value | Frozen equality rule |
| --- | --- |
| `Null` | All nulls are equal to each other. Null is never equal to a non-null value. |
| `Boolean` | `false` and `true` exact. |
| `Int8/16/32/64`, `UInt8/16/32/64` | Exact numeric value. Typed components: values of different component types are different even when numerically equal. |
| `Float32` / `Float64` | All NaN values are equal to each other regardless of sign or payload. `-0.0` equals `+0.0`. Finite values compare by exact IEEE value. No epsilon. |
| `Utf8` | Exact UTF-8 byte sequence. Empty string is distinct from null. No normalization, case folding, or collation. |
| `Binary` | Exact byte sequence. Empty binary is distinct from null. |
| `Date32` | Exact days-since-epoch value; null equal to null. |
| `Timestamp { unit, timezone }` | Equal only when the component type is identical and the integer epoch count in that unit is identical. No string parse, no instant conversion across units, no timezone lookup. Null equal to null. |
| `List(element)` | Equal only when element types are identical, lengths are equal, and every element is recursively equal. |
| `Struct(fields)` | Equal only when the struct type is identical and field values are recursively equal in declared field order. |

Within one `Deduplicate` rule each key id has one fixed working type, so
cross-type key coercion never occurs. The typed-tuple rule remains explicit
so a future engine cannot silently coerce key components.

### 6.3 Canonical key bytes

The SQLite index stores the full canonical key bytes. The encoding is
injective for each value and schema; it is not a hash and is not used as a
summary.

`encode_component(declared_type, value)`:

1. If `value` is null: emit the single byte `0x00`.
2. Otherwise emit one type-tag byte followed by the type payload:

| Tag | Type | Payload |
| --- | --- | --- |
| `0x01` | `Boolean` | one byte, `0x00` or `0x01` |
| `0x02` | `Int8` | one byte, little-endian two's complement |
| `0x03` | `Int16` | two bytes, little-endian two's complement |
| `0x04` | `Int32` | four bytes, little-endian two's complement |
| `0x05` | `Int64` | eight bytes, little-endian two's complement |
| `0x06` | `UInt8` | one byte, little-endian |
| `0x07` | `UInt16` | two bytes, little-endian |
| `0x08` | `UInt32` | four bytes, little-endian |
| `0x09` | `UInt64` | eight bytes, little-endian |
| `0x0A` | `Float32` | four canonical IEEE bits, little-endian |
| `0x0B` | `Float64` | eight canonical IEEE bits, little-endian |
| `0x0C` | `Utf8` | `u32` little-endian byte length, then exact UTF-8 bytes |
| `0x0D` | `Binary` | `u32` little-endian byte length, then exact bytes |
| `0x0E` | `Date32` | four bytes, little-endian `i32` days since epoch |
| `0x0F` | `Timestamp { unit, timezone }` | one unit-tag byte (`0=Second`, `1=Millisecond`, `2=Microsecond`, `3=Nanosecond`), `u32` little-endian timezone UTF-8 byte length, that many timezone bytes (`0` for `None`), then eight bytes little-endian `i64` epoch count |
| `0x10` | `List(element)` | `u32` little-endian element count, then each element recursively encoded with `element` as its declared type |
| `0x11` | `Struct(fields)` | each field value recursively encoded in declared field order |

Float canonicalization before encoding:

- any NaN becomes the single canonical quiet-NaN bits
  `0x7FC00000` (Float32) or `0x7FF8000000000000` (Float64);
- any zero becomes positive zero bits;
- finite non-zero values keep their exact IEEE bits.

`canonical_key_bytes` is the concatenation, in `keys` order, of
`encode_component(current_working_type(key_id), key_value)`. If the encoded
length exceeds `MAX_DEDUP_KEY_BYTES`, the run fails with
`EngineError::BoundExceeded` before the SQLite insert.

### 6.4 SQLite temporary index

`stillflow-storage` proposes an owned `DedupIndex` handle (section 7.3).
The handle is the only deduplication state; engine code must not contain a
`HashSet`, `HashMap`, Bloom filter, digest cache, or any other in-memory
duplicate index.

| Property | Frozen value |
| --- | --- |
| Storage format | One SQLite temporary database file, owned by `stillflow-storage` |
| File identity | `dedup_{owner_snapshot_id}.sqlite` under the managed temp root; `owner_snapshot_id` is the caller-injected accepted snapshot id |
| Table | `CREATE TABLE dedup_index (node_id BLOB NOT NULL, rule_ordinal INTEGER NOT NULL, key_bytes BLOB NOT NULL, PRIMARY KEY (node_id, rule_ordinal, key_bytes)) WITHOUT ROWID;` |
| `node_id` encoding | exact 16-byte `Uuid` bytes of `PlanNodeId` |
| `rule_ordinal` | zero-based rule index within the containing `ApplyRules` node |
| Insert decision | `INSERT INTO dedup_index (...) VALUES (...) ON CONFLICT DO NOTHING`; `changes() == 1` means first occurrence, `0` means duplicate. The engine never computes a hash to decide. |
| Cache memory | `PRAGMA cache_size = -512` (512 KiB maximum), counted in operator state |
| Journal | `PRAGMA journal_mode = DELETE`; the file is disposable and is not the publication transaction |
| Disk ceiling | `MAX_DEDUP_INDEX_DISK_BYTES` = 8 GiB per run |
| Index rows | At most `MAX_SNAPSHOT_ROWS` inserts per `(node_id, rule_ordinal)` namespace and across the run |
| Collision safety | SQLite BLOB primary-key equality over full canonical bytes; no hash-only or approximate path |
| Recovery | The index is per-run and disposable. `open_dedup_index` deletes any stale file for the same `owner_snapshot_id` before creating a new one. `DedupIndex::Drop` deletes the file and its journal residue. Storage recovery also removes stale files under the temp root. A crashed run publishes nothing and the next retry starts with an empty index. |

The dedup index is a correctness accelerator, not a persisted dataset. Its
loss can never make an already-committed snapshot appear, disappear, or
change. SQLite cache memory, file size, and row count are the only dedup
resources.

### 6.5 Keep-first algorithm

For each `Deduplicate` rule and each row in ascending `source_row_ordinal`:

1. compute `canonical_key_bytes` from the working row at that rule;
2. insert `(node_id, rule_ordinal, key_bytes)` into `DedupIndex`;
3. if the insert is new, keep the row in the accepted stream;
4. if the insert is a conflict, emit one `duplicate` event and remove the
   row from all later processing.

The index persists across execution chunks, connector envelopes, and
connector partitions within one run. Therefore keep-first is global, not
per-batch or per-partition.

## 7. Accepted / Rejected data model

### 7.1 Accepted snapshot

The accepted output is the existing E2 `SnapshotDraft` / `SnapshotWriter` /
`SnapshotManifest` path unchanged:

- schema is the Materialize working schema after all rules;
- row order is ascending `source_row_ordinal` among surviving rows;
- envelope boundaries use the same E2 canonical rebatcher and `batch_size`;
- warning rows remain accepted;
- Error and duplicate rows are absent;
- empty accepted output is a valid zero-row, zero-partition snapshot.

### 7.2 Rejected rows snapshot

E4 freezes a **second snapshot**, not a loose sidecar file set. The rejected
snapshot uses the same `SnapshotStore`, same Parquet partition model, same
manifest model, and same read path as accepted snapshots.

The rejected snapshot schema is:

```text
[all fields of prepared.scan_output in their exact order]
+ __stillflow_source_row_ordinal : UInt64, nullable = false
+ __stillflow_node_id            : Utf8,   nullable = false
+ __stillflow_rule_ordinal       : UInt32, nullable = false
+ __stillflow_event_kind         : Utf8,   nullable = false
+ __stillflow_severity           : Utf8,   nullable = true
+ __stillflow_message            : Utf8,   nullable = true
```

Field contracts:

| Field | Value |
| --- | --- |
| Original fields | Exact `prepared.scan_output` field order, `ColumnId`, name, `LogicalType`, nullability, and metadata. Values are copied from the projected connector row identified by `source_row_ordinal`. |
| `__stillflow_source_row_ordinal` | The ordinal assigned in section 5.1 |
| `__stillflow_node_id` | `PlanNodeId.as_uuid().to_string()` canonical lowercase hyphenated form |
| `__stillflow_rule_ordinal` | Zero-based index in the containing `ApplyRules.rules` vector |
| `__stillflow_event_kind` | Exactly one of `"validation_warning"`, `"validation_error"`, or `"duplicate"` |
| `__stillflow_severity` | `"warning"` for validation_warning; `"error"` for validation_error; null for duplicate |
| `__stillflow_message` | Exact plan-authored `Rule::Validate.message` for validation events; null for duplicate |

Reserved control `ColumnId` values are fixed contract constants defined in
section 7.4. They are compile-time constants, never generated by the engine.

Schema construction rules:

- Start from a clone of `prepared.scan_output` (including its metadata),
  append the six control fields in the order above, and validate through
  `LogicalSchema::validate`.
- Total field count must be `<= MAX_SCHEMA_FIELDS`; a source schema with
  more than `MAX_SCHEMA_FIELDS - 6` fields is preflight `InvalidPlan`.
- The source schema must not already contain any reserved control name or
  reserved control `ColumnId`; otherwise preflight `InvalidPlan`.
- The rejected snapshot source asset id is the same bound `SourceAsset.id`.
- The rejected snapshot `SnapshotDraft` uses caller-injected
  `rejected_snapshot_id`, `rejected_dataset_id`, `session_id`, `created_at`,
  lineage, and rejected quality score (section 8). `started_at` is the same
  caller-injected value as the accepted writer.

Event row semantics:

- Every quality event is one rejected-snapshot row.
- Multiple events for the same source row copy the original projected
  source row once per event. This is intentional and bounded by
  `MAX_QUALITY_EVENTS_PER_ROW` and `MAX_QUALITY_EVENTS_PER_RUN`.
- Event order is deterministic: ascending `source_row_ordinal`, then plan
  node order along the linear path, then ascending `rule_ordinal`.
- Warning events describe rows that are still accepted; `event_kind`
  distinguishes them from actual rejections. A reader filters
  `event_kind != 'validation_warning'` to obtain only rows removed from the
  accepted stream.
- Duplicate events have null severity and null message by definition.
- The rejected snapshot is a normal immutable `SnapshotManifest`; its
  `SnapshotStats.row_count` equals total quality event count.

Original-value preservation decision:

- The original value copied into a rejected row is the projected Scan
  output row **before** `Scan.predicate` and before any `ApplyRules`.
  Columns omitted by `Scan.projection` were never read (or already dropped
  by the connector) and are intentionally not recovered.
- Values produced by earlier Derive/Cast/Replace rules are not copied into
  rejected rows. The accepted row is removed on Error/duplicate, so those
  derived values exist only transiently inside the bounded engine payloads
  and are reconstructible by replaying the deterministic plan over the
  preserved source row.
- This fixed-schema choice keeps one rejected schema for all events and
  preserves source `Schema`, `ColumnId`, nullability, and physical Arrow
  values exactly.

### 7.3 Proposed public API

Names may be organized into modules. Semantics, field order, and limits
must match this section. This docs PR adds no Rust code.

```rust
pub const E4_MAX_LIVE_COLUMNAR_PAYLOADS: u8 = 4;
pub const E4_MAX_ENGINE_PEAK_BYTES: usize =
    (E4_MAX_LIVE_COLUMNAR_PAYLOADS as usize) * MAX_BATCH_BYTES
        + MAX_OPERATOR_STATE_BYTES; // 4 * 64 MiB + 5 MiB = 261 MiB

pub const MAX_DEDUP_KEY_COLUMNS: usize = 64;
pub const MAX_DEDUP_KEY_BYTES: usize = 64 * 1024;
pub const MAX_DEDUP_INDEX_CACHE_BYTES: usize = 512 * 1024;
pub const MAX_DEDUP_INDEX_DISK_BYTES: u64 = 8 * 1024 * 1024 * 1024;
pub const MAX_QUALITY_EVENTS_PER_ROW: usize = MAX_RULES_PER_NODE; // 256
pub const MAX_QUALITY_EVENTS_PER_RUN: u64 = MAX_SNAPSHOT_ROWS as u64;
pub const MAX_VALIDATION_MESSAGE_BYTES: usize = 1_024;
pub const E4_MAX_COMPILED_PLAN_BYTES: usize = 3 * 1024 * 1024;
pub const E4_MAX_ROUTING_STATE_BYTES: usize = 512 * 1024;

pub struct CleaningExecutionIdentities {
    pub snapshot_id: Uuid,
    pub dataset_id: Uuid,
    pub rejected_snapshot_id: Uuid,
    pub rejected_dataset_id: Uuid,
    pub session_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub lineage: BTreeSet<Uuid>,
    pub quality_score: Option<u8>,
    pub rejected_quality_score: Option<u8>,
}

pub struct CleaningRequest<'a> {
    pub plan: LogicalPlan,
    pub connection: SourceConnection,
    pub asset: SourceAsset,
    pub schema_override: Option<LogicalSchema>,
    pub identities: CleaningExecutionIdentities,
    pub context: RequestContext,
    pub batch_size: usize,
    pub store: &'a SnapshotStore,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RejectedRowsStats {
    pub validation_warning_count: u64,
    pub validation_error_count: u64,
    pub duplicate_count: u64,
}

impl RejectedRowsStats {
    pub const fn total_event_count(&self) -> u64; // saturating sum
}

pub struct CleaningPublication {
    pub accepted: SnapshotManifest,
    pub rejected: SnapshotManifest,
    pub rejected_stats: RejectedRowsStats,
}

impl ExecutionEngine {
    pub async fn materialize_cleaning(
        &self,
        request: CleaningRequest<'_>,
    ) -> Result<CleaningPublication, EngineError>;
}

impl SnapshotStore {
    pub fn begin_snapshot_pair(
        &self,
        accepted: SnapshotDraft,
        rejected: SnapshotDraft,
        started_at: DateTime<Utc>,
    ) -> Result<(SnapshotWriter, SnapshotWriter), StorageError>;

    pub fn commit_snapshot_pair(
        accepted: SnapshotWriter,
        rejected: SnapshotWriter,
    ) -> Result<(SnapshotManifest, SnapshotManifest), StorageError>;

    pub fn open_dedup_index(
        &self,
        owner_snapshot_id: Uuid,
    ) -> Result<DedupIndex, StorageError>;
}

impl DedupIndex {
    pub fn insert_first(
        &self,
        node_id: Uuid,
        rule_ordinal: u32,
        key_bytes: &[u8],
    ) -> Result<bool, StorageError>; // true = first occurrence
}
```

Compatibility note: `CleaningRequest` / `CleaningPublication` /
`CleaningExecutionIdentities` are new public types outside the current
E2/E3 surface and are `Proposed`. If PR #53 has changed
`PreviewRequest` / `PreviewResult` by the time E4 runtime starts, E4 must
rebase on the then-current `main` and reconcile only section 9; it must not
silently alter the E3 fields.

### 7.4 Reserved rejected-schema identities

These `ColumnId` constants are fixed for schema version 1 E4 output. They
are written in the later runtime crate, not generated:

```rust
pub const REJECTED_SOURCE_ROW_ORDINAL_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0001));
pub const REJECTED_NODE_ID_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0002));
pub const REJECTED_RULE_ORDINAL_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0003));
pub const REJECTED_EVENT_KIND_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0004));
pub const REJECTED_SEVERITY_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0005));
pub const REJECTED_MESSAGE_COLUMN_ID: ColumnId =
    ColumnId::from_uuid(Uuid::from_u128(0xE4C0_0000_0000_4000_8000_0000_0000_0006));
```

## 8. Atomic publication and identity injection

### 8.1 Two-snapshot transaction

`materialize_cleaning` always publishes two snapshots, even when the plan
contains no Validate/Deduplicate rule; in that case the rejected snapshot is
a valid empty snapshot and `RejectedRowsStats` is all zero. This keeps one
uniform atomic contract for callers.

Publication sequence:

1. Apply default deadline and acquire the existing E2 run-gate permit
   (`try_acquire`, never await).
2. Run shared E2 preflight with E4 target disabled and the E4 rule checks
   from sections 5 and 6.
3. Validate all injected identities.
4. `SnapshotStore::begin_snapshot_pair(accepted_draft, rejected_draft,
   identities.started_at)`. This acquires two storage publisher permits
   atomically; if two are not available it returns `StorageError::Busy`
   and the engine publishes nothing.
5. `SnapshotStore::open_dedup_index(identities.snapshot_id)`.
6. Open the connector stream exactly once and process both outputs through
   two canonical rebatchers.
7. Finish accepted and rejected streams, then
   `SnapshotStore::commit_snapshot_pair`.
8. Drop the dedup index and return `CleaningPublication`.

`commit_snapshot_pair` must be one storage transaction at the visibility
boundary: both manifest rows become visible together, or neither does.
Readers can never observe accepted without rejected, or rejected without
accepted, for one `CleaningPublication`.

### 8.2 Failure and cancellation

Cancellation and deadline are observed:

1. before preflight inspect I/O;
2. before opening `read_batches`;
3. on every connector stream poll;
4. before lowering each connector envelope;
5. before every accepted `SnapshotWriter::append`;
6. before every rejected `SnapshotWriter::append`;
7. before `commit_snapshot_pair`.

A cancelled or timed-out run returns `Cancelled` / `Timeout`, publishes no
snapshot, and leaves no partial partition or temp index. If a storage error
occurs during pair commit, storage must roll back both writers and remove
any installed partition directories; the engine returns `EngineError::Storage`
and no manifest is visible.

Dropping a not-yet-committed writer pair must abort both publications and
remove both staging directories. `DedupIndex::Drop` must remove the temp
database and any `-journal` residue. Tests must assert that both
`load_manifest` calls fail after every failure injection.

### 8.3 Identity and timestamp injection

| Field | Source |
| --- | --- |
| Accepted `id` | `identities.snapshot_id` |
| Accepted `dataset_id` | `identities.dataset_id` |
| Rejected `id` | `identities.rejected_snapshot_id` |
| Rejected `dataset_id` | `identities.rejected_dataset_id` |
| `session_id` | `identities.session_id` for both drafts |
| `source_asset_id` | bound `SourceAsset.id` for both drafts |
| `lineage` | `identities.lineage` for both drafts |
| Accepted `quality_score` | `identities.quality_score` |
| Rejected `quality_score` | `identities.rejected_quality_score` |
| `created_at` | `identities.created_at` for both drafts |
| `started_at` (`begin_snapshot_pair`) | `identities.started_at` for both |
| Dedup temp file identity | `identities.snapshot_id` only |

The engine must not call `Uuid::new_v4` or `Utc::now` for any published
identity or timestamp. Wall-clock `Instant` remains allowed only for
deadline observation. Nil ids, nil lineage ids, and quality scores above
100 are rejected before pair begin.

## 9. Preview relationship

E4-C0 does **not** extend `PreviewResult`, `PreviewRequest`, or the E3
preview execution path.

- The E3 preview contract in Issue #50 (and its current PR #53 revision)
  remains the only Preview semantics. Plans containing
  `Rule::Validate` / `Rule::Deduplicate` continue to return
  `UnsupportedRule` from the preview path until a future E4-P contract
  explicitly changes that.
- E4 runtime must not implement a second Validate/Deduplicate Preview
  path, must not return partial accepted/rejected preview payloads, and
  must not duplicate E4 routing logic for Preview.
- No public Preview field changes are authorized here. If a future Preview
  revision needs rejected/diagnostic preview fields, it must be a separate
  approved contract and must reuse the exact row identity, Validate
  true/false/null, canonical key equality, and routing rules frozen in this
  document.

## 10. Memory model and bounded state

### 10.1 E4 cleaning memory law

The E2 `materialize` law (three columnar payloads, 197 MiB) is unchanged.
The new `materialize_cleaning` path uses four live columnar payloads:

```text
connector envelope            <= MAX_BATCH_BYTES          (64 MiB)
complete Polars working set   <= MAX_BATCH_BYTES          (64 MiB)
accepted canonical remainder  <= MAX_BATCH_BYTES          (64 MiB)
rejected canonical remainder  <= MAX_BATCH_BYTES          (64 MiB)
bounded non-columnar state    <= MAX_OPERATOR_STATE_BYTES (5 MiB)
E4 peak                       = 4 * 64 MiB + 5 MiB = 261 MiB
```

Both remainders use the E2 move/freeze rule: flushing a remainder into an
output envelope moves the allocation; there is never a fifth
`MAX_BATCH_BYTES`-class copy. Output envelopes are dropped after
`SnapshotWriter::append` returns.

### 10.2 Operator-state budget

`MAX_OPERATOR_STATE_BYTES` remains 5 MiB. The E4 path allocates within it:

| Component | Ceiling |
| --- | --- |
| E4 compiled plan | `E4_MAX_COMPILED_PLAN_BYTES` = 3 MiB |
| FFI scratch | `MAX_FFI_SCRATCH_BYTES` = 1 MiB |
| SQLite dedup cache | `MAX_DEDUP_INDEX_CACHE_BYTES` = 512 KiB |
| Routing metadata (ordinals, masks, counters, control buffers) | `E4_MAX_ROUTING_STATE_BYTES` = 512 KiB |

The law is the actual sum, not the four ceilings added:
`actual_compiled + actual_ffi + actual_dedup_cache + actual_routing <= 5 MiB`.
If a plan would exceed `E4_MAX_COMPILED_PLAN_BYTES`, or the measured sum
would exceed 5 MiB, preflight/runtime returns `BoundExceeded` before pair
commit. The dedup index file itself is disk state and is not counted as
engine memory beyond its configured 512 KiB SQLite page cache.

### 10.3 No unbounded in-memory dedup

- `HashSet`, `HashMap`, Bloom filters, sketches, digest-only caches, and
  per-batch duplicate maps are forbidden for the keep-first decision.
- Source grep alone is not sufficient. Tests must instrument the engine
  allocator and SQLite cache and prove that dedup state does not grow with
  distinct keys beyond the configured cache and the routing-state ceiling.
- Approximate deduplication (locality-sensitive hashing, n-grams, fuzzy
  matching) is forbidden.

## 11. Security boundary

### 11.1 Raw values

Raw source cell values and derived failing values may exist only in:

- the bounded connector/Polars/remainder payloads during execution;
- accepted Parquet partitions (for accepted rows);
- rejected Parquet partitions (for quality events);
- the disposable SQLite dedup index as canonical key bytes.

They must never appear in:

- `EngineError` `Display` / `Debug`;
- `sanitized_summary().message()`;
- logs, tracing fields, or event metadata;
- snapshot manifests, SQLite control-plane tables outside the disposable
  dedup index, or generated API summaries.

The sanitization sentinel remains the UTF-8 string
`STILLFLOW_SENTINEL_CELL_VALUE_9f3c2a`. It must appear as a cell value in
failing fixtures and must not appear in any `EngineError` surface or
serialized sanitized summary/event metadata.

### 11.2 Validation message

- The plan-authored validation message is data-plane content, not an
  execution log. It is stored only in the rejected snapshot `message`
  column.
- Preflight enforces `1..=MAX_VALIDATION_MESSAGE_BYTES` UTF-8 bytes after
  trim and existing secret-field rejection.
- The engine must not put message text in `EngineError`, Debug, logs, or
  future E5 events. Future E5 events may carry counts, node id, rule
  ordinal, severity, and `source_row_ordinal` ranges only.
- `node_id`, `rule_ordinal`, `severity`, `event_kind`, row ordinal, batch
  sequence, and resource counts are safe correlation metadata and may
  appear in sanitized errors.

### 11.3 Error surface

E4 uses the existing `EngineError` variants only:

- bad Validate/Dedup shape or reserved-name collision: `InvalidPlan`;
- unknown key column: `UnknownColumn`;
- non-Boolean Validate predicate: `TypeError`;
- key/row/message/disk/memory cap exceeded: `BoundExceeded`;
- SQLite index failures: `Storage(inner)`;
- all other E2 categories unchanged.

`EngineError` remains non-`Serialize`; only `SanitizedErrorSummary` crosses
a public boundary.

## 12. Resource ceilings

| Resource | Ceiling | Source |
| --- | --- | --- |
| E4 live columnar payloads | 4 | this contract |
| E4 peak engine bytes | 261 MiB | `4 * 64 MiB + 5 MiB` |
| Operator state | 5 MiB | E2, shared budget in section 10.2 |
| E4 compiled plan | 3 MiB | this contract |
| Dedup SQLite cache | 512 KiB | this contract |
| Routing metadata | 512 KiB | this contract |
| Dedup key columns per rule | 64 | this contract |
| Encoded composite dedup key | 64 KiB | this contract |
| Dedup index disk | 8 GiB | this contract |
| Dedup index rows per run | `MAX_SNAPSHOT_ROWS` (1,000,000,000) | storage |
| Quality events per source row | 256 | this contract |
| Quality events per run | 1,000,000,000 | this contract |
| Validation message | 1,024 UTF-8 bytes | this contract |
| Source row ordinal domain | `0..MAX_SNAPSHOT_ROWS` | this contract |
| Rejected snapshot rows / bytes / partitions | `MAX_SNAPSHOT_ROWS` / 1 TiB / `MAX_SNAPSHOT_PARTITIONS` | storage |
| Accepted snapshot limits | unchanged E2 | storage |
| Input envelope rows / bytes | 65,536 / 64 MiB | core |
| `batch_size` | `1..=65_536` | E2 / `ReadRequest` |
| Plan nodes / rules per node / expr nodes / depth | 64 / 256 / 1,024 / 64 | E2 |
| Engine concurrent runs | `MAX_ENGINE_CONCURRENT_RUNS` = 4 | E2 run gate |
| Storage publishers | 8; pair begin acquires two atomically | storage |
| Default / maximum deadline | 15 min / 30 min | E2 |

Exceeding any ceiling is a typed error before visible publication. No TBD
value is permitted.

## 13. Determinism, partition invariance, and retry

A `materialize_cleaning` run is deterministic when all of the following
hold:

1. Identical authorized source rows and order, identical validated plan,
   identical `batch_size`, and identical injected identities produce:
   - identical accepted logical rows, schema, and envelope boundaries;
   - identical rejected logical rows, schema, and envelope boundaries;
   - identical `RejectedRowsStats`;
   - identical accepted/rejected `SnapshotStats`.
2. Changing only connector batch partitioning must not change which rows
   are accepted or rejected, their order, their schemas, the stats, or the
   envelope boundaries of either snapshot. `source_row_ordinal` follows
   logical row order, never physical partitions.
3. Deduplicate decisions depend only on `(node_id, rule_ordinal,
   canonical_key_bytes, source_row_ordinal)` and SQLite BLOB equality, not
   on hash iteration, HashMap order, locale, clock, process id, or Polars
   approximate duplicate routines.
4. Injected `created_at` / `started_at` are the only timestamps written
   into storage calls. `Instant` is used only for deadline observation.
5. Plan canonical bytes and fingerprints remain `stillflow-plan` values;
   E4 must not invent a second fingerprint.

Retry law:

- A retry after an aborted/failed attempt uses an empty dedup index
  (`open_dedup_index` deletes the stale file). With identical inputs and
  identities it produces identical rows, order, stats, and partition
  boundaries. Manifest timestamp fields are identical because they are
  caller-injected.
- If a failed publication left storage residue because a process died, the
  caller must run existing storage recovery before reusing the same
  snapshot ids. A retry without recovery may receive the existing storage
  uniqueness error; it must never silently merge with stale index entries.
- No committed snapshot is modified or recomputed by retry.

## 14. Acceptance matrix

The sanitization sentinel is
`STILLFLOW_SENTINEL_CELL_VALUE_9f3c2a`.

| ID | Criterion | Automated evidence |
| --- | --- | --- |
| V01 | Validate true passes; false Warning keeps accepted row; false Error rejects; null behaves as failure | One fixture per severity with true/false/null predicate columns; accepted row count, rejected event kind/severity/message, and stats match. Null predicate is never a pass. |
| V02 | All rows pass, all rows rejected, and empty source | Accepted/rejected row counts and zero-row snapshot invariants match; empty source yields two valid empty snapshots with `RejectedRowsStats` all zero. |
| V03 | Cross-batch global dedup keep-first | Distinct keys span at least three execution chunks/envelopes; only the lowest `source_row_ordinal` per key is accepted; later rows are duplicate events. |
| V04 | Connector partition invariance | Two partitionings of the same ordered rows produce identical accepted and rejected row sequences, schemas, stats, and fixed-`batch_size` envelope boundaries. |
| V05 | Null, NaN, `-0.0`/`+0.0` key equality | Null duplicates null; all NaN bit patterns group together; `-0.0` and `+0.0` are duplicates; finite distinct floats remain distinct. |
| V06 | Multiple Validate hits | Warning followed by Error on the same row emits both events in rule order; first Error terminates later rules; cap 256 is enforced with `BoundExceeded` and no publication. |
| V07 | Warning rows remain accepted; Error rows are absent from accepted | Compare accepted values against expected surviving rows; warning event still present in accepted snapshot. |
| V08 | Deduplicate duplicate rows enter rejected data plane, not silent deletion | Every duplicate has a `duplicate` event with original row copy, null severity/message, node id, rule ordinal, and ordinal. |
| V09 | Cancellation and deadline publish nothing | Inject cancel/deadline at each observation point in section 8.2; both `load_manifest` calls fail; temp index file is absent; no staging residue. |
| V10 | Dual-output atomicity | Inject storage failure during pair commit (manifest insert/move); neither manifest is visible and both staging directories are cleaned. Readers never observe one snapshot without the other. |
| V11 | Temp index recovery/cleanup | Success and failure both delete `dedup_{snapshot_id}.sqlite` and journal residue; storage recovery removes a stale temp file; retry starts with an empty index. |
| V12 | Memory ceiling | Instrumented live-payload counter shows `<= 4` and no fifth `MAX_BATCH_BYTES`-class copy; engine allocator and SQLite cache stay within section 10.2; source grep and allocator prove no `HashSet`/`HashMap` dedup index. |
| V13 | Secret sentinel | Sentinel appears in a failing cell but not in `EngineError` Display/Debug, `sanitized_summary()` JSON, or constructed event metadata. |
| V14 | Retry determinism | Two runs after abort with identical inputs/identities produce identical accepted/rejected rows, stats, and partition boundaries; dedup index is empty at second start. |
| V15 | Utf8 and Binary key equality | Exact byte equality; empty string/binary distinct from null; no normalization/collation. |
| V16 | Timestamp key equality | Same unit/timezone and epoch count duplicate; different unit or timezone component type does not compare equal; null groups together. |
| V17 | Canonical key bytes and collision safety | Golden byte vectors for every type table in section 6.3; two different values never produce equal bytes; SQLite BLOB PK is the only duplicate decision path. |
| V18 | Key bounds | 65th key column, encoded key > 64 KiB, and dedup disk > 8 GiB each fail `BoundExceeded` before pair commit with no visible snapshot. |
| V19 | Schema/ColumnId/original value preservation | Rejected schema field order and metadata match scan output + six control fields; ColumnIds unchanged; Arrow values of rejected original fields equal projected source values, including null/NaN/`-0.0`. |
| V20 | Validation message safety and length | 1,024-byte safe message is stored byte-exact; 1,025 bytes, empty-after-trim, and secret-like message are preflight `InvalidPlan`; message absent from errors/logs/events. |
| V21 | Existing E2/E3 compatibility | `materialize` still returns `UnsupportedRule` for Validate/Deduplicate; `preview` behavior is unchanged by the E4 code path; no `PreviewResult` field changed. |
| V22 | CI and docs-only diff | `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, and unchanged frontend checks pass in the later runtime PR; this docs PR modifies only the file named in section 2. |

## 15. Stop conditions

Stop and return to contract review if implementation needs:

- a public type or field not named `Proposed` in section 7.3;
- a change to `PreviewRequest` / `PreviewResult` or a second Preview
  Validate/Deduplicate semantics;
- an in-memory `HashSet`/`HashMap` duplicate index or hash-only decision;
- approximate/fuzzy deduplication;
- a new cleaning rule language in DuckDB, SQL, or Python;
- unbounded collect, prefetch, full-source materialization, or temp files
  outside the storage-managed temp root;
- generated snapshot/dataset/session ids, timestamps, or quality scores;
- visible publication through any path except `commit_snapshot_pair`;
- a message or cell value in `EngineError`, Debug, logs, events, or
  sanitized summaries;
- serializing `EngineError`;
- Dependabot or unrelated lockfile edits;
- Join/Union execution or a third runtime path.

## 16. Known risks

- Two canonical remainders are live in E4; the 261 MiB peak is mandatory
  unless a later contract replaces the rejected remainder with a disk spool.
  A fifth `MAX_BATCH_BYTES`-class copy is `Internal`.
- SQLite BLOB primary-key equality is exact but the temp file is large.
  The 8 GiB disk ceiling and 512 KiB cache must be enforced by
  `stillflow-storage`, not left to SQLite defaults.
- Copying the projected source row per event is intentionally redundant.
  The 256-events-per-row and 1,000,000,000-events-per-run caps are the only
  guards; reviewers must not remove them to save memory.
- `source_row_ordinal` is assigned before `Scan.predicate`, so rejected
  ordinals can have gaps. APIs must document gaps and never use max ordinal
  as a row count.
- The reserved six control fields reduce the maximum cleanable source
  schema from 4,096 to 4,090 fields. This is frozen, not dynamic.
- Timestamp equality is deliberately type-local. Any future cross-unit or
  timezone-normalized dedup is a new contract and must not reuse the
  `0x0F` encoding silently.
- PR #53 may still revise the E3 public surface. E4 runtime must not start
  until PR #53 merges and must reconcile only section 9 against the merged
  API.
